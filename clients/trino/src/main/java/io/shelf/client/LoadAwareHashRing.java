/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.shelf.client;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Load-aware extension of {@link HashRing} implementing Slicer-inspired routing
 * (§8.4 from TODO-fix-shelf-performance.md).
 *
 * <p>Retains HRW-by-key as the first-choice routing but consults each pod's
 * recently-published {@code shelf_pod_load_qps} and demotes candidates whose
 * load exceeds the cluster median by the configured threshold (default 50%).
 * Falls back to the next-highest HRW candidate.
 *
 * <h2>Why novel for OSS OLAP</h2>
 *
 * <p><a href="https://research.google/pubs/slicer-auto-sharding-for-datacenter-applications/">
 * Slicer (Adya et al., OSDI 2016)</a> is the canonical reference for load-aware
 * sharding: it reports the median production workload's most-loaded task at
 * 30–180% of mean load and shows that the median workload uses 63% fewer
 * resources than static sharding.
 *
 * <p>Nobody has published a Slicer-style load-aware HRW variant tuned specifically
 * for byte-range OLAP caches with content-addressed keys, where the cache-locality
 * cost of a routing change is bounded (peer-fetch absorbs it cleanly).
 *
 * <h2>Usage</h2>
 *
 * <pre>{@code
 * // Create with load statistics from /stats endpoint polling
 * Map<String, Double> podLoads = Map.of(
 *     "shelf-0", 100.0,
 *     "shelf-1", 250.0,   // overloaded
 *     "shelf-2", 80.0
 * );
 *
 * LoadAwareHashRing ring = new LoadAwareHashRing(members, podLoads, 1.5);
 * Optional<HashRing.Node> owner = ring.ownerFor(cacheKey);
 * }</pre>
 *
 * <h2>Composes with</h2>
 *
 * <ul>
 *   <li>{@code shelfd/src/pod_load.rs} — publishes {@code shelf_pod_load_qps}
 *   <li>{@code shelfd/src/router.rs} — Rust-side HRW (unchanged)
 *   <li>SHELF-23 peer-fetch — handles cold requests after re-routing
 * </ul>
 *
 * <h2>Risk mitigation</h2>
 *
 * <p>Cache-locality regression: a key that the HRW primary would have served
 * from a warm cache gets routed to a cold peer. Mitigation: peer-fetch (SHELF-23)
 * handles the second request from the cold peer transparently; the cost is one
 * cold first-request per re-route.
 *
 * @see HashRing
 * @see <a href="https://research.google/pubs/slicer-auto-sharding-for-datacenter-applications/">Slicer (OSDI 2016)</a>
 */
public final class LoadAwareHashRing
{
    /**
     * Default load threshold factor: demote pods with load > median * factor.
     */
    public static final double DEFAULT_LOAD_THRESHOLD_FACTOR = 1.5;

    private final HashRing baseRing;
    private final Map<String, Double> podLoads;
    private final double loadThresholdFactor;
    private final double medianLoad;
    private final double loadThreshold;

    /**
     * Create a load-aware hash ring.
     *
     * @param members             ring membership (same as {@link HashRing})
     * @param podLoads            current QPS per pod, keyed by podId
     * @param loadThresholdFactor demote pods with load > median * this factor
     */
    public LoadAwareHashRing(
            List<HashRing.Node> members,
            Map<String, Double> podLoads,
            double loadThresholdFactor)
    {
        this.baseRing = new HashRing(members);
        this.podLoads = new ConcurrentHashMap<>(Objects.requireNonNull(podLoads, "podLoads"));
        this.loadThresholdFactor = loadThresholdFactor;
        this.medianLoad = computeMedianLoad(podLoads);
        this.loadThreshold = medianLoad * loadThresholdFactor;
    }

    /**
     * Create with default load threshold factor (1.5x median).
     */
    public LoadAwareHashRing(List<HashRing.Node> members, Map<String, Double> podLoads)
    {
        this(members, podLoads, DEFAULT_LOAD_THRESHOLD_FACTOR);
    }

    /**
     * Return the pod that owns the given key, considering load balancing.
     *
     * <p>If the HRW-optimal pod exceeds the load threshold, returns the next
     * candidate that is under threshold. If all candidates exceed threshold,
     * returns the least-loaded pod.
     *
     * @param contentAddressedKey cache key (see {@link HashRing#ownerFor(byte[])})
     * @return the owning pod, considering load
     */
    public Optional<HashRing.Node> ownerFor(byte[] contentAddressedKey)
    {
        if (baseRing.members().isEmpty()) {
            return Optional.empty();
        }

        // Get all candidates ranked by HRW score (descending)
        List<ScoredNode> ranked = new ArrayList<>(baseRing.members().size());
        for (HashRing.Node node : baseRing.members()) {
            double score = HashRing.score(contentAddressedKey, node);
            ranked.add(new ScoredNode(node, score));
        }
        ranked.sort(Comparator.comparingDouble(ScoredNode::score).reversed());

        // Find first candidate under load threshold
        for (ScoredNode sn : ranked) {
            double load = podLoads.getOrDefault(sn.node().podId(), 0.0);
            if (load <= loadThreshold) {
                return Optional.of(sn.node());
            }
        }

        // All pods are overloaded — fall back to least-loaded
        return baseRing.members().stream()
                .min(Comparator.comparingDouble(
                        n -> podLoads.getOrDefault(n.podId(), Double.MAX_VALUE)));
    }

    /**
     * Return the underlying base ring for inspection.
     */
    public HashRing baseRing()
    {
        return baseRing;
    }

    /**
     * Return current load threshold (median * factor).
     */
    public double loadThreshold()
    {
        return loadThreshold;
    }

    /**
     * Return computed median load.
     */
    public double medianLoad()
    {
        return medianLoad;
    }

    /**
     * Check if a pod is currently considered overloaded.
     */
    public boolean isOverloaded(String podId)
    {
        return podLoads.getOrDefault(podId, 0.0) > loadThreshold;
    }

    /**
     * Get the load skew ratio: max load / median load.
     * Slicer reports typical skew of 1.3–1.8 in production.
     */
    public double loadSkewRatio()
    {
        if (medianLoad <= 0) {
            return 1.0;
        }
        double maxLoad = podLoads.values().stream()
                .mapToDouble(Double::doubleValue)
                .max()
                .orElse(0.0);
        return maxLoad / medianLoad;
    }

    private static double computeMedianLoad(Map<String, Double> loads)
    {
        if (loads.isEmpty()) {
            return 0.0;
        }
        List<Double> sorted = new ArrayList<>(loads.values());
        sorted.sort(Double::compareTo);
        int mid = sorted.size() / 2;
        if (sorted.size() % 2 == 0) {
            return (sorted.get(mid - 1) + sorted.get(mid)) / 2.0;
        }
        return sorted.get(mid);
    }

    private record ScoredNode(HashRing.Node node, double score) {}
}
