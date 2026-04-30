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

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * Highest Random Weight (HRW, aka Rendezvous) hashing over Shelf pod names.
 *
 * <p>Per ADR-0002, Shelf uses HRW over the live K8s headless-service membership
 * instead of a 2000-vnode consistent hash ring. For each key, the pod with the
 * largest {@code weight / -ln(x)} owns the key, where
 *
 * <pre>{@code
 *   h      = sha256(key || pod_id.getBytes(UTF_8))
 *   u64_be = big-endian u64 from h[0..8]
 *   top53  = u64_be >>> 11           // top 53 bits, fits a double exactly
 *   x      = top53 / 2^53            // in [0, 1)
 *   score  = weight / (-ln x)
 * }</pre>
 *
 * <p>Ties are broken by lexicographically-smaller {@code podId}. The Rust
 * daemon (see {@code shelfd/src/router.rs}) implements the same function; the
 * cross-language golden-vector fixture
 * {@code shelfd/tests/fixtures/hrw_golden_vectors.txt} is consumed by both
 * sides' test suites so a drift breaks the build immediately.
 *
 * <p>Weights come from each pod's {@code /stats} endpoint
 * ({@code capacity_bytes - used_bytes}).
 *
 * <p>This class is intentionally side-effect free: DNS refresh and {@code /stats}
 * polling are responsibilities of the membership resolver (SHELF-20).
 */
public final class HashRing
{
    /**
     * Immutable snapshot of ring membership at a point in time. The membership
     * resolver publishes a new snapshot; readers hold whichever snapshot they
     * fetched before the resolver's swap (see SHELF-20 for rebalance semantics).
     *
     * @param podId stable K8s pod identity, e.g. {@code shelf-2}.
     * @param weight non-negative; zero marks a draining / over-full pod.
     */
    public record Node(String podId, double weight)
    {
        public Node
        {
            Objects.requireNonNull(podId, "podId");
            if (Double.isNaN(weight) || weight < 0d) {
                throw new IllegalArgumentException("weight must be non-negative, got " + weight);
            }
        }
    }

    private final List<Node> members;

    public HashRing(List<Node> members)
    {
        this.members = List.copyOf(Objects.requireNonNull(members, "members"));
    }

    public List<Node> members()
    {
        return members;
    }

    /**
     * Return the pod that owns the given content-addressed key.
     *
     * @param contentAddressedKey {@code sha256(etag || offset || length)} bytes
     *                            produced by {@link Key} (SHELF-04), or in the
     *                            test fixture any 32-byte sequence.
     * @return the owning pod, or {@link Optional#empty()} if the ring is empty.
     */
    public Optional<Node> ownerFor(byte[] contentAddressedKey)
    {
        Objects.requireNonNull(contentAddressedKey, "contentAddressedKey");
        Node best = null;
        double bestScore = Double.NEGATIVE_INFINITY;
        for (Node n : members) {
            double s = score(contentAddressedKey, n);
            boolean take = (best == null)
                    || s > bestScore
                    || (s == bestScore && n.podId().compareTo(best.podId()) < 0);
            if (take) {
                best = n;
                bestScore = s;
            }
        }
        return Optional.ofNullable(best);
    }

    /**
     * Capacity-weighted HRW score for a single (key, node) pair. Exposed for
     * the golden-vector test; not part of the stable API.
     */
    public static double score(byte[] key, Node node)
    {
        MessageDigest digest;
        try {
            digest = MessageDigest.getInstance("SHA-256");
        }
        catch (NoSuchAlgorithmException impossible) {
            throw new IllegalStateException("SHA-256 unavailable", impossible);
        }
        digest.update(key);
        digest.update(node.podId().getBytes(StandardCharsets.UTF_8));
        byte[] h = digest.digest();

        long u64 = 0L;
        for (int i = 0; i < 8; i++) {
            u64 = (u64 << 8) | (h[i] & 0xffL);
        }
        long top53 = u64 >>> 11;
        double x = (double) top53 / (double) (1L << 53);
        if (x <= 0.0) {
            return Double.POSITIVE_INFINITY;
        }
        double negLn = -Math.log(x);
        return node.weight() / negLn;
    }
}
