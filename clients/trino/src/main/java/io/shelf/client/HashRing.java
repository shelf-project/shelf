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

import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * Highest Random Weight (HRW, aka Rendezvous) hashing over Shelf pod names.
 *
 * <p>Per ADR-0002, Shelf uses HRW over the live K8s headless-service membership
 * instead of a 2000-vnode consistent hash ring. For each key, the pod with the
 * largest {@code sha256(key || node_id) * weight(node)} owns the key. The plugin
 * and {@code shelfd} compute the owner identically (golden-vector crosscheck
 * lands in ticket SHELF-19).
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
     *                            produced by {@code shelf-key} (SHELF-04).
     * @return the owning pod, or {@link Optional#empty()} if the ring is empty.
     */
    public Optional<Node> ownerFor(byte[] contentAddressedKey)
    {
        Objects.requireNonNull(contentAddressedKey, "contentAddressedKey");
        // TODO(SHELF-19): implement HRW per ADR-0002.
        //   For each node compute score = hashScore(key || podId) * weight;
        //   argmax wins. Golden-vector test cross-checks Rust and Java
        //   (see SHELF-04 + SHELF-19).
        if (members.isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(members.get(0));
    }
}
