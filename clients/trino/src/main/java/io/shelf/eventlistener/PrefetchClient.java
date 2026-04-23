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
package io.shelf.eventlistener;

/**
 * gRPC client stub for the Shelf control-plane {@code Prefetch} RPC
 * (BLUEPRINT §8.2).
 *
 * <p><b>Phase-2 deliverable.</b> The v0.1 Phase-0 plugin ships with this stub
 * as a no-op: {@link ShelfPrefetchListener} calls into it, and it drops the
 * request on the floor. The real gRPC implementation arrives with Phase 2
 * (plan-aware push prefetch), driven by the outcome of experiment E1
 * (see 03-plan.md §2 + §3 Phase 2).
 *
 * <p>Keeping the class in-tree lets us wire the listener to the call site
 * now without pulling in the gRPC dependency until the Phase-2 ticket
 * introduces it.
 */
public final class PrefetchClient
{
    public PrefetchClient() {}

    /**
     * Fire-and-forget prefetch request. Bounded by a hard coordinator-side
     * deadline ({@value DEFAULT_COORDINATOR_DEADLINE_MS} ms); see §9.5 +
     * 03-plan.md §3 Phase 2.
     */
    public void prefetch(PrefetchRequest request)
    {
        // TODO(SHELF-PHASE-2): wire gRPC client + non-blocking submit
        //   with coordinator-side deadline. Circuit-breaker wraps the call.
        //   Parked until 03-plan.md §3 Phase 2 entry criterion (E1/E2 signal).
    }

    /** Cancel any in-flight prefetches for this query id. */
    public void cancel(String queryId)
    {
        // TODO(SHELF-PHASE-2): cancellation on QueryCompletedEvent.
    }

    public static final long DEFAULT_COORDINATOR_DEADLINE_MS = 10L;

    /** Minimal request record sufficient for the skeleton call sites. */
    public record PrefetchRequest(String queryId, String tenant) {}
}
