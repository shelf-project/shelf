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

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

class ShelfPrefetchListenerTest
{
    @Test
    @Disabled("TODO(SHELF-PHASE-2): wire once E1 confirms QueryMetadata.plan signal — see 03-plan.md §2 E1.")
    void queryCreatedExtractsTablesAndPredicates()
    {
        // Property: queryCreated never blocks the coordinator for > 10 ms
        // (R-09) and never throws. QueryCompleted drains operatorSummaries
        // per ADR-0005.
    }
}
