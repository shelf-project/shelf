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

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

class CircuitBreakerTest
{
    @Test
    @Disabled("TODO(SHELF-11): 9+ state-machine tests per 03-plan.md §4 SHELF-11 + BLUEPRINT §9.5.")
    void closedToOpenAfterFiveConsecutiveFailures()
    {
        // Closed -> Open on 5th consecutive failure. Exponential timer on
        // re-open is checked via the Clock seam.
    }
}
