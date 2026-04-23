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
package io.shelf.config;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

class ShelfConfigTest
{
    @Test
    @Disabled("TODO(SHELF-10): parse + validate each property key per BLUEPRINT §6.2 — see 03-plan.md §4 SHELF-10.")
    void rejectsInvalidFallbackOnErrorValue()
    {
        // Round-trip: every documented key name and default lands on the
        // right ShelfConfig field. Validation errors throw
        // IllegalArgumentException with the offending key in the message.
    }
}
