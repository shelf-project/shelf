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
package io.shelf.filesystem;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

class ShelfFileSystemTest
{
    @Test
    @Disabled("TODO(SHELF-10): implement once ShelfFileSystem is wired — see 03-plan.md §4.")
    void failsOpenWhenShelfIsUnreachable()
    {
        // Property: for any sequence of {success, timeout, 503, connect-close},
        // Trino never sees a Shelf-specific exception. Exercised end-to-end once
        // the real read path lands.
    }
}
