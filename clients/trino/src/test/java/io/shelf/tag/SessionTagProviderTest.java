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
package io.shelf.tag;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link SessionTagProvider}'s thread-local lifecycle.
 */
final class SessionTagProviderTest
{
    @AfterEach
    void clear()
    {
        SessionTagProvider.clear();
    }

    @Test
    void defaultsToEmptyOutsideInstall()
    {
        assertThat(SessionTagProvider.INSTANCE.currentTag().isEmpty()).isTrue();
    }

    @Test
    void installFromSessionPropertiesFiltersByPrefix()
            throws Exception
    {
        Map<String, String> session = new LinkedHashMap<>();
        session.put("shelf.tag.experiment", "b1_on");
        session.put("query_max_memory", "10GB");
        try (AutoCloseable handle = SessionTagProvider.install(session)) {
            TagSet current = SessionTagProvider.INSTANCE.currentTag();
            assertThat(current.size()).isEqualTo(1);
            assertThat(current.asMap()).containsOnlyKeys("experiment");
            assertThat(current.toWire())
                    .isEqualTo("%7B%22experiment%22%3A%22b1_on%22%7D");
        }
        assertThat(SessionTagProvider.INSTANCE.currentTag().isEmpty()).isTrue();
    }

    @Test
    void installRestoresPreviousTagOnClose()
            throws Exception
    {
        Map<String, String> outer = Map.of("shelf.tag.experiment", "outer");
        Map<String, String> inner = Map.of("shelf.tag.experiment", "inner");
        try (AutoCloseable o = SessionTagProvider.install(outer)) {
            assertThat(SessionTagProvider.INSTANCE.currentTag().asMap())
                    .containsEntry("experiment", "outer");
            try (AutoCloseable i = SessionTagProvider.install(inner)) {
                assertThat(SessionTagProvider.INSTANCE.currentTag().asMap())
                        .containsEntry("experiment", "inner");
            }
            assertThat(SessionTagProvider.INSTANCE.currentTag().asMap())
                    .containsEntry("experiment", "outer");
        }
    }

    @Test
    void misconfiguredSessionPropFailsOpen()
            throws Exception
    {
        // Value too long → fromMap throws. install() catches and
        // installs an empty tag instead of propagating, satisfying the
        // fail-open contract.
        StringBuilder big = new StringBuilder();
        for (int i = 0; i <= TagSet.MAX_VALUE_BYTES; i++) {
            big.append('x');
        }
        Map<String, String> bad = Map.of("shelf.tag.experiment", big.toString());
        try (AutoCloseable handle = SessionTagProvider.install(bad)) {
            assertThat(SessionTagProvider.INSTANCE.currentTag().isEmpty()).isTrue();
        }
    }
}
