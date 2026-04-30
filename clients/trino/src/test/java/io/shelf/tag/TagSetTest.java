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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Unit tests for {@link TagSet}.
 *
 * <p>Includes a parity test against {@code ab-tag-vectors.json} so the
 * Java and Rust implementations stay byte-for-byte compatible at the
 * wire level.
 */
final class TagSetTest
{
    @Test
    void emptyMapYieldsEmptyTag()
    {
        TagSet tag = TagSet.fromMap(Map.of());
        assertThat(tag.isEmpty()).isTrue();
        assertThat(tag.toWire()).isNull();
        assertThat(tag.toJson()).isEqualTo("{}");
    }

    @Test
    void singlePairRoundTripsThroughWireForm()
    {
        TagSet tag = TagSet.fromMap(Map.of("experiment", "b1_compression_on"));
        assertThat(tag.toJson()).isEqualTo("{\"experiment\":\"b1_compression_on\"}");
        assertThat(tag.toWire()).isEqualTo("%7B%22experiment%22%3A%22b1_compression_on%22%7D");
    }

    @Test
    void keysAreSortedLexicographicallyInWireForm()
    {
        Map<String, String> input = new LinkedHashMap<>();
        input.put("experiment", "b1_on");
        input.put("cohort", "rep1");
        TagSet tag = TagSet.fromMap(input);
        // "cohort" sorts before "experiment".
        assertThat(tag.toJson()).startsWith("{\"cohort\":");
        assertThat(tag.toWire())
                .isEqualTo("%7B%22cohort%22%3A%22rep1%22%2C%22experiment%22%3A%22b1_on%22%7D");
    }

    @Test
    void coercesIntAndBoolValuesToStrings()
    {
        Map<String, Object> input = new LinkedHashMap<>();
        input.put("epoch", 1714512345L);
        input.put("on", true);
        TagSet tag = TagSet.fromMap(input);
        assertThat(tag.asMap()).containsEntry("epoch", "1714512345").containsEntry("on", "true");
    }

    @Test
    void rejectsBadKey()
    {
        assertThatThrownBy(() -> TagSet.fromMap(Map.of("1bad", "x")))
                .isInstanceOf(TagSet.TagValidationException.class)
                .hasMessageContaining("rejected key");
    }

    @Test
    void rejectsValueTooLong()
    {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i <= TagSet.MAX_VALUE_BYTES; i++) {
            sb.append('x');
        }
        assertThatThrownBy(() -> TagSet.fromMap(Map.of("k", sb.toString())))
                .isInstanceOf(TagSet.TagValidationException.class)
                .hasMessageContaining("cap is " + TagSet.MAX_VALUE_BYTES);
    }

    @Test
    void rejectsTooManyKeys()
    {
        Map<String, String> tooMany = new LinkedHashMap<>();
        for (int i = 0; i <= TagSet.MAX_KEYS; i++) {
            tooMany.put("k" + i, "v");
        }
        assertThatThrownBy(() -> TagSet.fromMap(tooMany))
                .isInstanceOf(TagSet.TagValidationException.class)
                .hasMessageContaining("cap is " + TagSet.MAX_KEYS);
    }

    @Test
    void fromSessionPropertiesFiltersByPrefix()
    {
        Map<String, String> session = new LinkedHashMap<>();
        session.put("shelf.tag.experiment", "b1_on");
        session.put("query_max_memory", "10GB");
        session.put("shelf.notatag", "ignored");
        TagSet tag = TagSet.fromSessionProperties(session);
        assertThat(tag.size()).isEqualTo(1);
        assertThat(tag.asMap()).containsOnlyKeys("experiment");
    }

    @Test
    void fromWireRoundTripsBackToTheSameMap()
    {
        TagSet original = TagSet.fromMap(Map.of("experiment", "b1_on"));
        String wire = original.toWire();
        TagSet parsed = TagSet.fromWire(wire);
        assertThat(parsed.asMap()).isEqualTo(original.asMap());
    }

    @Test
    void parsesGoldenVectorsForRustParity()
            throws Exception
    {
        ObjectMapper mapper = new ObjectMapper();
        try (InputStream in = TagSetTest.class.getResourceAsStream("/ab-tag-vectors.json")) {
            assertThat(in).as("ab-tag-vectors.json must be on the test classpath").isNotNull();
            byte[] raw = in.readAllBytes();
            JsonNode root = mapper.readTree(new String(raw, StandardCharsets.UTF_8));
            JsonNode vectors = root.get("vectors");
            assertThat(vectors).as("vectors array").isNotNull();
            assertThat(vectors.isArray()).isTrue();
            for (JsonNode v : vectors) {
                String name = v.get("name").asText();
                Map<String, String> sessionProps = new LinkedHashMap<>();
                v.get("session_props").fields().forEachRemaining(e -> sessionProps.put(
                        e.getKey(), e.getValue().asText()));
                TagSet derived = TagSet.fromSessionProperties(sessionProps);

                JsonNode normalized = v.get("normalized");
                if (normalized.isNull()) {
                    assertThat(derived.isEmpty())
                            .as("vector %s: derived must be empty", name)
                            .isTrue();
                }
                else {
                    Map<String, String> expected = new LinkedHashMap<>();
                    normalized.fields().forEachRemaining(e -> expected.put(
                            e.getKey(), e.getValue().asText()));
                    assertThat(derived.asMap())
                            .as("vector %s: derived map", name)
                            .isEqualTo(expected);
                }

                JsonNode wireNode = v.get("wire");
                if (wireNode.isNull()) {
                    assertThat(derived.toWire())
                            .as("vector %s: wire null on empty", name)
                            .isNull();
                }
                else {
                    String expectedWire = wireNode.asText();
                    assertThat(derived.toWire())
                            .as("vector %s: wire form", name)
                            .isEqualTo(expectedWire);
                    // Round-trip the wire form back through Java's parser
                    // to assert byte-identity in both directions.
                    TagSet reparsed = TagSet.fromWire(expectedWire);
                    assertThat(reparsed.asMap())
                            .as("vector %s: re-parse round-trip", name)
                            .isEqualTo(derived.asMap());
                }
            }
        }
    }
}
