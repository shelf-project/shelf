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
package io.shelf.listener.extract;

import io.shelf.listener.support.TestEvents;
import io.trino.spi.eventlistener.QueryCompletedEvent;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class EventExtractorTest
{
    private final EventExtractor extractor = new EventExtractor(64 * 1024);

    @Test
    void extractsEveryDocumentedColumn()
    {
        QueryCompletedEvent event = TestEvents.canonical(o -> {
            o.queryId = "20260430_010101_00042_oss";
            o.user = "alice";
            o.query = "SELECT 1";
            o.physicalInputBytes = 12345;
        });
        ExtractedRow row = extractor.extract(event);

        assertThat(row.queryId).isEqualTo("20260430_010101_00042_oss");
        assertThat(row.queryState).isEqualTo("FINISHED");
        assertThat(row.user).isEqualTo("alice");
        assertThat(row.principal).isEqualTo("alice@dc.example");
        assertThat(row.source).isEqualTo("trino-cli");
        assertThat(row.catalog).isEqualTo("hive");
        assertThat(row.schemaName).isEqualTo("default");
        assertThat(row.resourceGroupId).isEqualTo("global.interactive");
        assertThat(row.serverAddress).isEqualTo("10.0.0.42");
        assertThat(row.physicalInputBytes).isEqualTo(12345);
        assertThat(row.physicalInputReadTimeMillis).isEqualTo(99);
        assertThat(row.wallTimeMillis).isEqualTo(456);
        assertThat(row.cpuTimeMillis).isEqualTo(123);
        assertThat(row.queuedTimeMillis).isEqualTo(7);
        assertThat(row.planningTimeMillis).isEqualTo(8);
        assertThat(row.executeTimeMillis).isEqualTo(440);
        assertThat(row.peakUserMemoryBytes).isEqualTo(1024);
        assertThat(row.peakTotalMemoryBytes).isEqualTo(2048);
        assertThat(row.processedInputBytes).isEqualTo(950_000);
        assertThat(row.processedInputRows).isEqualTo(9_500);
        assertThat(row.outputBytes).isEqualTo(1024);
        assertThat(row.outputRows).isEqualTo(16);
        assertThat(row.queryHash).hasSize(64);
        assertThat(row.createTime).isNotNull();
        assertThat(row.endTime).isNotNull();
        assertThat(row.inputsJson).isEqualTo("[]");
        assertThat(row.outputsJson).isEqualTo("[]");
        assertThat(row.tagsJson).isEqualTo("{}");
    }

    @Test
    void extractsErrorFieldsWhenQueryFailed()
    {
        QueryCompletedEvent event = TestEvents.canonical(o -> {
            o.queryState = "FAILED";
            o.failure = java.util.Optional.of(TestEvents.failure(
                    65557, "ICEBERG_INVALID_METADATA", "EXTERNAL", "boom"));
        });
        ExtractedRow row = extractor.extract(event);
        assertThat(row.queryState).isEqualTo("FAILED");
        assertThat(row.errorCode).isEqualTo("ICEBERG_INVALID_METADATA");
        assertThat(row.errorType).isEqualTo("EXTERNAL");
        assertThat(row.errorMessage).isEqualTo("boom");
    }

    @Test
    void inputsJsonContainsTableTuples()
    {
        QueryCompletedEvent event = TestEvents.canonical(o -> {
            o.inputs = List.of(
                    TestEvents.input("hive", "default", "t1"),
                    TestEvents.input("iceberg", "logs", "queries"));
        });
        ExtractedRow row = extractor.extract(event);
        assertThat(row.inputsJson)
                .contains("\"catalog\":\"hive\"")
                .contains("\"table\":\"t1\"")
                .contains("\"catalog\":\"iceberg\"")
                .contains("\"table\":\"queries\"");
    }

    @Test
    void tagsJsonOnlyCarriesShelfTagPrefix()
    {
        QueryCompletedEvent event = TestEvents.canonical(o -> {
            LinkedHashMap<String, String> props = new LinkedHashMap<>();
            props.put("shelf.tag.experiment", "foo");
            props.put("shelf.tag.arm", "B");
            props.put("hive.parquet_use_column_index", "true");
            props.put("shelf.tag.", "should-be-ignored");
            o.sessionProperties = props;
        });
        ExtractedRow row = extractor.extract(event);
        assertThat(row.tagsJson)
                .contains("\"experiment\":\"foo\"")
                .contains("\"arm\":\"B\"")
                .doesNotContain("hive.parquet_use_column_index")
                .doesNotContain("should-be-ignored");
    }

    @Test
    void queryTextIsTruncatedAtConfiguredCap()
    {
        EventExtractor small = new EventExtractor(512);
        StringBuilder big = new StringBuilder();
        for (int i = 0; i < 2_000; i++) {
            big.append('x');
        }
        QueryCompletedEvent event = TestEvents.canonical(o -> o.query = big.toString());
        // Inject the large query text via the metadata position by re-invoking extractor on a
        // synthetic event whose query is the big string.
        ExtractedRow row = small.extract(event);
        assertThat(row.queryText.length()).isLessThanOrEqualTo(512);
    }

    @Test
    void utf8TruncationDoesNotSplitCodepoints()
    {
        // Five 4-byte emojis = 20 bytes. Cap at 17 → must back off to 16 (4 codepoints).
        String s = "\uD83D\uDE00\uD83D\uDE00\uD83D\uDE00\uD83D\uDE00\uD83D\uDE00";
        String truncated = EventExtractor.truncateUtf8(s, 17);
        assertThat(truncated).isEqualTo("\uD83D\uDE00\uD83D\uDE00\uD83D\uDE00\uD83D\uDE00");
        assertThat(truncated.getBytes(java.nio.charset.StandardCharsets.UTF_8)).hasSize(16);
    }

    @Test
    void hashIsDeterministicAndSensitive()
    {
        String h1 = EventExtractor.sha256Hex("SELECT 1");
        String h2 = EventExtractor.sha256Hex("SELECT 1");
        String h3 = EventExtractor.sha256Hex("SELECT 2");
        assertThat(h1).isEqualTo(h2).hasSize(64);
        assertThat(h1).isNotEqualTo(h3);
    }
}
