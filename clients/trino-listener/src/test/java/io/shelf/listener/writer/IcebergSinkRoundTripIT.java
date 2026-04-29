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
package io.shelf.listener.writer;

import io.shelf.listener.ShelfIcebergEventListener;
import io.shelf.listener.config.ListenerConfig;
import io.shelf.listener.metrics.ListenerMetrics;
import io.shelf.listener.support.TestEvents;
import io.trino.spi.eventlistener.QueryCompletedEvent;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.io.CloseableIterable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * End-to-end round trip: enqueue real {@link QueryCompletedEvent}s
 * through the listener, let the writer thread flush, then read the
 * Iceberg table back via {@code IcebergGenerics.read} and verify the
 * row schema + values.
 *
 * <p>Gated on {@code SHELF_INTEGRATION=1} (mirrors the shelfd Rust
 * convention; without it the test reports "skipped", never "passed").
 * Boots a {@code HadoopCatalog} on a tmpdir — no Hive metastore, no
 * external services. ~5 s on a warm laptop.
 */
@EnabledIfEnvironmentVariable(named = "SHELF_INTEGRATION", matches = "1")
class IcebergSinkRoundTripIT
{
    @TempDir
    Path tempDir;

    private ShelfIcebergEventListener listener;

    @BeforeEach
    void setUp()
    {
        Map<String, String> raw = new LinkedHashMap<>();
        raw.put(ListenerConfig.K_ICEBERG_CATALOG, "test_hadoop");
        raw.put(ListenerConfig.K_ICEBERG_TABLE, "trino_logs.queries");
        raw.put(ListenerConfig.K_ICEBERG_CATALOG_IMPL, "org.apache.iceberg.hadoop.HadoopCatalog");
        raw.put("shelf.listener.iceberg.warehouse", tempDir.toUri().toString());
        raw.put(ListenerConfig.K_BATCH_MAX_ROWS, "5");
        raw.put(ListenerConfig.K_BATCH_MAX_INTERVAL_SECS, "1");
        ListenerConfig cfg = ListenerConfig.fromMap(raw);
        listener = new ShelfIcebergEventListener(cfg);
    }

    @AfterEach
    void tearDown()
    {
        if (listener != null) {
            listener.close();
        }
    }

    @Test
    void roundTripFiveEvents()
            throws Exception
    {
        for (int i = 0; i < 5; i++) {
            int idx = i;
            QueryCompletedEvent ev = TestEvents.canonical(o -> {
                o.queryId = "q_" + idx;
                o.physicalInputBytes = 1_000_000L * idx;
                o.user = "u_" + idx;
            });
            listener.queryCompleted(ev);
        }

        // Block until the writer thread has flushed all 5.
        long deadline = System.nanoTime() + Duration.ofSeconds(15).toNanos();
        long written;
        do {
            Thread.sleep(50);
            ListenerMetrics.Snapshot s = listener.metrics().snapshot();
            written = s.events.getOrDefault("written", 0L);
        }
        while (written < 5 && System.nanoTime() < deadline);
        assertThat(written).isEqualTo(5);

        // Read the table back through iceberg-data's generic reader.
        Table table = openTableForReadback();
        int rowsRead = 0;
        try (CloseableIterable<Record> it = IcebergGenerics.read(table).build()) {
            for (Record r : it) {
                rowsRead++;
                assertThat(r.getField("query_id")).asString().startsWith("q_");
                assertThat(r.getField("query_state")).isEqualTo("FINISHED");
                assertThat(r.getField("user")).asString().startsWith("u_");
                assertThat(r.getField("query_hash")).asString().hasSize(64);
                assertThat(r.getField("inputs_json")).isNotNull();
                assertThat(r.getField("tags_json")).isNotNull();
            }
        }
        assertThat(rowsRead).isEqualTo(5);
    }

    private Table openTableForReadback()
    {
        org.apache.iceberg.hadoop.HadoopCatalog hc =
                new org.apache.iceberg.hadoop.HadoopCatalog();
        hc.setConf(new org.apache.hadoop.conf.Configuration());
        hc.initialize("test_hadoop", Map.of("warehouse", tempDir.toUri().toString()));
        return hc.loadTable(org.apache.iceberg.catalog.TableIdentifier.of("trino_logs", "queries"));
    }
}
