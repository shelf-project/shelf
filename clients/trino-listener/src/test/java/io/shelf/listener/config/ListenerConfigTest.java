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
package io.shelf.listener.config;

import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ListenerConfigTest
{
    @Test
    void parsesMandatoryFields()
    {
        Map<String, String> raw = new LinkedHashMap<>();
        raw.put("shelf.listener.iceberg.catalog", "hive");
        raw.put("shelf.listener.iceberg.table", "trino_logs.queries");
        ListenerConfig cfg = ListenerConfig.fromMap(raw);
        assertThat(cfg.catalogName()).isEqualTo("hive");
        assertThat(cfg.tableSchema()).isEqualTo("trino_logs");
        assertThat(cfg.tableName()).isEqualTo("queries");
        assertThat(cfg.failMode()).isEqualTo(FailMode.DROP);
        assertThat(cfg.batchMaxRows()).isEqualTo(ListenerConfig.DEFAULT_BATCH_MAX_ROWS);
        assertThat(cfg.queueCapacity()).isEqualTo(ListenerConfig.DEFAULT_QUEUE_CAPACITY);
    }

    @Test
    void forwardsCatalogNamespaceProperties()
    {
        Map<String, String> raw = new LinkedHashMap<>();
        raw.put("shelf.listener.iceberg.catalog", "hive");
        raw.put("shelf.listener.iceberg.table", "default.t");
        raw.put("shelf.listener.iceberg.warehouse", "s3a://bucket/wh/");
        raw.put("shelf.listener.iceberg.uri", "thrift://hms:9083");
        raw.put("shelf.listener.iceberg.io-impl", "org.apache.iceberg.aws.s3.S3FileIO");
        ListenerConfig cfg = ListenerConfig.fromMap(raw);
        assertThat(cfg.catalogProperties())
                .containsEntry("warehouse", "s3a://bucket/wh/")
                .containsEntry("uri", "thrift://hms:9083")
                .containsEntry("io-impl", "org.apache.iceberg.aws.s3.S3FileIO");
    }

    @Test
    void rejectsTableMissingDot()
    {
        Map<String, String> raw = new LinkedHashMap<>();
        raw.put("shelf.listener.iceberg.catalog", "hive");
        raw.put("shelf.listener.iceberg.table", "noschema");
        assertThatThrownBy(() -> ListenerConfig.fromMap(raw))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("<schema>.<table>");
    }

    @Test
    void parsesFailModeVariants()
    {
        assertThat(FailMode.parse(null)).isEqualTo(FailMode.DROP);
        assertThat(FailMode.parse("drop")).isEqualTo(FailMode.DROP);
        assertThat(FailMode.parse("BLOCK")).isEqualTo(FailMode.BLOCK);
        assertThat(FailMode.parse("log_only")).isEqualTo(FailMode.LOG_ONLY);
        assertThat(FailMode.parse("log-only")).isEqualTo(FailMode.LOG_ONLY);
        assertThatThrownBy(() -> FailMode.parse("frob"))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void rejectsOutOfRangeIntegers()
    {
        Map<String, String> raw = new LinkedHashMap<>();
        raw.put("shelf.listener.iceberg.catalog", "hive");
        raw.put("shelf.listener.iceberg.table", "s.t");
        raw.put("shelf.listener.batch.max-rows", "0");
        assertThatThrownBy(() -> ListenerConfig.fromMap(raw))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("batch.max-rows");
    }
}
