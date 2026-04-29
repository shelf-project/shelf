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

import io.shelf.listener.extract.ExtractedRow;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.types.Types;

import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;

/**
 * Iceberg schema + partition spec for the SHELF-37 sink, plus the
 * {@link ExtractedRow} → {@link Record} bridge.
 *
 * <p>The schema is partitioned by {@code day(create_time)}. The OSS
 * module surfaces the partition source as {@code create_time} (the
 * Trino SPI's own naming) so downstream tooling does not silently
 * inherit any pre-existing in-house column name. SHELF-40 / SHELF-42
 * read this schema directly.
 */
public final class IcebergSchema
{
    private IcebergSchema() {}

    public static final Schema SCHEMA = new Schema(
            // Identity
            Types.NestedField.required(1, "query_id", Types.StringType.get()),
            Types.NestedField.required(2, "query_state", Types.StringType.get()),
            Types.NestedField.optional(3, "error_code", Types.StringType.get()),
            Types.NestedField.optional(4, "error_type", Types.StringType.get()),
            Types.NestedField.optional(5, "error_message", Types.StringType.get()),

            // Principal / source
            Types.NestedField.optional(6, "principal", Types.StringType.get()),
            Types.NestedField.required(7, "user", Types.StringType.get()),
            Types.NestedField.optional(8, "source", Types.StringType.get()),
            Types.NestedField.optional(9, "catalog", Types.StringType.get()),
            Types.NestedField.optional(10, "schema", Types.StringType.get()),
            Types.NestedField.optional(11, "resource_group_id", Types.StringType.get()),

            // Query text + hash
            Types.NestedField.required(12, "query_text", Types.StringType.get()),
            Types.NestedField.required(13, "query_hash", Types.StringType.get()),

            // Timing
            Types.NestedField.required(14, "create_time", Types.TimestampType.withZone()),
            Types.NestedField.required(15, "end_time", Types.TimestampType.withZone()),
            Types.NestedField.required(16, "execute_time_millis", Types.LongType.get()),
            Types.NestedField.required(17, "queued_time_millis", Types.LongType.get()),
            Types.NestedField.required(18, "planning_time_millis", Types.LongType.get()),
            Types.NestedField.required(19, "wall_time_millis", Types.LongType.get()),
            Types.NestedField.required(20, "cpu_time_millis", Types.LongType.get()),

            // Physical input
            Types.NestedField.required(21, "physical_input_bytes", Types.LongType.get()),
            Types.NestedField.required(22, "physical_input_read_time_millis", Types.LongType.get()),
            Types.NestedField.required(23, "physical_input_rows", Types.LongType.get()),

            // Processed input + output + memory
            Types.NestedField.required(24, "processed_input_bytes", Types.LongType.get()),
            Types.NestedField.required(25, "processed_input_rows", Types.LongType.get()),
            Types.NestedField.required(26, "output_bytes", Types.LongType.get()),
            Types.NestedField.required(27, "output_rows", Types.LongType.get()),
            Types.NestedField.required(28, "peak_user_memory_bytes", Types.LongType.get()),
            Types.NestedField.required(29, "peak_total_memory_bytes", Types.LongType.get()),

            // Coordinator pod IP (NOT a hostname; see ExtractedRow javadoc)
            Types.NestedField.optional(30, "server_address", Types.StringType.get()),

            // JSON sidecars
            Types.NestedField.required(31, "inputs_json", Types.StringType.get()),
            Types.NestedField.required(32, "outputs_json", Types.StringType.get()),
            Types.NestedField.required(33, "tags_json", Types.StringType.get()));

    /** Identity-by-day partitioning on {@code create_time}. */
    public static final PartitionSpec SPEC = PartitionSpec.builderFor(SCHEMA)
            .day("create_time")
            .build();

    /** Project an extracted row to a fresh {@link GenericRecord} for the writer. */
    public static Record toRecord(ExtractedRow row)
    {
        GenericRecord rec = GenericRecord.create(SCHEMA);
        rec.setField("query_id", row.queryId);
        rec.setField("query_state", row.queryState);
        rec.setField("error_code", row.errorCode);
        rec.setField("error_type", row.errorType);
        rec.setField("error_message", row.errorMessage);
        rec.setField("principal", row.principal);
        rec.setField("user", row.user);
        rec.setField("source", row.source);
        rec.setField("catalog", row.catalog);
        rec.setField("schema", row.schemaName);
        rec.setField("resource_group_id", row.resourceGroupId);
        rec.setField("query_text", row.queryText);
        rec.setField("query_hash", row.queryHash);
        rec.setField("create_time", normalize(row.createTime));
        rec.setField("end_time", normalize(row.endTime));
        rec.setField("execute_time_millis", row.executeTimeMillis);
        rec.setField("queued_time_millis", row.queuedTimeMillis);
        rec.setField("planning_time_millis", row.planningTimeMillis);
        rec.setField("wall_time_millis", row.wallTimeMillis);
        rec.setField("cpu_time_millis", row.cpuTimeMillis);
        rec.setField("physical_input_bytes", row.physicalInputBytes);
        rec.setField("physical_input_read_time_millis", row.physicalInputReadTimeMillis);
        rec.setField("physical_input_rows", row.physicalInputRows);
        rec.setField("processed_input_bytes", row.processedInputBytes);
        rec.setField("processed_input_rows", row.processedInputRows);
        rec.setField("output_bytes", row.outputBytes);
        rec.setField("output_rows", row.outputRows);
        rec.setField("peak_user_memory_bytes", row.peakUserMemoryBytes);
        rec.setField("peak_total_memory_bytes", row.peakTotalMemoryBytes);
        rec.setField("server_address", row.serverAddress);
        rec.setField("inputs_json", row.inputsJson);
        rec.setField("outputs_json", row.outputsJson);
        rec.setField("tags_json", row.tagsJson);
        return rec;
    }

    /**
     * Iceberg's {@code TimestampType.withZone()} stores microseconds-since-epoch
     * UTC. The Java in-memory representation is {@code OffsetDateTime} at
     * {@link ZoneOffset#UTC}; the Parquet writer rejects any other zone. We
     * normalise here so callers can hand us any offset.
     */
    private static OffsetDateTime normalize(OffsetDateTime t)
    {
        if (t == null) {
            return OffsetDateTime.of(LocalDate.EPOCH.atStartOfDay(), ZoneOffset.UTC);
        }
        return t.withOffsetSameInstant(ZoneOffset.UTC);
    }
}
