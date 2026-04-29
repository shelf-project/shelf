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

import java.time.OffsetDateTime;

/**
 * Plain-data projection of {@code QueryCompletedEvent} into the columns
 * the SHELF-37 Iceberg sink writes. Order matches
 * {@link io.shelf.listener.writer.IcebergSchema}.
 *
 * <p><b>Schema note for downstream tooling</b> ({@code server_address}):
 * the value is the coordinator's <em>pod IP</em>, not a hostname. Trino's
 * own conventions populate {@code QueryContext.serverAddress} that way;
 * SHELF-40 / SHELF-42 must not silently assume a DNS name.
 */
public final class ExtractedRow
{
    public final String queryId;
    public final String queryState;
    public final String errorCode;
    public final String errorType;
    public final String errorMessage;

    public final String principal;
    public final String user;
    public final String source;
    public final String catalog;
    public final String schemaName;
    public final String resourceGroupId;

    public final String queryText;
    public final String queryHash;

    public final OffsetDateTime createTime;
    public final OffsetDateTime endTime;
    public final long executeTimeMillis;
    public final long queuedTimeMillis;
    public final long planningTimeMillis;
    public final long wallTimeMillis;
    public final long cpuTimeMillis;

    public final long physicalInputBytes;
    public final long physicalInputReadTimeMillis;
    public final long physicalInputRows;
    public final long processedInputBytes;
    public final long processedInputRows;
    public final long outputBytes;
    public final long outputRows;
    public final long peakUserMemoryBytes;
    public final long peakTotalMemoryBytes;

    public final String serverAddress;

    public final String inputsJson;
    public final String outputsJson;
    public final String tagsJson;

    private ExtractedRow(Builder b)
    {
        this.queryId = b.queryId;
        this.queryState = b.queryState;
        this.errorCode = b.errorCode;
        this.errorType = b.errorType;
        this.errorMessage = b.errorMessage;
        this.principal = b.principal;
        this.user = b.user;
        this.source = b.source;
        this.catalog = b.catalog;
        this.schemaName = b.schemaName;
        this.resourceGroupId = b.resourceGroupId;
        this.queryText = b.queryText;
        this.queryHash = b.queryHash;
        this.createTime = b.createTime;
        this.endTime = b.endTime;
        this.executeTimeMillis = b.executeTimeMillis;
        this.queuedTimeMillis = b.queuedTimeMillis;
        this.planningTimeMillis = b.planningTimeMillis;
        this.wallTimeMillis = b.wallTimeMillis;
        this.cpuTimeMillis = b.cpuTimeMillis;
        this.physicalInputBytes = b.physicalInputBytes;
        this.physicalInputReadTimeMillis = b.physicalInputReadTimeMillis;
        this.physicalInputRows = b.physicalInputRows;
        this.processedInputBytes = b.processedInputBytes;
        this.processedInputRows = b.processedInputRows;
        this.outputBytes = b.outputBytes;
        this.outputRows = b.outputRows;
        this.peakUserMemoryBytes = b.peakUserMemoryBytes;
        this.peakTotalMemoryBytes = b.peakTotalMemoryBytes;
        this.serverAddress = b.serverAddress;
        this.inputsJson = b.inputsJson;
        this.outputsJson = b.outputsJson;
        this.tagsJson = b.tagsJson;
    }

    public static Builder builder() { return new Builder(); }

    public static final class Builder
    {
        public String queryId = "";
        public String queryState = "";
        public String errorCode;
        public String errorType;
        public String errorMessage;
        public String principal;
        public String user = "";
        public String source;
        public String catalog;
        public String schemaName;
        public String resourceGroupId;
        public String queryText = "";
        public String queryHash = "";
        public OffsetDateTime createTime;
        public OffsetDateTime endTime;
        public long executeTimeMillis;
        public long queuedTimeMillis;
        public long planningTimeMillis;
        public long wallTimeMillis;
        public long cpuTimeMillis;
        public long physicalInputBytes;
        public long physicalInputReadTimeMillis;
        public long physicalInputRows;
        public long processedInputBytes;
        public long processedInputRows;
        public long outputBytes;
        public long outputRows;
        public long peakUserMemoryBytes;
        public long peakTotalMemoryBytes;
        public String serverAddress;
        public String inputsJson = "[]";
        public String outputsJson = "[]";
        public String tagsJson = "{}";

        public ExtractedRow build()
        {
            return new ExtractedRow(this);
        }
    }
}
