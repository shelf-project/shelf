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

import io.shelf.listener.config.ListenerConfig;
import io.shelf.listener.extract.ExtractedRow;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionKey;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.GenericAppenderFactory;
import org.apache.iceberg.data.InternalRecordWrapper;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.io.FileAppenderFactory;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.io.PartitionedFanoutWriter;
import org.apache.iceberg.io.WriteResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Single-table Iceberg sink. Owns the {@link Catalog} handle, the
 * {@link Table}, and a partition-key + record-wrapper that the writer
 * thread reuses across batches. Per the SHELF-37 acceptance criteria
 * we open a fresh {@link OutputFileFactory} on every {@link #write}
 * call so writers never straddle the in-memory batch boundary.
 *
 * <p>Append-only: every flush ends in {@code newAppend().commit()}. No
 * MERGE, no UPDATE — the listener is a tail writer and consumers do
 * idempotent reads ({@code DISTINCT query_id}). Failed commits are
 * surfaced to the writer thread which counts them under
 * {@code shelf_listener_write_errors_total{reason="iceberg_commit"}}.
 */
public final class IcebergSink
        implements AutoCloseable
{
    private static final Logger LOG = LoggerFactory.getLogger(IcebergSink.class);

    private final Catalog catalog;
    private final Table table;
    private final long targetFileSize;
    private final AtomicLong taskCounter = new AtomicLong();

    public IcebergSink(ListenerConfig cfg)
    {
        Configuration hadoop = new Configuration();
        cfg.catalogProperties().forEach(hadoop::set);

        java.util.Map<String, String> props = new java.util.LinkedHashMap<>(cfg.catalogProperties());
        // HadoopCatalog wants the warehouse on `warehouse`. Operators set it
        // via shelf.listener.iceberg.warehouse, which we already forwarded.
        this.catalog = CatalogUtil.loadCatalog(
                cfg.catalogImpl(), cfg.catalogName(), props, hadoop);

        Namespace ns = Namespace.of(cfg.tableSchema());
        if (this.catalog instanceof SupportsNamespaces sn) {
            try {
                if (!sn.namespaceExists(ns)) {
                    sn.createNamespace(ns);
                }
            }
            catch (RuntimeException e) {
                // Race-tolerant — another writer may have created it concurrently.
                LOG.debug("namespace create raced; continuing", e);
            }
        }

        TableIdentifier id = TableIdentifier.of(ns, cfg.tableName());
        if (catalog.tableExists(id)) {
            this.table = catalog.loadTable(id);
        }
        else {
            this.table = catalog.createTable(
                    id,
                    IcebergSchema.SCHEMA,
                    IcebergSchema.SPEC,
                    java.util.Map.of("write.format.default", "parquet"));
        }
        this.targetFileSize = 64L * 1024 * 1024;
    }

    /** Test-only constructor that uses an externally constructed catalog. */
    IcebergSink(Catalog catalog, Table table)
    {
        this.catalog = catalog;
        this.table = table;
        this.targetFileSize = 64L * 1024 * 1024;
    }

    public Table table() { return table; }

    /**
     * Write a batch and append it as a single Iceberg snapshot. Throws on
     * any failure; the caller decides counter vs surfaced error.
     */
    public int write(List<ExtractedRow> batch)
            throws IOException
    {
        if (batch.isEmpty()) {
            return 0;
        }
        long taskId = taskCounter.getAndIncrement();
        OutputFileFactory fileFactory = OutputFileFactory.builderFor(table, /*partitionId=*/0, taskId)
                .format(FileFormat.PARQUET)
                .operationId(UUID.randomUUID().toString())
                .build();

        FileAppenderFactory<Record> appenderFactory = new GenericAppenderFactory(
                table.schema(), table.spec());

        try (RowFanoutWriter writer = new RowFanoutWriter(
                table.spec(),
                FileFormat.PARQUET,
                appenderFactory,
                fileFactory,
                table.io(),
                targetFileSize,
                table.schema())) {
            for (ExtractedRow row : batch) {
                writer.write(IcebergSchema.toRecord(row));
            }
            WriteResult result = writer.complete();
            DataFile[] dataFiles = result.dataFiles();
            if (dataFiles.length == 0) {
                return 0;
            }
            AppendFiles append = table.newAppend();
            for (DataFile df : dataFiles) {
                append.appendFile(df);
            }
            append.commit();
            return batch.size();
        }
    }

    @Override
    public void close()
    {
        if (catalog instanceof AutoCloseable c) {
            try {
                c.close();
            }
            catch (Exception e) {
                LOG.debug("catalog close failed", e);
            }
        }
    }

    /**
     * {@link PartitionedFanoutWriter} subclass that drives the partition
     * key from the record itself. Iceberg-data does not ship a generic
     * implementation; this is the conventional ~10-line bridge.
     */
    private static final class RowFanoutWriter
            extends PartitionedFanoutWriter<Record>
    {
        private final PartitionKey partitionKey;
        private final InternalRecordWrapper wrapper;

        RowFanoutWriter(
                org.apache.iceberg.PartitionSpec spec,
                FileFormat format,
                FileAppenderFactory<Record> appenderFactory,
                OutputFileFactory fileFactory,
                org.apache.iceberg.io.FileIO io,
                long targetFileSize,
                org.apache.iceberg.Schema schema)
        {
            super(spec, format, appenderFactory, fileFactory, io, targetFileSize);
            this.partitionKey = new PartitionKey(spec, schema);
            this.wrapper = new InternalRecordWrapper(schema.asStruct());
        }

        @Override
        protected PartitionKey partition(Record record)
        {
            partitionKey.partition(wrapper.wrap(record));
            return partitionKey;
        }
    }
}
