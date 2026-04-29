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

import java.time.Duration;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;

/**
 * Strongly-typed view of the {@code Map<String,String>} Trino hands to
 * {@code EventListenerFactory.create(...)}. Construction is parse-once
 * /never-mutate; every getter returns the parsed value or the documented
 * default.
 *
 * <p>The configuration matrix is the single source of truth for
 * {@code clients/trino-listener/README.md}; keep the two in sync.
 *
 * <h2>Keys</h2>
 *
 * <table>
 *   <caption>Listener configuration keys</caption>
 *   <tr><th>key</th><th>required</th><th>default</th><th>meaning</th></tr>
 *   <tr><td>{@code shelf.listener.iceberg.catalog}</td><td>yes</td><td>—</td>
 *       <td>Iceberg catalog name as configured at the listener layer (the
 *           listener instantiates its own catalog handle via
 *           {@code CatalogUtil.loadCatalog}, independent of any Trino
 *           connector). Pair with {@code iceberg.catalog-impl} for the
 *           Java class name.</td></tr>
 *   <tr><td>{@code shelf.listener.iceberg.table}</td><td>yes</td><td>—</td>
 *       <td>Fully-qualified {@code <schema>.<table>}. The listener
 *           auto-creates the table on first commit if absent.</td></tr>
 *   <tr><td>{@code shelf.listener.iceberg.catalog-impl}</td><td>no</td>
 *       <td>{@code org.apache.iceberg.hadoop.HadoopCatalog}</td>
 *       <td>Class name passed to {@code CatalogUtil.loadCatalog}. Override
 *           with HiveCatalog / GlueCatalog / RestCatalog for production.</td></tr>
 *   <tr><td>{@code shelf.listener.iceberg.warehouse}</td><td>no</td><td>—</td>
 *       <td>Warehouse path. Required by HadoopCatalog and HiveCatalog.</td></tr>
 *   <tr><td>{@code shelf.listener.iceberg.*}</td><td>—</td><td>—</td>
 *       <td>Any other key under this prefix is forwarded verbatim (with
 *           the prefix stripped) into the catalog properties map. Use
 *           this for {@code uri}, {@code s3.endpoint}, {@code io-impl},
 *           {@code s3.region} etc.</td></tr>
 *   <tr><td>{@code shelf.listener.batch.max-rows}</td><td>no</td><td>{@code 1000}</td>
 *       <td>Flush trigger by row count.</td></tr>
 *   <tr><td>{@code shelf.listener.batch.max-interval-secs}</td><td>no</td><td>{@code 30}</td>
 *       <td>Flush trigger by wall time. The smaller of the two wins.</td></tr>
 *   <tr><td>{@code shelf.listener.queue.capacity}</td><td>no</td><td>{@code 8192}</td>
 *       <td>Hard cap on in-memory event count.</td></tr>
 *   <tr><td>{@code shelf.listener.queue.block-timeout-ms}</td><td>no</td><td>{@code 50}</td>
 *       <td>Maximum block under {@link FailMode#BLOCK}; after this we drop.</td></tr>
 *   <tr><td>{@code shelf.listener.write.enabled}</td><td>no</td><td>{@code true}</td>
 *       <td>Kill switch — when {@code false}, events are received but no
 *           Iceberg writes occur (mirrors {@code log_only}).</td></tr>
 *   <tr><td>{@code shelf.listener.fail-mode}</td><td>no</td><td>{@code drop}</td>
 *       <td>One of {@code drop}, {@code block}, {@code log_only}.</td></tr>
 *   <tr><td>{@code shelf.listener.query-text-max-bytes}</td><td>no</td><td>{@code 65536}</td>
 *       <td>Hard cap on the {@code query_text} column to prevent runaway rows.</td></tr>
 *   <tr><td>{@code shelf.listener.metrics.prometheus.enabled}</td><td>no</td><td>{@code false}</td>
 *       <td>Bind a {@code GET /metrics} HTTP server (off by default — most
 *           operators scrape the JMX MBean instead).</td></tr>
 *   <tr><td>{@code shelf.listener.metrics.prometheus.port}</td><td>no</td><td>{@code 9099}</td>
 *       <td>TCP port for the Prometheus endpoint.</td></tr>
 *   <tr><td>{@code shelf.listener.metrics.prometheus.bind}</td><td>no</td><td>{@code 0.0.0.0}</td>
 *       <td>Bind address for the Prometheus endpoint.</td></tr>
 * </table>
 */
public final class ListenerConfig
{
    public static final String K_ICEBERG_PREFIX = "shelf.listener.iceberg.";
    public static final String K_ICEBERG_CATALOG = "shelf.listener.iceberg.catalog";
    public static final String K_ICEBERG_TABLE = "shelf.listener.iceberg.table";
    public static final String K_ICEBERG_CATALOG_IMPL = "shelf.listener.iceberg.catalog-impl";

    public static final String K_BATCH_MAX_ROWS = "shelf.listener.batch.max-rows";
    public static final String K_BATCH_MAX_INTERVAL_SECS = "shelf.listener.batch.max-interval-secs";

    public static final String K_QUEUE_CAPACITY = "shelf.listener.queue.capacity";
    public static final String K_QUEUE_BLOCK_TIMEOUT_MS = "shelf.listener.queue.block-timeout-ms";

    public static final String K_WRITE_ENABLED = "shelf.listener.write.enabled";
    public static final String K_FAIL_MODE = "shelf.listener.fail-mode";

    public static final String K_QUERY_TEXT_MAX_BYTES = "shelf.listener.query-text-max-bytes";

    public static final String K_PROM_ENABLED = "shelf.listener.metrics.prometheus.enabled";
    public static final String K_PROM_PORT = "shelf.listener.metrics.prometheus.port";
    public static final String K_PROM_BIND = "shelf.listener.metrics.prometheus.bind";

    public static final String DEFAULT_CATALOG_IMPL = "org.apache.iceberg.hadoop.HadoopCatalog";

    public static final int DEFAULT_BATCH_MAX_ROWS = 1000;
    public static final int DEFAULT_BATCH_MAX_INTERVAL_SECS = 30;
    public static final int DEFAULT_QUEUE_CAPACITY = 8192;
    public static final long DEFAULT_BLOCK_TIMEOUT_MS = 50L;
    public static final int DEFAULT_QUERY_TEXT_MAX_BYTES = 65_536;
    public static final int DEFAULT_PROM_PORT = 9099;
    public static final String DEFAULT_PROM_BIND = "0.0.0.0";

    private final String catalogName;
    private final String catalogImpl;
    private final String tableSchema;
    private final String tableName;
    private final Map<String, String> catalogProperties;
    private final int batchMaxRows;
    private final Duration batchMaxInterval;
    private final int queueCapacity;
    private final Duration queueBlockTimeout;
    private final boolean writeEnabled;
    private final FailMode failMode;
    private final int queryTextMaxBytes;
    private final boolean prometheusEnabled;
    private final int prometheusPort;
    private final String prometheusBind;

    private ListenerConfig(Builder b)
    {
        this.catalogName = b.catalogName;
        this.catalogImpl = b.catalogImpl;
        this.tableSchema = b.tableSchema;
        this.tableName = b.tableName;
        this.catalogProperties = Collections.unmodifiableMap(new LinkedHashMap<>(b.catalogProperties));
        this.batchMaxRows = b.batchMaxRows;
        this.batchMaxInterval = b.batchMaxInterval;
        this.queueCapacity = b.queueCapacity;
        this.queueBlockTimeout = b.queueBlockTimeout;
        this.writeEnabled = b.writeEnabled;
        this.failMode = b.failMode;
        this.queryTextMaxBytes = b.queryTextMaxBytes;
        this.prometheusEnabled = b.prometheusEnabled;
        this.prometheusPort = b.prometheusPort;
        this.prometheusBind = b.prometheusBind;
    }

    public static ListenerConfig fromMap(Map<String, String> raw)
    {
        Objects.requireNonNull(raw, "config map");
        Builder b = new Builder();
        b.catalogName = require(raw, K_ICEBERG_CATALOG);
        String fqTable = require(raw, K_ICEBERG_TABLE);
        int dot = fqTable.indexOf('.');
        if (dot <= 0 || dot == fqTable.length() - 1) {
            throw new IllegalArgumentException(
                    K_ICEBERG_TABLE + " must be <schema>.<table>; got: " + fqTable);
        }
        b.tableSchema = fqTable.substring(0, dot);
        b.tableName = fqTable.substring(dot + 1);
        b.catalogImpl = raw.getOrDefault(K_ICEBERG_CATALOG_IMPL, DEFAULT_CATALOG_IMPL);
        b.batchMaxRows = parseInt(raw, K_BATCH_MAX_ROWS, DEFAULT_BATCH_MAX_ROWS, 1, 1_000_000);
        b.batchMaxInterval = Duration.ofSeconds(parseInt(
                raw, K_BATCH_MAX_INTERVAL_SECS, DEFAULT_BATCH_MAX_INTERVAL_SECS, 1, 3600));
        b.queueCapacity = parseInt(raw, K_QUEUE_CAPACITY, DEFAULT_QUEUE_CAPACITY, 16, 1_000_000);
        b.queueBlockTimeout = Duration.ofMillis(parseLong(
                raw, K_QUEUE_BLOCK_TIMEOUT_MS, DEFAULT_BLOCK_TIMEOUT_MS, 0L, 10_000L));
        b.writeEnabled = parseBool(raw, K_WRITE_ENABLED, true);
        b.failMode = FailMode.parse(raw.get(K_FAIL_MODE));
        b.queryTextMaxBytes = parseInt(
                raw, K_QUERY_TEXT_MAX_BYTES, DEFAULT_QUERY_TEXT_MAX_BYTES, 256, 16 * 1024 * 1024);
        b.prometheusEnabled = parseBool(raw, K_PROM_ENABLED, false);
        b.prometheusPort = parseInt(raw, K_PROM_PORT, DEFAULT_PROM_PORT, 1, 65_535);
        b.prometheusBind = raw.getOrDefault(K_PROM_BIND, DEFAULT_PROM_BIND);

        Map<String, String> catProps = new LinkedHashMap<>();
        for (Map.Entry<String, String> e : raw.entrySet()) {
            String k = e.getKey();
            if (k.startsWith(K_ICEBERG_PREFIX)
                    && !k.equals(K_ICEBERG_CATALOG)
                    && !k.equals(K_ICEBERG_TABLE)
                    && !k.equals(K_ICEBERG_CATALOG_IMPL)) {
                catProps.put(k.substring(K_ICEBERG_PREFIX.length()), e.getValue());
            }
        }
        b.catalogProperties = catProps;
        return new ListenerConfig(b);
    }

    public String catalogName() { return catalogName; }
    public String catalogImpl() { return catalogImpl; }
    public String tableSchema() { return tableSchema; }
    public String tableName() { return tableName; }
    public Map<String, String> catalogProperties() { return catalogProperties; }
    public int batchMaxRows() { return batchMaxRows; }
    public Duration batchMaxInterval() { return batchMaxInterval; }
    public int queueCapacity() { return queueCapacity; }
    public Duration queueBlockTimeout() { return queueBlockTimeout; }
    public boolean writeEnabled() { return writeEnabled; }
    public FailMode failMode() { return failMode; }
    public int queryTextMaxBytes() { return queryTextMaxBytes; }
    public boolean prometheusEnabled() { return prometheusEnabled; }
    public int prometheusPort() { return prometheusPort; }
    public String prometheusBind() { return prometheusBind; }

    private static String require(Map<String, String> raw, String key)
    {
        String v = raw.get(key);
        if (v == null || v.isBlank()) {
            throw new IllegalArgumentException("Missing required configuration: " + key);
        }
        return v;
    }

    private static int parseInt(Map<String, String> raw, String key, int dflt, int min, int max)
    {
        String v = raw.get(key);
        if (v == null || v.isBlank()) {
            return dflt;
        }
        int n;
        try {
            n = Integer.parseInt(v.trim());
        }
        catch (NumberFormatException ex) {
            throw new IllegalArgumentException(key + " must be an integer; got: " + v);
        }
        if (n < min || n > max) {
            throw new IllegalArgumentException(key + " out of range [" + min + ", " + max + "]: " + n);
        }
        return n;
    }

    private static long parseLong(Map<String, String> raw, String key, long dflt, long min, long max)
    {
        String v = raw.get(key);
        if (v == null || v.isBlank()) {
            return dflt;
        }
        long n;
        try {
            n = Long.parseLong(v.trim());
        }
        catch (NumberFormatException ex) {
            throw new IllegalArgumentException(key + " must be a long; got: " + v);
        }
        if (n < min || n > max) {
            throw new IllegalArgumentException(key + " out of range [" + min + ", " + max + "]: " + n);
        }
        return n;
    }

    private static boolean parseBool(Map<String, String> raw, String key, boolean dflt)
    {
        String v = raw.get(key);
        if (v == null) {
            return dflt;
        }
        String t = v.trim().toLowerCase(Locale.ROOT);
        if (t.equals("true") || t.equals("yes") || t.equals("1")) {
            return true;
        }
        if (t.equals("false") || t.equals("no") || t.equals("0")) {
            return false;
        }
        throw new IllegalArgumentException(key + " must be a boolean; got: " + v);
    }

    /** Mutable builder used internally + by tests that synthesize a config. */
    public static final class Builder
    {
        public String catalogName;
        public String catalogImpl = DEFAULT_CATALOG_IMPL;
        public String tableSchema;
        public String tableName;
        public Map<String, String> catalogProperties = new LinkedHashMap<>();
        public int batchMaxRows = DEFAULT_BATCH_MAX_ROWS;
        public Duration batchMaxInterval = Duration.ofSeconds(DEFAULT_BATCH_MAX_INTERVAL_SECS);
        public int queueCapacity = DEFAULT_QUEUE_CAPACITY;
        public Duration queueBlockTimeout = Duration.ofMillis(DEFAULT_BLOCK_TIMEOUT_MS);
        public boolean writeEnabled = true;
        public FailMode failMode = FailMode.DROP;
        public int queryTextMaxBytes = DEFAULT_QUERY_TEXT_MAX_BYTES;
        public boolean prometheusEnabled;
        public int prometheusPort = DEFAULT_PROM_PORT;
        public String prometheusBind = DEFAULT_PROM_BIND;

        public ListenerConfig build()
        {
            Objects.requireNonNull(catalogName, "catalogName");
            Objects.requireNonNull(tableSchema, "tableSchema");
            Objects.requireNonNull(tableName, "tableName");
            return new ListenerConfig(this);
        }
    }

}
