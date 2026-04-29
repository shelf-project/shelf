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
package io.shelf.listener.metrics;

import com.sun.net.httpserver.HttpServer;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.Executors;

/**
 * Tiny Prometheus text-format exporter on {@code GET /metrics}. Backed by
 * the JDK's {@code com.sun.net.httpserver} so the listener pulls in no
 * extra HTTP / metrics dependency.
 *
 * <p>Off by default — most operators scrape the JMX bean via
 * {@code jmx_prometheus_javaagent} that already runs in every Trino pod
 * (see AGENTS.md). The HTTP path exists for clusters that prefer a
 * direct Prom scrape target on a dedicated port.
 */
public final class PromExporter
        implements AutoCloseable
{
    private final HttpServer server;

    public PromExporter(String bindAddress, int port, ListenerMetrics metrics)
            throws IOException
    {
        InetSocketAddress addr = new InetSocketAddress(bindAddress, port);
        HttpServer server = HttpServer.create(addr, 0);
        server.setExecutor(Executors.newSingleThreadExecutor(r -> {
            Thread t = new Thread(r, "shelf-listener-metrics");
            t.setDaemon(true);
            return t;
        }));
        server.createContext("/metrics", exchange -> {
            try {
                byte[] payload = render(metrics).getBytes(StandardCharsets.UTF_8);
                exchange.getResponseHeaders().set("Content-Type", "text/plain; version=0.0.4");
                exchange.sendResponseHeaders(200, payload.length);
                try (OutputStream os = exchange.getResponseBody()) {
                    os.write(payload);
                }
            }
            finally {
                exchange.close();
            }
        });
        server.createContext("/healthz", exchange -> {
            try {
                byte[] payload = "ok\n".getBytes(StandardCharsets.UTF_8);
                exchange.sendResponseHeaders(200, payload.length);
                try (OutputStream os = exchange.getResponseBody()) {
                    os.write(payload);
                }
            }
            finally {
                exchange.close();
            }
        });
        server.start();
        this.server = server;
    }

    public int port()
    {
        return server.getAddress().getPort();
    }

    static String render(ListenerMetrics metrics)
    {
        ListenerMetrics.Snapshot s = metrics.snapshot();
        StringBuilder out = new StringBuilder(2048);

        out.append("# HELP shelf_listener_events_total Lifecycle counter for events seen by the listener.\n");
        out.append("# TYPE shelf_listener_events_total counter\n");
        for (Map.Entry<String, Long> e : s.events.entrySet()) {
            out.append("shelf_listener_events_total{outcome=\"")
                    .append(escape(e.getKey()))
                    .append("\"} ")
                    .append(e.getValue())
                    .append('\n');
        }

        out.append("# HELP shelf_listener_queue_depth Current ingest queue depth.\n");
        out.append("# TYPE shelf_listener_queue_depth gauge\n");
        out.append("shelf_listener_queue_depth ").append(s.queueDepth).append('\n');

        out.append("# HELP shelf_listener_queue_capacity Configured ingest queue capacity.\n");
        out.append("# TYPE shelf_listener_queue_capacity gauge\n");
        out.append("shelf_listener_queue_capacity ").append(s.queueCapacity).append('\n');

        out.append("# HELP shelf_listener_write_seconds Iceberg flush latency (seconds).\n");
        out.append("# TYPE shelf_listener_write_seconds histogram\n");
        for (int i = 0; i < ListenerMetrics.WRITE_BUCKETS.length; i++) {
            out.append("shelf_listener_write_seconds_bucket{le=\"")
                    .append(ListenerMetrics.WRITE_BUCKETS[i])
                    .append("\"} ")
                    .append(s.writeBucketsCumulative[i])
                    .append('\n');
        }
        out.append("shelf_listener_write_seconds_bucket{le=\"+Inf\"} ")
                .append(s.writeBucketsCumulative[s.writeBucketsCumulative.length - 1])
                .append('\n');
        out.append("shelf_listener_write_seconds_sum ").append(s.writeSecondsSum).append('\n');
        out.append("shelf_listener_write_seconds_count ").append(s.writeCount).append('\n');

        out.append("# HELP shelf_listener_write_errors_total Failure counter by reason.\n");
        out.append("# TYPE shelf_listener_write_errors_total counter\n");
        for (Map.Entry<String, Long> e : s.writeErrors.entrySet()) {
            out.append("shelf_listener_write_errors_total{reason=\"")
                    .append(escape(e.getKey()))
                    .append("\"} ")
                    .append(e.getValue())
                    .append('\n');
        }

        out.append("# HELP shelf_listener_dropped_total Dropped events by reason.\n");
        out.append("# TYPE shelf_listener_dropped_total counter\n");
        for (Map.Entry<String, Long> e : s.dropped.entrySet()) {
            out.append("shelf_listener_dropped_total{reason=\"")
                    .append(escape(e.getKey()))
                    .append("\"} ")
                    .append(e.getValue())
                    .append('\n');
        }
        return out.toString();
    }

    private static String escape(String s)
    {
        return s.replace("\\", "\\\\").replace("\"", "\\\"").replace("\n", "\\n");
    }

    @Override
    public void close()
    {
        server.stop(0);
    }
}
