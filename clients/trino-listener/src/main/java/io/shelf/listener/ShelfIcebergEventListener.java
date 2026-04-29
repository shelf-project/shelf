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
package io.shelf.listener;

import io.shelf.listener.config.FailMode;
import io.shelf.listener.config.ListenerConfig;
import io.shelf.listener.extract.EventExtractor;
import io.shelf.listener.extract.ExtractedRow;
import io.shelf.listener.metrics.ListenerMBean;
import io.shelf.listener.metrics.ListenerMetrics;
import io.shelf.listener.metrics.ListenerMetricsBean;
import io.shelf.listener.metrics.PromExporter;
import io.shelf.listener.writer.IcebergSink;
import io.shelf.listener.writer.WriterThread;
import io.trino.spi.eventlistener.EventListener;
import io.trino.spi.eventlistener.QueryCompletedEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.management.MBeanServer;
import javax.management.ObjectName;

import java.lang.management.ManagementFactory;
import java.util.Objects;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * SHELF-37 event listener. Captures every {@link QueryCompletedEvent}
 * into a configurable Iceberg table.
 *
 * <p><b>Coordinator-thread safety.</b> {@code queryCompleted} runs on a
 * Trino coordinator thread. We extract synchronously (fast — bytes
 * already in the SPI object) but the Iceberg write is dispatched onto
 * the {@link WriterThread} via a bounded queue; under
 * {@link FailMode#DROP} the SPI thread <em>never</em> blocks longer than
 * the {@link BlockingQueue#offer(Object)} call itself.
 */
public final class ShelfIcebergEventListener
        implements EventListener, AutoCloseable
{
    private static final Logger LOG = LoggerFactory.getLogger(ShelfIcebergEventListener.class);

    private final ListenerConfig config;
    private final EventExtractor extractor;
    private final BlockingQueue<ExtractedRow> queue;
    private final ListenerMetrics metrics;
    private final IcebergSink sink;
    private final WriterThread writerThread;
    private final PromExporter promExporter;
    private final ObjectName mbeanName;
    private final AtomicLong logOnlyCounter = new AtomicLong();

    public ShelfIcebergEventListener(ListenerConfig config)
    {
        this(config, openSinkOrNull(config));
    }

    /**
     * Test-friendly constructor. When {@code sink} is non-null the
     * writer thread starts; when it is {@code null} the listener still
     * runs (extracts + counts) but every flush call is short-circuited
     * to the dropped-shutdown counter — useful for failure-mode tests.
     */
    public ShelfIcebergEventListener(ListenerConfig config, IcebergSink sink)
    {
        this.config = Objects.requireNonNull(config, "config");
        this.extractor = new EventExtractor(config.queryTextMaxBytes());
        this.queue = new ArrayBlockingQueue<>(config.queueCapacity());
        this.metrics = new ListenerMetrics();
        this.metrics.setQueueCapacity(config.queueCapacity());
        this.sink = sink;
        if (config.writeEnabled() && sink != null) {
            this.writerThread = new WriterThread(queue, sink, metrics, config);
            this.writerThread.start();
        }
        else {
            this.writerThread = null;
        }

        this.promExporter = openPromExporter(config, metrics);
        this.mbeanName = registerMbean(metrics);
    }

    @Override
    public void queryCompleted(QueryCompletedEvent event)
    {
        if (event == null) {
            return;
        }
        metrics.recordEvent("received");

        if (!config.writeEnabled() || config.failMode() == FailMode.LOG_ONLY) {
            metrics.recordDropped("log_only");
            // Throttled WARN every 1024 events so log_only does not silently lie.
            if ((logOnlyCounter.incrementAndGet() & 1023L) == 0L) {
                LOG.warn("listener in log_only mode; dropped {} events so far",
                        logOnlyCounter.get());
            }
            return;
        }

        ExtractedRow row;
        try {
            row = extractor.extract(event);
        }
        catch (Throwable t) {
            metrics.recordWriteError("serialization");
            metrics.recordEvent("dropped");
            LOG.warn("extract failed; query_id={}",
                    safe(event.getMetadata().getQueryId()), t);
            return;
        }

        boolean enqueued;
        if (config.failMode() == FailMode.BLOCK) {
            try {
                enqueued = queue.offer(row,
                        config.queueBlockTimeout().toMillis(), TimeUnit.MILLISECONDS);
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                enqueued = false;
            }
        }
        else {
            enqueued = queue.offer(row);
        }

        metrics.setQueueDepth(queue.size());
        if (!enqueued) {
            metrics.recordDropped("queue_full");
            metrics.recordEvent("dropped");
        }
    }

    @Override
    public void close()
    {
        if (writerThread != null) {
            writerThread.shutdown(5_000);
        }
        if (sink != null) {
            try {
                sink.close();
            }
            catch (Exception e) {
                LOG.debug("sink close failed", e);
            }
        }
        if (promExporter != null) {
            try {
                promExporter.close();
            }
            catch (RuntimeException e) {
                LOG.debug("prom exporter close failed", e);
            }
        }
        if (mbeanName != null) {
            try {
                ManagementFactory.getPlatformMBeanServer().unregisterMBean(mbeanName);
            }
            catch (Exception e) {
                LOG.debug("mbean unregister failed", e);
            }
        }
    }

    /** Visible for tests: read-only metrics handle. */
    public ListenerMetrics metrics()
    {
        return metrics;
    }

    /** Visible for tests: current queue depth. */
    public int queueDepth()
    {
        return queue.size();
    }

    /** Visible for tests: read-only config. */
    public ListenerConfig config()
    {
        return config;
    }

    private static IcebergSink openSinkOrNull(ListenerConfig config)
    {
        if (!config.writeEnabled()) {
            return null;
        }
        try {
            return new IcebergSink(config);
        }
        catch (RuntimeException e) {
            // We intentionally swallow rather than fail Trino startup —
            // operators rarely want a misconfigured listener to take down
            // a coordinator. The metrics surface still reports zero
            // writes; alerting on that is the operator's responsibility.
            LOG.warn("Iceberg sink failed to open; running in log_only mode: {}", e.toString());
            return null;
        }
    }

    private static PromExporter openPromExporter(ListenerConfig config, ListenerMetrics metrics)
    {
        if (!config.prometheusEnabled()) {
            return null;
        }
        try {
            return new PromExporter(config.prometheusBind(), config.prometheusPort(), metrics);
        }
        catch (java.io.IOException e) {
            LOG.warn("Prometheus exporter failed to bind {}:{}: {}",
                    config.prometheusBind(), config.prometheusPort(), e.toString());
            return null;
        }
    }

    private static ObjectName registerMbean(ListenerMetrics metrics)
    {
        try {
            ObjectName name = new ObjectName("io.shelf.listener:type=Listener");
            MBeanServer server = ManagementFactory.getPlatformMBeanServer();
            if (!server.isRegistered(name)) {
                ListenerMBean bean = new ListenerMetricsBean(metrics);
                server.registerMBean(bean, name);
            }
            return name;
        }
        catch (Exception e) {
            LOG.debug("MBean registration failed", e);
            return null;
        }
    }

    private static String safe(String s)
    {
        return s == null ? "<unknown>" : s;
    }
}
