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

/**
 * MBean delegate that reads from a {@link ListenerMetrics} snapshot per
 * attribute call. Each method takes one snapshot per invocation, which
 * keeps the bean stateless and identical to what the Prom exporter sees.
 */
public final class ListenerMetricsBean
        implements ListenerMBean
{
    private final ListenerMetrics metrics;

    public ListenerMetricsBean(ListenerMetrics metrics)
    {
        this.metrics = metrics;
    }

    @Override
    public long getEventsReceived()
    {
        return metrics.snapshot().events.getOrDefault("received", 0L);
    }

    @Override
    public long getEventsWritten()
    {
        return metrics.snapshot().events.getOrDefault("written", 0L);
    }

    @Override
    public long getEventsDropped()
    {
        return metrics.snapshot().events.getOrDefault("dropped", 0L);
    }

    @Override
    public long getQueueDepth()
    {
        return metrics.snapshot().queueDepth;
    }

    @Override
    public long getQueueCapacity()
    {
        return metrics.snapshot().queueCapacity;
    }

    @Override
    public long getWriteCount()
    {
        return metrics.snapshot().writeCount;
    }

    @Override
    public double getWriteSecondsSum()
    {
        return metrics.snapshot().writeSecondsSum;
    }

    @Override
    public long getWriteErrorsIcebergCommit()
    {
        return metrics.snapshot().writeErrors.getOrDefault("iceberg_commit", 0L);
    }

    @Override
    public long getWriteErrorsSerialization()
    {
        return metrics.snapshot().writeErrors.getOrDefault("serialization", 0L);
    }

    @Override
    public long getWriteErrorsUnknown()
    {
        return metrics.snapshot().writeErrors.getOrDefault("unknown", 0L);
    }

    @Override
    public long getDroppedQueueFull()
    {
        return metrics.snapshot().dropped.getOrDefault("queue_full", 0L);
    }

    @Override
    public long getDroppedLogOnly()
    {
        return metrics.snapshot().dropped.getOrDefault("log_only", 0L);
    }

    @Override
    public long getDroppedShutdown()
    {
        return metrics.snapshot().dropped.getOrDefault("shutdown", 0L);
    }
}
