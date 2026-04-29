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
 * JMX MBean surface for {@link ListenerMetrics}. Trino's
 * {@code jmx_prometheus_javaagent} sidecar will pick this up under
 * {@code io.shelf.listener:type=Listener} so operators get the same
 * metrics they have for shelfd's Rust surface even when the Prom HTTP
 * exporter (port 9099) is left disabled.
 */
public interface ListenerMBean
{
    long getEventsReceived();

    long getEventsWritten();

    long getEventsDropped();

    long getQueueDepth();

    long getQueueCapacity();

    long getWriteCount();

    double getWriteSecondsSum();

    long getWriteErrorsIcebergCommit();

    long getWriteErrorsSerialization();

    long getWriteErrorsUnknown();

    long getDroppedQueueFull();

    long getDroppedLogOnly();

    long getDroppedShutdown();
}
