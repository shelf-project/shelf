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

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class PromExporterTest
{
    @Test
    void rendersAllExpectedSeries()
    {
        ListenerMetrics m = new ListenerMetrics();
        m.recordEvent("received");
        m.recordEvent("received");
        m.recordEvent("written");
        m.recordEvent("dropped");
        m.recordWriteError("iceberg_commit");
        m.recordDropped("queue_full");
        m.recordDropped("queue_full");
        m.setQueueDepth(3);
        m.setQueueCapacity(8192);
        m.recordWriteSeconds(0.001);
        m.recordWriteSeconds(0.500);

        String text = PromExporter.render(m);
        assertThat(text).contains("shelf_listener_events_total{outcome=\"received\"} 2");
        assertThat(text).contains("shelf_listener_events_total{outcome=\"written\"} 1");
        assertThat(text).contains("shelf_listener_events_total{outcome=\"dropped\"} 1");
        assertThat(text).contains("shelf_listener_queue_depth 3");
        assertThat(text).contains("shelf_listener_queue_capacity 8192");
        assertThat(text).contains("shelf_listener_write_errors_total{reason=\"iceberg_commit\"} 1");
        assertThat(text).contains("shelf_listener_dropped_total{reason=\"queue_full\"} 2");
        assertThat(text).contains("shelf_listener_write_seconds_count 2");
    }

    @Test
    void rendersZeroSeriesForUnpopulatedLabels()
    {
        // Pre-populated labels mean even a quiescent listener exposes
        // every {outcome} / {reason} the dashboards rely on.
        ListenerMetrics m = new ListenerMetrics();
        String text = PromExporter.render(m);
        assertThat(text).contains("shelf_listener_events_total{outcome=\"written\"} 0");
        assertThat(text).contains("shelf_listener_events_total{outcome=\"dropped\"} 0");
        assertThat(text).contains("shelf_listener_dropped_total{reason=\"queue_full\"} 0");
        assertThat(text).contains("shelf_listener_dropped_total{reason=\"log_only\"} 0");
        assertThat(text).contains("shelf_listener_write_errors_total{reason=\"unknown\"} 0");
    }
}
