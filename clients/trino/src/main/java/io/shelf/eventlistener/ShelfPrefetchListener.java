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
package io.shelf.eventlistener;

import io.shelf.config.ShelfConfig;
import io.trino.spi.eventlistener.EventListener;
import io.trino.spi.eventlistener.QueryCompletedEvent;
import io.trino.spi.eventlistener.QueryCreatedEvent;
import io.trino.spi.eventlistener.QueryMetadata;
import io.trino.spi.eventlistener.QueryStatistics;

import java.util.List;
import java.util.Objects;

/**
 * Coordinator-side {@link EventListener} that drives plan-aware push prefetch
 * (BLUEPRINT §7.2) and post-hoc operator-summary learning.
 *
 * <p><b>ADR-0005 compliance.</b> This listener does <em>not</em> rely on
 * {@code EventListener#splitCompleted} — that SPI was removed in Trino PR
 * #26436 (merged 2025-08-19). Row-group prefetch is instead driven by
 * plugin-side observation of footer reads inside {@code ShelfFileSystem}
 * (phase 2b-signal-1), with post-hoc learning from
 * {@link QueryStatistics#getOperatorSummaries()} on
 * {@link #queryCompleted(QueryCompletedEvent)}.
 *
 * <p><b>Coordinator-thread safety.</b> Both hooks run on the Trino coordinator
 * thread. Every path inside them is bounded by a 10 ms hard deadline; on any
 * failure we log at DEBUG and return. See risk row R-09 in 03-plan.md §5.
 */
public final class ShelfPrefetchListener
        implements EventListener
{
    private final ShelfConfig config;
    private final PrefetchClient client;

    public ShelfPrefetchListener(ShelfConfig config, PrefetchClient client)
    {
        this.config = Objects.requireNonNull(config, "config");
        this.client = Objects.requireNonNull(client, "client");
    }

    @Override
    public void queryCreated(QueryCreatedEvent event)
    {
        Objects.requireNonNull(event, "event");
        QueryMetadata metadata = event.getMetadata();
        // TODO(SHELF-PHASE-2): extract tables + predicates from QueryMetadata.
        //   metadata.getTables() yields referenced tables; metadata.getJsonPlan()
        //   (if present) carries filter predicates — shape confirmed by E1.
        //   On confirmed signal, fire PrefetchRequest with priority 0 for dashboard
        //   queries and priority 10 for bulk. Hard 10 ms deadline per R-09.
        //   See 03-plan.md §3 Phase 2 + BLUEPRINT §7.2.
        @SuppressWarnings("unused")
        List<?> tables = metadata.getTables();
    }

    @Override
    public void queryCompleted(QueryCompletedEvent event)
    {
        Objects.requireNonNull(event, "event");
        QueryStatistics statistics = event.getStatistics();
        // TODO(SHELF-PHASE-2): pull operatorSummaries (ADR-0005 post-hoc learning
        //   path). Aggregate into (query_sketch -> likely_row_groups) via the
        //   nightly trainer. Also cancel any in-flight prefetch for this queryId.
        @SuppressWarnings("unused")
        List<String> operatorSummaries = statistics.getOperatorSummaries();
        // Retain direct reference to avoid unused-field warnings in the skeleton.
        client.cancel(event.getMetadata().getQueryId());
    }

    /** Exposed for unit-test shapes (see {@code ShelfPrefetchListenerTest}). */
    public ShelfConfig config()
    {
        return config;
    }
}
