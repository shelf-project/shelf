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
package io.shelf.listener.support;

import io.trino.spi.ErrorCode;
import io.trino.spi.ErrorType;
import io.trino.spi.eventlistener.QueryCompletedEvent;
import io.trino.spi.eventlistener.QueryContext;
import io.trino.spi.eventlistener.QueryFailureInfo;
import io.trino.spi.eventlistener.QueryIOMetadata;
import io.trino.spi.eventlistener.QueryInputMetadata;
import io.trino.spi.eventlistener.QueryMetadata;
import io.trino.spi.eventlistener.QueryOutputMetadata;
import io.trino.spi.eventlistener.QueryStatistics;
import io.trino.spi.eventlistener.RoutineInfo;
import io.trino.spi.eventlistener.TableInfo;
import io.trino.spi.metrics.Metrics;
import io.trino.spi.resourcegroups.ResourceGroupId;
import io.trino.spi.session.ResourceEstimates;

import java.net.URI;
import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.function.Consumer;

/**
 * Test helper that builds {@link QueryCompletedEvent} instances with
 * sensible defaults so individual tests only override the fields they
 * care about.
 *
 * <p>The Trino SPI constructors are very wide ({@code QueryStatistics}
 * takes 41 positional args); without this helper every test would
 * recreate the same boilerplate.
 */
public final class TestEvents
{
    private TestEvents() {}

    public static QueryCompletedEvent canonical()
    {
        return canonical(o -> {});
    }

    public static QueryCompletedEvent canonical(Consumer<Override> tweak)
    {
        Override o = new Override();
        tweak.accept(o);
        return build(o);
    }

    /** Mutable knobs the tests poke. Construction is value-by-value. */
    public static final class Override
    {
        public String queryId = "20260430_000000_00001_oss";
        public String queryState = "FINISHED";
        public String query = "SELECT 1";
        public String user = "alice";
        public Optional<String> principal = Optional.of("alice@dc.example");
        public Optional<String> source = Optional.of("trino-cli");
        public Optional<String> catalog = Optional.of("hive");
        public Optional<String> schema = Optional.of("default");
        public Optional<ResourceGroupId> resourceGroupId =
                Optional.of(new ResourceGroupId(List.of("global", "interactive")));
        public Map<String, String> sessionProperties = Collections.emptyMap();
        public String serverAddress = "10.0.0.42";
        public Optional<QueryFailureInfo> failure = Optional.empty();

        public Duration cpuTime = Duration.ofMillis(123);
        public Duration wallTime = Duration.ofMillis(456);
        public Duration queuedTime = Duration.ofMillis(7);
        public Optional<Duration> planningTime = Optional.of(Duration.ofMillis(8));
        public Optional<Duration> executionTime = Optional.of(Duration.ofMillis(440));
        public Optional<Duration> physicalInputReadTime = Optional.of(Duration.ofMillis(99));
        public long peakUserMemoryBytes = 1024;
        public long peakTaskTotalMemory = 2048;
        public long physicalInputBytes = 1_000_000;
        public long physicalInputRows = 10_000;
        public long processedInputBytes = 950_000;
        public long processedInputRows = 9_500;
        public long outputBytes = 1024;
        public long outputRows = 16;

        public List<QueryInputMetadata> inputs = List.of();
        public Optional<QueryOutputMetadata> output = Optional.empty();
        public List<TableInfo> tables = List.of();

        public Instant createTime = Instant.parse("2026-04-30T03:55:00Z");
        public Instant executionStartTime = Instant.parse("2026-04-30T03:55:00.010Z");
        public Instant endTime = Instant.parse("2026-04-30T03:55:00.456Z");
    }

    private static QueryCompletedEvent build(Override o)
    {
        QueryMetadata metadata = new QueryMetadata(
                o.queryId,
                Optional.empty(),
                Optional.empty(),
                o.query,
                Optional.empty(),
                Optional.empty(),
                o.queryState,
                o.tables,
                List.<RoutineInfo>of(),
                URI.create("http://localhost:8080/v1/query/" + o.queryId),
                Optional.empty(),
                Optional.empty(),
                Optional.<String>empty());

        QueryStatistics statistics = new QueryStatistics(
                o.cpuTime,
                Duration.ZERO,
                o.wallTime,
                o.queuedTime,
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                o.planningTime,
                Optional.empty(),
                Optional.empty(),
                o.executionTime,
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                o.physicalInputReadTime,
                Optional.empty(),
                o.peakUserMemoryBytes,
                /* peakTaskUserMemory */ o.peakUserMemoryBytes,
                o.peakTaskTotalMemory,
                o.physicalInputBytes,
                o.physicalInputRows,
                o.processedInputBytes,
                o.processedInputRows,
                /* internalNetworkBytes */ 0L,
                /* internalNetworkRows */ 0L,
                o.outputBytes,
                o.outputRows,
                /* writtenBytes */ 0L,
                /* writtenRows */ 0L,
                /* spilledBytes */ 0L,
                /* cumulativeMemory */ 0.0,
                /* failedCumulativeMemory */ 0.0,
                List.of(),
                /* completedSplits */ 0,
                /* complete */ true,
                List.of(),
                List.of(),
                List.of(),
                List.of(),
                List.of(),
                List.<String>of(),
                List.of(),
                Map.<String, Metrics>of(),
                Map.<String, Metrics>of(),
                Optional.<String>empty());

        QueryContext context = new QueryContext(
                o.user,
                /* originalUser */ o.user,
                /* originalRoles */ Set.of(),
                o.principal,
                /* enabledRoles */ Set.of(),
                /* groups */ Set.of(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Set.of(),
                Set.of(),
                o.source,
                "UTC",
                o.catalog,
                o.schema,
                o.resourceGroupId,
                o.sessionProperties,
                new ResourceEstimates(Optional.empty(), Optional.empty(), Optional.empty()),
                o.serverAddress,
                /* serverVersion */ "480",
                /* environment */ "test",
                Optional.empty(),
                /* retryPolicy */ "NONE");

        QueryIOMetadata io = new QueryIOMetadata(o.inputs, o.output);

        return new QueryCompletedEvent(
                metadata,
                statistics,
                context,
                io,
                Optional.empty(),
                o.failure,
                List.of(),
                o.createTime,
                o.executionStartTime,
                o.endTime);
    }

    public static QueryFailureInfo failure(int code, String name, String type, String message)
    {
        ErrorType errorType = ErrorType.valueOf(type);
        return new QueryFailureInfo(
                new ErrorCode(code, name, errorType, /* fatal */ false),
                Optional.of(name),
                Optional.of(message),
                Optional.empty(),
                Optional.empty(),
                "{}");
    }

    public static QueryInputMetadata input(String catalog, String schema, String table)
    {
        return new QueryInputMetadata(
                Optional.of(catalog),
                catalog,
                new io.trino.spi.connector.CatalogVersion("test"),
                schema,
                table,
                List.of(),
                Optional.empty(),
                io.trino.spi.metrics.Metrics.EMPTY,
                OptionalLong.empty(),
                OptionalLong.empty());
    }
}
