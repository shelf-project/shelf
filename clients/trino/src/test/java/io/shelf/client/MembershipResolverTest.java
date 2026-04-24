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
package io.shelf.client;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.http.HttpClient;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * JDK-only integration of {@link MembershipResolver} against in-process
 * {@code /stats} servers backed by {@link HttpServer}. No Testcontainers,
 * no Mockito — per SHELF-20 ACs and the test-infra constraints.
 */
class MembershipResolverTest
{
    private final List<HttpServer> servers = new ArrayList<>();

    @AfterEach
    void stopServers()
    {
        for (HttpServer s : servers) {
            s.stop(0);
        }
        servers.clear();
    }

    @Test
    void threePodsReachable_ringHasThreeMembersWithDerivedWeights()
            throws IOException
    {
        HttpServer a = startStatsServer("shelf-0", 100L, 10L);   // weight 90
        HttpServer b = startStatsServer("shelf-1", 100L, 30L);   // weight 70
        HttpServer c = startStatsServer("shelf-2", 100L, 0L);    // weight 100

        try (MembershipResolver resolver = newResolver(uriOf(a), uriOf(b), uriOf(c))) {
            resolver.start();
            resolver.refreshNow();
            MembershipResolver.Snapshot s = resolver.snapshot();

            assertThat(s.ring().members())
                    .extracting(HashRing.Node::podId)
                    .containsExactlyInAnyOrder("shelf-0", "shelf-1", "shelf-2");
            assertThat(weight(s, "shelf-0")).isEqualTo(90.0);
            assertThat(weight(s, "shelf-1")).isEqualTo(70.0);
            assertThat(weight(s, "shelf-2")).isEqualTo(100.0);

            Optional<MembershipResolver.Target> t = resolver.ownerFor(new byte[]{1, 2, 3});
            assertThat(t).isPresent();
            assertThat(t.get().endpoint()).isIn(uriOf(a), uriOf(b), uriOf(c));
        }
    }

    @Test
    void onePodUnreachable_ringDropsItGracefully()
            throws IOException
    {
        HttpServer a = startStatsServer("shelf-0", 100L, 10L);
        HttpServer b = startStatsServer("shelf-1", 100L, 30L);
        // Reserve a port then refuse: use a bound-then-closed server so
        // localhost connects are actively refused rather than timing out.
        URI deadEndpoint = reservedButClosed();

        try (MembershipResolver resolver = newResolver(uriOf(a), uriOf(b), deadEndpoint)) {
            resolver.start();
            resolver.refreshNow();
            MembershipResolver.Snapshot s = resolver.snapshot();

            assertThat(s.ring().members())
                    .extracting(HashRing.Node::podId)
                    .containsExactlyInAnyOrder("shelf-0", "shelf-1");
        }
    }

    @Test
    void malformedJsonOnOnePod_sameGracefulDegradation()
            throws IOException
    {
        HttpServer a = startStatsServer("shelf-0", 100L, 10L);
        HttpServer bad = startRawServer("{ this is not valid json at all");
        HttpServer c = startStatsServer("shelf-2", 100L, 0L);

        try (MembershipResolver resolver = newResolver(uriOf(a), uriOf(bad), uriOf(c))) {
            resolver.start();
            resolver.refreshNow();
            MembershipResolver.Snapshot s = resolver.snapshot();

            assertThat(s.ring().members())
                    .extracting(HashRing.Node::podId)
                    .containsExactlyInAnyOrder("shelf-0", "shelf-2");
        }
    }

    @Test
    void emptyEndpointList_emptyRingNoThrow()
            throws IOException
    {
        try (MembershipResolver resolver = newResolver(/* no endpoints */)) {
            resolver.start();
            resolver.refreshNow();
            MembershipResolver.Snapshot s = resolver.snapshot();
            assertThat(s.isEmpty()).isTrue();
            assertThat(resolver.ownerFor(new byte[]{1})).isEmpty();
        }
    }

    @Test
    void dnsFailure_keepsLastGoodSnapshot()
            throws IOException
    {
        HttpServer a = startStatsServer("shelf-0", 100L, 10L);
        // A source that first returns the server, then throws on every call.
        List<Boolean> shouldThrow = new ArrayList<>();
        shouldThrow.add(false);
        MembershipResolver.EndpointSource flakyDns = () -> {
            if (shouldThrow.get(0)) {
                throw new IOException("dns down");
            }
            return List.of(uriOf(a));
        };
        try (MembershipResolver resolver = new MembershipResolver(
                flakyDns,
                HttpClient.newHttpClient(),
                Duration.ofSeconds(1),
                Duration.ofSeconds(1))) {
            resolver.refreshNow();
            assertThat(resolver.snapshot().ring().members()).hasSize(1);

            shouldThrow.set(0, true);
            resolver.refreshNow();
            assertThat(resolver.snapshot().ring().members())
                    .as("stale snapshot retained on DNS failure")
                    .hasSize(1);
        }
    }

    @Test
    void usedExceedsCapacity_weightClampedToZero()
            throws IOException
    {
        HttpServer a = startStatsServer("shelf-0", 100L, 1000L);   // over-full
        HttpServer b = startStatsServer("shelf-1", 100L, 10L);

        try (MembershipResolver resolver = newResolver(uriOf(a), uriOf(b))) {
            resolver.start();
            resolver.refreshNow();
            MembershipResolver.Snapshot s = resolver.snapshot();
            assertThat(s.ring().members())
                    .extracting(HashRing.Node::podId)
                    .containsExactlyInAnyOrder("shelf-0", "shelf-1");
            assertThat(weight(s, "shelf-0")).isEqualTo(0.0);
            assertThat(weight(s, "shelf-1")).isEqualTo(90.0);
        }
    }

    @Test
    void breakerInstanceRetainedAcrossRefreshes()
            throws IOException
    {
        HttpServer a = startStatsServer("shelf-0", 100L, 10L);

        try (MembershipResolver resolver = newResolver(uriOf(a))) {
            resolver.refreshNow();
            CircuitBreaker first = resolver.snapshot().breakers().get("shelf-0");
            assertThat(first).isNotNull();
            resolver.refreshNow();
            CircuitBreaker second = resolver.snapshot().breakers().get("shelf-0");
            assertThat(second).isSameAs(first);
        }
    }

    @Test
    void parser_ignoresNestedFieldsThatShareTopLevelNames()
    {
        String json = """
                {
                  "pod_id": "shelf-3",
                  "capacity_bytes": 12884901888,
                  "used_bytes": 3221225472,
                  "metadata_pool": { "capacity_bytes": 1073741824, "used_bytes": 536870912 },
                  "rowgroup_pool": { "capacity_bytes": 8589934592, "used_bytes": 2684354560 }
                }
                """;
        Optional<MembershipResolver.PodStats> stats = MembershipResolver.StatsParser.parse(json);
        assertThat(stats).isPresent();
        assertThat(stats.get().podId()).isEqualTo("shelf-3");
        assertThat(stats.get().capacityBytes()).isEqualTo(12884901888L);
        assertThat(stats.get().usedBytes()).isEqualTo(3221225472L);
    }

    @Test
    void parser_returnsEmptyOnMissingRequiredField()
    {
        assertThat(MembershipResolver.StatsParser.parse("{\"pod_id\":\"x\"}")).isEmpty();
        assertThat(MembershipResolver.StatsParser.parse("")).isEmpty();
        assertThat(MembershipResolver.StatsParser.parse("not json")).isEmpty();
        assertThat(MembershipResolver.StatsParser.parse(null)).isEmpty();
    }

    // ------------------------------------------------------------------
    // Helpers

    private MembershipResolver newResolver(URI... endpoints)
    {
        List<URI> list = List.of(endpoints);
        return new MembershipResolver(
                () -> list,
                HttpClient.newBuilder()
                        .version(HttpClient.Version.HTTP_1_1)
                        .connectTimeout(Duration.ofSeconds(1))
                        .build(),
                Duration.ofSeconds(5),
                Duration.ofMillis(1500));
    }

    private static double weight(MembershipResolver.Snapshot s, String podId)
    {
        return s.ring().members().stream()
                .filter(n -> n.podId().equals(podId))
                .findFirst()
                .orElseThrow()
                .weight();
    }

    private HttpServer startStatsServer(String podId, long capacity, long used)
            throws IOException
    {
        String json = String.format(
                "{\"pod_id\":\"%s\",\"capacity_bytes\":%d,\"used_bytes\":%d,"
                        + "\"metadata_pool\":{\"capacity_bytes\":%d,\"used_bytes\":%d},"
                        + "\"rowgroup_pool\":{\"capacity_bytes\":%d,\"used_bytes\":%d}}",
                podId, capacity, used,
                capacity / 4, used / 4,
                3 * capacity / 4, 3 * used / 4);
        return startRawServer(json);
    }

    private HttpServer startRawServer(String body)
            throws IOException
    {
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/stats", new StaticBodyHandler(body));
        server.setExecutor(null);
        server.start();
        servers.add(server);
        return server;
    }

    private static URI uriOf(HttpServer server)
    {
        return URI.create("http://127.0.0.1:" + server.getAddress().getPort());
    }

    /**
     * Bind a server to a free port, then stop it so that the port is
     * closed. {@code URI} still points at that port; polling it yields
     * a prompt connection-refused instead of a timeout.
     */
    private URI reservedButClosed()
            throws IOException
    {
        HttpServer throwaway = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        int port = throwaway.getAddress().getPort();
        throwaway.start();
        throwaway.stop(0);
        return URI.create("http://127.0.0.1:" + port);
    }

    private static final class StaticBodyHandler
            implements HttpHandler
    {
        private final byte[] body;

        StaticBodyHandler(String body)
        {
            this.body = body.getBytes();
        }

        @Override
        public void handle(HttpExchange exchange)
                throws IOException
        {
            exchange.getResponseHeaders().set("Content-Type", "application/json");
            exchange.sendResponseHeaders(200, body.length);
            exchange.getResponseBody().write(body);
            exchange.getResponseBody().close();
        }
    }
}
