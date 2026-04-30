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

import com.sun.net.httpserver.Headers;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import io.shelf.tag.SessionTagProvider;
import io.shelf.tag.TagProvider;
import io.shelf.tag.TagSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * SHELF-42 — verifies that {@link ShelfHttpClient#withTagProvider} stamps
 * the {@code X-Shelf-Tag} header onto every outbound HTTP request when a
 * provider returns a non-empty tag, and skips the header when the tag is
 * empty or the provider misbehaves.
 *
 * <p>The harness captures the request headers off the in-process
 * {@link HttpServer} so the assertions exercise the wire form, not the
 * provider's internal state.
 */
final class ShelfHttpClientTagTest
{
    private HttpServer server;
    private String endpoint;
    private final AtomicReference<Headers> lastHeaders = new AtomicReference<>();

    @BeforeEach
    void setUp()
            throws IOException
    {
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.setExecutor(Executors.newFixedThreadPool(2));
        server.createContext("/cache", ex -> {
            lastHeaders.set(new Headers());
            lastHeaders.get().putAll(ex.getRequestHeaders());
            try {
                byte[] body = new byte[16];
                ex.sendResponseHeaders(200, body.length);
                ex.getResponseBody().write(body);
                ex.close();
            }
            catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
        server.start();
        endpoint = "http://127.0.0.1:" + server.getAddress().getPort();
    }

    @AfterEach
    void tearDown()
    {
        if (server != null) {
            server.stop(0);
        }
        SessionTagProvider.clear();
    }

    @Test
    void noTagProviderInstalledOmitsTheHeader()
            throws Exception
    {
        ShelfHttpClient client = new ShelfHttpClient(Duration.ofSeconds(2));
        client.rangeGet(endpoint, Pool.ROWGROUP, "deadbeef", 0L, 16L);
        assertThat(lastHeaders.get().getFirst(TagSet.HEADER_NAME))
                .as("no provider ⇒ no X-Shelf-Tag")
                .isNull();
    }

    @Test
    void tagProviderReturningEmptyOmitsTheHeader()
            throws Exception
    {
        ShelfHttpClient client = new ShelfHttpClient(Duration.ofSeconds(2))
                .withTagProvider(() -> TagSet.empty());
        client.rangeGet(endpoint, Pool.ROWGROUP, "deadbeef", 0L, 16L);
        assertThat(lastHeaders.get().getFirst(TagSet.HEADER_NAME))
                .as("empty tag ⇒ no header")
                .isNull();
    }

    @Test
    void sessionPropertyExperimentBecomesXShelfTagHeader()
            throws Exception
    {
        ShelfHttpClient client = new ShelfHttpClient(Duration.ofSeconds(2))
                .withTagProvider(SessionTagProvider.INSTANCE);
        try (AutoCloseable handle = SessionTagProvider.install(
                Map.of("shelf.tag.experiment", "b1_on"))) {
            client.rangeGet(endpoint, Pool.ROWGROUP, "deadbeef", 0L, 16L);
        }
        String header = lastHeaders.get().getFirst(TagSet.HEADER_NAME);
        assertThat(header).isEqualTo("%7B%22experiment%22%3A%22b1_on%22%7D");
    }

    @Test
    void multipleSessionPropertiesAreSortedInWireForm()
            throws Exception
    {
        ShelfHttpClient client = new ShelfHttpClient(Duration.ofSeconds(2))
                .withTagProvider(SessionTagProvider.INSTANCE);
        try (AutoCloseable handle = SessionTagProvider.install(Map.of(
                "shelf.tag.experiment", "shelf_46_bloom",
                "shelf.tag.cohort", "rep2_canary"))) {
            client.rangeGet(endpoint, Pool.ROWGROUP, "deadbeef", 0L, 16L);
        }
        // From the golden fixture for `experiment_and_cohort_sorted`.
        String expected = "%7B%22cohort%22%3A%22rep2_canary%22%2C%22experiment%22%3A%22shelf_46_bloom%22%7D";
        assertThat(lastHeaders.get().getFirst(TagSet.HEADER_NAME)).isEqualTo(expected);
    }

    @Test
    void misbehavingProviderFailsOpen()
            throws Exception
    {
        TagProvider boom = () -> {
            throw new RuntimeException("simulated provider crash");
        };
        ShelfHttpClient client = new ShelfHttpClient(Duration.ofSeconds(2))
                .withTagProvider(boom);
        client.rangeGet(endpoint, Pool.ROWGROUP, "deadbeef", 0L, 16L);
        assertThat(lastHeaders.get().getFirst(TagSet.HEADER_NAME))
                .as("provider exception must NOT surface to wire")
                .isNull();
    }
}
