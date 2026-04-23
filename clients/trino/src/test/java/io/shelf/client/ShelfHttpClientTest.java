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
import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.Arrays;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests {@link ShelfHttpClient#rangeGet} against an in-process HTTP server.
 * The JDK {@link java.net.http.HttpClient} negotiates HTTP/1.1 with
 * {@code com.sun.net.httpserver.HttpServer}; the wire exchange is identical
 * from the plugin's POV (range paths + response bytes).
 */
class ShelfHttpClientTest
{
    private HttpServer server;
    private String endpoint;
    private final AtomicInteger hits = new AtomicInteger();
    private volatile String lastPath;
    private volatile Consumer<HttpExchange> handler;

    @BeforeEach
    void setUp()
            throws IOException
    {
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.setExecutor(Executors.newFixedThreadPool(2));
        server.createContext("/cache", ex -> {
            hits.incrementAndGet();
            lastPath = ex.getRequestURI().getRawPath();
            Consumer<HttpExchange> h = handler;
            if (h == null) {
                ex.sendResponseHeaders(500, -1);
                ex.close();
                return;
            }
            h.accept(ex);
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
    }

    @Test
    void rangeGetReturnsBodyOn200()
            throws Exception
    {
        byte[] payload = new byte[128];
        for (int i = 0; i < payload.length; i++) {
            payload[i] = (byte) (i & 0xff);
        }
        handler = ex -> {
            try {
                ex.getResponseHeaders().add("Content-Range", "bytes 0-127/128");
                ex.sendResponseHeaders(200, payload.length);
                ex.getResponseBody().write(payload);
                ex.close();
            }
            catch (IOException e) {
                throw new RuntimeException(e);
            }
        };

        ShelfHttpClient client = new ShelfHttpClient(Duration.ofSeconds(2));
        byte[] got = client.rangeGet(endpoint, Pool.ROWGROUP, "deadbeef", 0L, 128L);

        assertThat(got).isEqualTo(payload);
        assertThat(lastPath).isEqualTo("/cache/rowgroup/deadbeef/0-127");
        assertThat(hits.get()).isEqualTo(1);
    }

    @Test
    void rangeGetPathUsesMetadataPoolWhenRequested()
            throws Exception
    {
        handler = respondWith(200, new byte[16]);
        ShelfHttpClient client = new ShelfHttpClient(Duration.ofSeconds(2));
        client.rangeGet(endpoint, Pool.METADATA, "cafebabe", 1024L, 16L);
        assertThat(lastPath).isEqualTo("/cache/metadata/cafebabe/1024-1039");
    }

    @Test
    void rangeGetEndpointWithoutSchemeGetsHttpPrefix()
            throws Exception
    {
        handler = respondWith(200, new byte[4]);
        ShelfHttpClient client = new ShelfHttpClient(Duration.ofSeconds(2));
        String bareHostPort = "127.0.0.1:" + server.getAddress().getPort();
        byte[] got = client.rangeGet(bareHostPort, Pool.ROWGROUP, "aa", 0L, 4L);
        assertThat(got).hasSize(4);
    }

    @Test
    void nonTwoxxMapsToShelfUnavailable()
    {
        handler = ex -> {
            try {
                ex.sendResponseHeaders(503, -1);
                ex.close();
            }
            catch (IOException e) {
                throw new RuntimeException(e);
            }
        };
        ShelfHttpClient client = new ShelfHttpClient(Duration.ofSeconds(2));
        assertThatThrownBy(() -> client.rangeGet(endpoint, Pool.ROWGROUP, "k", 0L, 4L))
                .isInstanceOf(ShelfHttpClient.ShelfUnavailableException.class)
                .hasMessageContaining("503");
    }

    @Test
    void shortBodyMapsToShelfUnavailable()
    {
        handler = respondWith(200, new byte[3]);
        ShelfHttpClient client = new ShelfHttpClient(Duration.ofSeconds(2));
        assertThatThrownBy(() -> client.rangeGet(endpoint, Pool.ROWGROUP, "k", 0L, 128L))
                .isInstanceOf(ShelfHttpClient.ShelfUnavailableException.class)
                .hasMessageContaining("expected 128");
    }

    @Test
    void slowServerTriggersTimeout()
    {
        handler = ex -> {
            try {
                Thread.sleep(400);
                ex.sendResponseHeaders(200, 0);
                ex.close();
            }
            catch (InterruptedException | IOException e) {
                // best effort
            }
        };
        ShelfHttpClient client = new ShelfHttpClient(Duration.ofMillis(80));
        assertThatThrownBy(() -> client.rangeGet(endpoint, Pool.ROWGROUP, "k", 0L, 4L))
                .isInstanceOf(ShelfHttpClient.ShelfUnavailableException.class);
    }

    @Test
    void connectionRefusedMapsToShelfUnavailable()
    {
        ShelfHttpClient client = new ShelfHttpClient(Duration.ofMillis(200));
        // Pick an arbitrary unused high port on localhost.
        String dead = "http://127.0.0.1:1";
        assertThatThrownBy(() -> client.rangeGet(dead, Pool.ROWGROUP, "k", 0L, 4L))
                .isInstanceOf(ShelfHttpClient.ShelfUnavailableException.class);
    }

    @Test
    void rangeGetIntegratesWithCircuitBreakerOnFailure()
    {
        handler = ex -> {
            try {
                ex.sendResponseHeaders(503, -1);
                ex.close();
            }
            catch (IOException e) {
                throw new RuntimeException(e);
            }
        };
        ShelfHttpClient client = new ShelfHttpClient(Duration.ofSeconds(1));
        CircuitBreaker breaker = new CircuitBreaker("shelf-0");

        for (int i = 0; i < CircuitBreaker.DEFAULT_FAILURE_THRESHOLD; i++) {
            try {
                client.rangeGet(endpoint, Pool.ROWGROUP, "k", 0L, 4L);
            }
            catch (ShelfHttpClient.ShelfUnavailableException expected) {
                breaker.recordFailure();
            }
        }
        assertThat(breaker.isOpen()).isTrue();
    }

    @Test
    void rejectsNonPositiveLength()
    {
        ShelfHttpClient client = new ShelfHttpClient(Duration.ofSeconds(1));
        assertThatThrownBy(() -> client.rangeGet(endpoint, Pool.ROWGROUP, "k", 0L, 0L))
                .isInstanceOf(IllegalArgumentException.class);
    }

    private Consumer<HttpExchange> respondWith(int status, byte[] body)
    {
        return ex -> {
            try {
                ex.sendResponseHeaders(status, body.length);
                ex.getResponseBody().write(body);
                ex.close();
            }
            catch (IOException e) {
                throw new RuntimeException(e);
            }
        };
    }

    @SuppressWarnings("unused")
    private static byte[] slice(byte[] src, int from, int toExclusive)
    {
        return Arrays.copyOfRange(src, from, toExclusive);
    }
}
