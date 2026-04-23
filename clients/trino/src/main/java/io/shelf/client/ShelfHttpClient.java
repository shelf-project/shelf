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

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * HTTP/2 range-GET client for the Shelf data plane.
 *
 * <p>Per ADR-0004, v1 uses HTTP/2 for <em>all</em> payload sizes. Arrow Flight
 * is deferred. The "1 MB HTTP / {@code >=} 1 MB Flight" split from BLUEPRINT
 * §8.1 does <em>not</em> apply in v1.
 *
 * <p>All calls carry a default 200 ms deadline ({@link #DEFAULT_TIMEOUT}).
 * Every failure — {@link IOException}, {@link TimeoutException}, HTTP 503/504,
 * connection closed — throws {@link ShelfUnavailableException} so the caller
 * (typically {@link io.shelf.filesystem.ShelfInputStream}) can mark the
 * relevant {@link CircuitBreaker} as having failed and fall through to direct
 * S3. Any non-2xx / non-5xx response body is returned as a
 * {@link ShelfUnavailableException} as well: the plugin never distinguishes
 * Shelf-originated 4xx from Shelf-originated 5xx, because both violate the
 * fail-open contract equally.
 *
 * @see io.shelf.filesystem.ShelfFileSystem for the fail-open invariant.
 */
public final class ShelfHttpClient
{
    public static final Duration DEFAULT_TIMEOUT = Duration.ofMillis(200);

    private final HttpClient http;
    private final Duration timeout;

    public ShelfHttpClient()
    {
        this(DEFAULT_TIMEOUT);
    }

    public ShelfHttpClient(Duration timeout)
    {
        this(timeout, buildHttpClient(timeout));
    }

    /** Test seam: inject a pre-built {@link HttpClient}. */
    public ShelfHttpClient(Duration timeout, HttpClient http)
    {
        this.timeout = Objects.requireNonNull(timeout, "timeout");
        this.http = Objects.requireNonNull(http, "http");
    }

    private static HttpClient buildHttpClient(Duration connectTimeout)
    {
        return HttpClient.newBuilder()
                .version(HttpClient.Version.HTTP_2)
                .connectTimeout(connectTimeout)
                .build();
    }

    public Duration timeout()
    {
        return timeout;
    }

    public HttpClient httpClient()
    {
        return http;
    }

    /**
     * Issue a range-GET against a Shelf pod.
     *
     * @param endpoint  owning pod endpoint as {@code host:port} or a full
     *                  {@code http://host:port} URL.
     * @param pool      the target cache pool (metadata vs rowgroup).
     * @param contentKey content-addressed key (hex string of
     *                  {@code sha256(etag || offset || length || rg)}).
     * @param offset    byte offset in the origin object.
     * @param length    number of bytes requested; must be positive.
     * @return raw bytes on 2xx success.
     * @throws ShelfUnavailableException on any Shelf-originated failure;
     *         the caller maps this to a direct-S3 read per BLUEPRINT §9.5.
     */
    public byte[] rangeGet(String endpoint, Pool pool, String contentKey, long offset, long length)
            throws ShelfUnavailableException
    {
        Objects.requireNonNull(endpoint, "endpoint");
        Objects.requireNonNull(pool, "pool");
        Objects.requireNonNull(contentKey, "contentKey");
        if (length <= 0L) {
            throw new IllegalArgumentException("length must be > 0, got " + length);
        }
        if (offset < 0L) {
            throw new IllegalArgumentException("offset must be >= 0, got " + offset);
        }

        URI uri;
        try {
            String base = endpoint.contains("://") ? endpoint : "http://" + endpoint;
            long end = offset + length - 1L;
            uri = new URI(base + "/cache/" + pool.wire() + "/" + contentKey + "/" + offset + "-" + end);
        }
        catch (URISyntaxException e) {
            throw new ShelfUnavailableException("invalid shelfd endpoint: " + endpoint, e);
        }

        HttpRequest req = HttpRequest.newBuilder(uri)
                .timeout(timeout)
                .GET()
                .build();

        HttpResponse<byte[]> resp;
        try {
            resp = http.sendAsync(req, HttpResponse.BodyHandlers.ofByteArray())
                    .get(timeout.toNanos(), TimeUnit.NANOSECONDS);
        }
        catch (TimeoutException e) {
            throw new ShelfUnavailableException("shelfd timeout after " + timeout.toMillis() + "ms", e);
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new ShelfUnavailableException("shelfd call interrupted", e);
        }
        catch (java.util.concurrent.ExecutionException e) {
            throw new ShelfUnavailableException("shelfd transport error", e.getCause() != null ? e.getCause() : e);
        }

        int status = resp.statusCode();
        if (status / 100 != 2) {
            throw new ShelfUnavailableException("shelfd returned HTTP " + status);
        }
        byte[] body = resp.body();
        // shelfd always returns the exact range on 2xx. Defensive check so
        // an upstream bug doesn't silently feed Trino a short buffer.
        if (body.length != length) {
            throw new ShelfUnavailableException(
                    "shelfd returned " + body.length + " bytes, expected " + length);
        }
        return body;
    }

    /** Thrown by {@link #rangeGet}; always triggers a direct-S3 fall-through. */
    public static final class ShelfUnavailableException
            extends IOException
    {
        public ShelfUnavailableException(String message)
        {
            super(message);
        }

        public ShelfUnavailableException(String message, Throwable cause)
        {
            super(message, cause);
        }
    }
}
