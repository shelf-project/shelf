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

import java.net.http.HttpClient;
import java.time.Duration;
import java.util.Objects;

/**
 * HTTP/2 range-GET client for the Shelf data plane.
 *
 * <p>Per ADR-0004, v1 uses HTTP/2 for <em>all</em> payload sizes. Arrow Flight
 * is deferred. The "1 MB HTTP / {@code >=} 1 MB Flight" split from BLUEPRINT
 * §8.1 does <em>not</em> apply in v1.
 *
 * <p>All calls carry a default 200 ms deadline ({@link #DEFAULT_TIMEOUT}); every
 * exception the JDK client may throw — {@code IOException},
 * {@code HttpTimeoutException}, {@code ConnectException} — maps to a
 * {@link CircuitBreaker#recordFailure()} and a direct-S3 fall-through by the
 * caller. See {@link io.shelf.filesystem.ShelfFileSystem} for the fail-open
 * invariant.
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
        this.timeout = Objects.requireNonNull(timeout, "timeout");
        this.http = HttpClient.newBuilder()
                .version(HttpClient.Version.HTTP_2)
                .connectTimeout(timeout)
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
     * @param target            owning pod endpoint (host:port).
     * @param contentKey        content-addressed key (hex string of
     *                          {@code sha256(etag || offset || length)}).
     * @param offset            byte offset in the origin object.
     * @param length            bytes to read.
     * @return raw bytes on success.
     * @throws java.io.IOException on Shelf-originated failure — caller maps
     *                             this to a direct-S3 read per BLUEPRINT §9.5.
     */
    public byte[] rangeGet(String target, String contentKey, long offset, long length)
            throws java.io.IOException
    {
        Objects.requireNonNull(target, "target");
        Objects.requireNonNull(contentKey, "contentKey");
        // TODO(SHELF-15): implement GET /cache/{key}/{offset}-{length} with h2
        //   multiplexing + pooled connection reuse, per ADR-0004 and
        //   BLUEPRINT §8.1 (v1 HTTP/2-only variant) + 03-plan.md §4 SHELF-15.
        throw new UnsupportedOperationException("SHELF-15: range-GET not wired yet");
    }
}
