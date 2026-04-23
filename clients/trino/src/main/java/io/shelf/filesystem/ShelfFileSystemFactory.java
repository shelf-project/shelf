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
package io.shelf.filesystem;

import io.shelf.client.CircuitBreaker;
import io.shelf.client.RangeFetcher;
import io.shelf.config.ShelfConfig;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.spi.security.ConnectorIdentity;

import java.util.Objects;

/**
 * Builds per-identity {@link ShelfFileSystem} instances.
 *
 * <p>Trino instantiates this factory once per catalog and invokes
 * {@link #create(ConnectorIdentity)} per query. The factory owns the long-lived
 * resources (HTTP client pool, circuit-breaker registry, hash-ring snapshot);
 * the per-identity {@link ShelfFileSystem} references them.
 *
 * <p>In Phase-1 we wire a single {@link CircuitBreaker} for the whole
 * endpoint. Once SHELF-20 lands the per-pod membership resolver, the
 * factory will own a {@code Map<String, CircuitBreaker>} keyed by pod id
 * and select the right one via {@link io.shelf.client.HashRing#ownerFor}.
 */
public final class ShelfFileSystemFactory
        implements TrinoFileSystemFactory
{
    private final ShelfConfig config;
    private final TrinoFileSystemFactory delegateFactory;
    private final RangeFetcher fetcher;
    private final CircuitBreaker breaker;

    public ShelfFileSystemFactory(
            ShelfConfig config,
            TrinoFileSystemFactory delegateFactory,
            RangeFetcher fetcher,
            CircuitBreaker breaker)
    {
        this.config = Objects.requireNonNull(config, "config");
        this.delegateFactory = Objects.requireNonNull(delegateFactory, "delegateFactory");
        this.fetcher = Objects.requireNonNull(fetcher, "fetcher");
        this.breaker = Objects.requireNonNull(breaker, "breaker");
    }

    @Override
    public TrinoFileSystem create(ConnectorIdentity identity)
    {
        Objects.requireNonNull(identity, "identity");
        TrinoFileSystem delegate = delegateFactory.create(identity);
        return new ShelfFileSystem(config, delegate, fetcher, breaker);
    }
}
