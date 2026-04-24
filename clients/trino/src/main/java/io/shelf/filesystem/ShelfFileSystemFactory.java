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

import io.shelf.client.MembershipResolver;
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
 * resources (HTTP client pool, {@link MembershipResolver}); the per-identity
 * {@link ShelfFileSystem} references them and the resolver owns the
 * {@code Map<String, CircuitBreaker>} keyed by pod id.
 *
 * <p>Endpoint / breaker selection for a given read goes through
 * {@link MembershipResolver#ownerFor(byte[])} — see
 * {@link ShelfInputFile#newStream()} for the per-stream binding.
 */
public final class ShelfFileSystemFactory
        implements TrinoFileSystemFactory
{
    private final ShelfConfig config;
    private final TrinoFileSystemFactory delegateFactory;
    private final RangeFetcher fetcher;
    private final MembershipResolver resolver;

    public ShelfFileSystemFactory(
            ShelfConfig config,
            TrinoFileSystemFactory delegateFactory,
            RangeFetcher fetcher,
            MembershipResolver resolver)
    {
        this.config = Objects.requireNonNull(config, "config");
        this.delegateFactory = Objects.requireNonNull(delegateFactory, "delegateFactory");
        this.fetcher = Objects.requireNonNull(fetcher, "fetcher");
        this.resolver = Objects.requireNonNull(resolver, "resolver");
    }

    public MembershipResolver resolver()
    {
        return resolver;
    }

    @Override
    public TrinoFileSystem create(ConnectorIdentity identity)
    {
        Objects.requireNonNull(identity, "identity");
        TrinoFileSystem delegate = delegateFactory.create(identity);
        return new ShelfFileSystem(config, delegate, fetcher, resolver);
    }
}
