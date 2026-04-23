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
 */
public final class ShelfFileSystemFactory
        implements TrinoFileSystemFactory
{
    private final ShelfConfig config;

    public ShelfFileSystemFactory(ShelfConfig config)
    {
        this.config = Objects.requireNonNull(config, "config");
    }

    @Override
    public TrinoFileSystem create(ConnectorIdentity identity)
    {
        Objects.requireNonNull(identity, "identity");
        // TODO(SHELF-10): build a ShelfFileSystem that delegates to an S3-backed
        //   TrinoFileSystem obtained from the coordinator's FS factory registry
        //   + intercepts reads for configured prefixes. Per 03-plan.md §4 SHELF-10.
        return new ShelfFileSystem(config);
    }
}
