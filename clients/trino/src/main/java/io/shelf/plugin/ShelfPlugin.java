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
package io.shelf.plugin;

import io.shelf.config.ShelfConfig;
import io.shelf.eventlistener.PrefetchClient;
import io.shelf.eventlistener.ShelfPrefetchListener;
import io.shelf.filesystem.ShelfFileSystemFactory;
import io.trino.spi.Plugin;
import io.trino.spi.eventlistener.EventListener;
import io.trino.spi.eventlistener.EventListenerFactory;

import java.util.List;
import java.util.Map;

/**
 * Root {@link Plugin} for Shelf.
 *
 * <p>Registers two SPI entry points:
 * <ul>
 *   <li>{@link ShelfFileSystemFactory} — Trino's per-query file system for
 *       configured prefixes.</li>
 *   <li>{@link ShelfPrefetchListener} — coordinator-side plan-aware prefetch
 *       (ADR-0005 compliant, no split-completed dependency).</li>
 * </ul>
 *
 * <p>The FileSystem is wired via the Trino 480 plugin FS factory registry.
 * We expose it through a small in-process holder on the plugin rather than
 * via {@link Plugin} directly, since the Trino 480 SPI does not have a
 * {@code getFileSystemFactories()} method yet (this lives in
 * {@code io.trino.filesystem} outside {@code io.trino.spi}). Ticket SHELF-10
 * finalises the wiring once we've decided whether to load Shelf as a Trino
 * connector or as a standalone plugin.
 */
public final class ShelfPlugin
        implements Plugin
{
    public ShelfPlugin() {}

    @Override
    public Iterable<EventListenerFactory> getEventListenerFactories()
    {
        return List.of(new ShelfEventListenerFactory());
    }

    /**
     * Accessor used by the FS-factory registration path (SHELF-10).
     * Intentionally package-private and not part of the Trino SPI.
     */
    ShelfFileSystemFactory buildFileSystemFactory(ShelfConfig config)
    {
        // TODO(SHELF-10): wired from catalog config via ShelfConfig.fromMap.
        return new ShelfFileSystemFactory(config);
    }

    /** Nested EventListenerFactory so Trino can instantiate it via the SPI. */
    public static final class ShelfEventListenerFactory
            implements EventListenerFactory
    {
        public static final String NAME = "shelf-prefetch";

        @Override
        public String getName()
        {
            return NAME;
        }

        @Override
        public EventListener create(Map<String, String> config, EventListenerContext context)
        {
            ShelfConfig parsed = ShelfConfig.fromMap(config);
            return new ShelfPrefetchListener(parsed, new PrefetchClient());
        }
    }
}
