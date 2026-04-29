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
package io.shelf.listener.plugin;

import io.trino.spi.Plugin;
import io.trino.spi.eventlistener.EventListenerFactory;

import java.util.List;

/**
 * Trino {@link Plugin} entry point for the SHELF-37 Iceberg event-listener.
 *
 * <p>Trino loads plugins from {@code plugin/&lt;name&gt;/}, instantiates the
 * {@code Plugin} declared in {@code META-INF/services/io.trino.spi.Plugin},
 * then walks {@link Plugin#getEventListenerFactories()}. We register a
 * single {@link ShelfIcebergEventListenerFactory} under the name
 * {@code shelf-iceberg-listener} — this is the value operators put in
 * {@code etc/event-listener.properties} as {@code event-listener.name}.
 *
 * <p>The plugin owns no global state. All per-instance state (config map,
 * bounded queue, writer thread, metrics registry, Iceberg table handle)
 * lives on the {@link io.shelf.listener.ShelfIcebergEventListener}
 * returned by the factory's {@code create()} call.
 */
public final class ShelfIcebergListenerPlugin
        implements Plugin
{
    public ShelfIcebergListenerPlugin() {}

    @Override
    public Iterable<EventListenerFactory> getEventListenerFactories()
    {
        return List.of(new ShelfIcebergEventListenerFactory());
    }
}
