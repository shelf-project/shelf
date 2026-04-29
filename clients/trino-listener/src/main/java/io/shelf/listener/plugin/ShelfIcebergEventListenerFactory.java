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

import io.shelf.listener.ShelfIcebergEventListener;
import io.shelf.listener.config.ListenerConfig;
import io.trino.spi.eventlistener.EventListener;
import io.trino.spi.eventlistener.EventListenerFactory;

import java.util.Map;

/**
 * SPI factory exposed by {@link ShelfIcebergListenerPlugin}.
 *
 * <p>Operators wire this listener via {@code etc/event-listener.properties}:
 *
 * <pre>{@code
 * event-listener.name=shelf-iceberg-listener
 * shelf.listener.iceberg.catalog=hive
 * shelf.listener.iceberg.table=trino_logs.trino_queries_oss
 * shelf.listener.iceberg.catalog-impl=org.apache.iceberg.hive.HiveCatalog
 * shelf.listener.iceberg.warehouse=s3a://my-warehouse-bucket/trino-logs/
 * shelf.listener.iceberg.uri=thrift://my-hms.cluster.local:9083
 * shelf.listener.fail-mode=drop
 * }</pre>
 */
public final class ShelfIcebergEventListenerFactory
        implements EventListenerFactory
{
    public static final String NAME = "shelf-iceberg-listener";

    @Override
    public String getName()
    {
        return NAME;
    }

    @Override
    public EventListener create(Map<String, String> config, EventListenerContext context)
    {
        ListenerConfig parsed = ListenerConfig.fromMap(config);
        return new ShelfIcebergEventListener(parsed);
    }
}
