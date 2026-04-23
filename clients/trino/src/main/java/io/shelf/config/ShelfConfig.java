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
package io.shelf.config;

import java.time.Duration;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Configuration surface for the Shelf Trino plugin.
 *
 * <p>Key names match BLUEPRINT.md §6.2 exactly. See {@code docs/config.md}
 * for the authoritative default / range / notes table.
 *
 * <p>This class is the only place in the plugin that parses user-facing config.
 * Stubbed for now; the Phase-0 v0.1 wiring in ticket SHELF-10 promotes the
 * getters from placeholders to full validation.
 */
public final class ShelfConfig
{
    // ---- property keys ------------------------------------------------------

    public static final String KEY_ENABLED = "shelf.enabled";
    public static final String KEY_ENDPOINT = "shelf.endpoint";
    public static final String KEY_TENANT = "shelf.tenant";
    public static final String KEY_FALLBACK_ON_ERROR = "shelf.fallback.on-error";
    public static final String KEY_PREFETCH_ENABLED = "shelf.prefetch.enabled";
    public static final String KEY_GRANULARITY = "shelf.granularity";

    // ---- defaults -----------------------------------------------------------

    public static final boolean DEFAULT_ENABLED = false;
    public static final String DEFAULT_ENDPOINT = "shelf.shelf.svc.cluster.local:9090";
    public static final String DEFAULT_TENANT = "default";
    public static final String DEFAULT_FALLBACK_ON_ERROR = "direct-s3";
    public static final boolean DEFAULT_PREFETCH_ENABLED = false;
    public static final String DEFAULT_GRANULARITY = "row-group,footer,manifest";

    /** Per-RPC deadline; aligns with the {@link io.shelf.client.ShelfHttpClient} budget. */
    public static final Duration DEFAULT_RPC_TIMEOUT = Duration.ofMillis(200);

    // ---- state --------------------------------------------------------------

    private final boolean enabled;
    private final String endpoint;
    private final String tenant;
    private final String fallbackOnError;
    private final boolean prefetchEnabled;
    private final Set<String> granularity;

    private ShelfConfig(
            boolean enabled,
            String endpoint,
            String tenant,
            String fallbackOnError,
            boolean prefetchEnabled,
            Set<String> granularity)
    {
        this.enabled = enabled;
        this.endpoint = Objects.requireNonNull(endpoint, "endpoint");
        this.tenant = Objects.requireNonNull(tenant, "tenant");
        this.fallbackOnError = Objects.requireNonNull(fallbackOnError, "fallbackOnError");
        this.prefetchEnabled = prefetchEnabled;
        this.granularity = Set.copyOf(Objects.requireNonNull(granularity, "granularity"));
    }

    public static ShelfConfig fromMap(Map<String, String> props)
    {
        Objects.requireNonNull(props, "props");
        // TODO(SHELF-10): full parsing + validation per BLUEPRINT §6.2 + 03-plan.md §4.
        //   For now we return the defaults so the plugin loads cleanly in Trino 480.
        return defaults();
    }

    public static ShelfConfig defaults()
    {
        return new ShelfConfig(
                DEFAULT_ENABLED,
                DEFAULT_ENDPOINT,
                DEFAULT_TENANT,
                DEFAULT_FALLBACK_ON_ERROR,
                DEFAULT_PREFETCH_ENABLED,
                Set.of("row-group", "footer", "manifest"));
    }

    public boolean isEnabled()
    {
        return enabled;
    }

    public String getEndpoint()
    {
        return endpoint;
    }

    public String getTenant()
    {
        return tenant;
    }

    public String getFallbackOnError()
    {
        return fallbackOnError;
    }

    public boolean isPrefetchEnabled()
    {
        return prefetchEnabled;
    }

    public Set<String> getGranularity()
    {
        return granularity;
    }
}
