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
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;

/**
 * Configuration surface for the Shelf Trino plugin.
 *
 * <p>Key names match BLUEPRINT.md §6.2 exactly. See {@code docs/config.md}
 * for the authoritative default / range / notes table.
 *
 * <p>All parsing + validation happens in {@link #fromMap(Map)}. Any invalid
 * value throws {@link IllegalArgumentException} with the offending key in
 * the message so catalog init fails loud.
 *
 * <p>The key {@code shelf.fallback.on-error} is a "documented-non-tuneable":
 * it is accepted for observability (so operators can see the value in catalog
 * properties) but the only legal value is {@code "direct-s3"}. Any other
 * value fails plugin init per BLUEPRINT §1 and §9.5.
 */
public final class ShelfConfig
{
    public static final String KEY_ENABLED = "shelf.enabled";
    public static final String KEY_ENDPOINT = "shelf.endpoint";
    public static final String KEY_TENANT = "shelf.tenant";
    public static final String KEY_FALLBACK_ON_ERROR = "shelf.fallback.on-error";
    public static final String KEY_PREFETCH_ENABLED = "shelf.prefetch.enabled";
    public static final String KEY_GRANULARITY = "shelf.granularity";
    public static final String KEY_RPC_TIMEOUT_MS = "shelf.rpc.timeout-ms";
    public static final String KEY_MEMBERSHIP_REFRESH_INTERVAL_MS = "shelf.membership.refresh-interval-ms";
    public static final String KEY_MEMBERSHIP_STATS_TIMEOUT_MS = "shelf.membership.stats-timeout-ms";
    /** SHELF-15. Controls the Parquet-footer prefetch window (in KiB). */
    public static final String KEY_FOOTER_PREFETCH_KIB = "shelf.footer.prefetch.kib";

    public static final boolean DEFAULT_ENABLED = false;
    public static final String DEFAULT_ENDPOINT = "shelf.shelf.svc.cluster.local:9090";
    public static final String DEFAULT_TENANT = "default";
    public static final String DEFAULT_FALLBACK_ON_ERROR = "direct-s3";
    public static final boolean DEFAULT_PREFETCH_ENABLED = false;
    public static final String DEFAULT_GRANULARITY = "row-group,footer,manifest";
    /** Per-RPC deadline; aligns with the {@link io.shelf.client.ShelfHttpClient} budget. */
    public static final Duration DEFAULT_RPC_TIMEOUT = Duration.ofMillis(200);
    /** How often the {@link io.shelf.client.MembershipResolver} re-resolves DNS + polls {@code /stats}. */
    public static final Duration DEFAULT_MEMBERSHIP_REFRESH_INTERVAL = Duration.ofMillis(5000);
    /** Per-pod {@code /stats} poll deadline (background thread; independent of hot-path 200 ms). */
    public static final Duration DEFAULT_MEMBERSHIP_STATS_TIMEOUT = Duration.ofMillis(2000);
    /** SHELF-15 default footer prefetch window: 64 KiB covers the vast majority of Iceberg Parquet footers. */
    public static final int DEFAULT_FOOTER_PREFETCH_KIB = 64;
    /** SHELF-15 lower bound. Zero would mean "no prefetch" which is already expressible via {@link #KEY_PREFETCH_ENABLED}. */
    public static final int MIN_FOOTER_PREFETCH_KIB = 1;
    /**
     * SHELF-15 upper bound. 256 KiB matches BLUEPRINT §7.3: any file that
     * keeps a genuinely larger footer is an outlier, and prefetching more
     * starts competing with row-group bandwidth for no payoff.
     */
    public static final int MAX_FOOTER_PREFETCH_KIB = 256;

    private static final long MAX_MEMBERSHIP_REFRESH_INTERVAL_MS = 300_000L;
    private static final long MAX_MEMBERSHIP_STATS_TIMEOUT_MS = 60_000L;

    public static final Set<String> LEGAL_GRANULARITY = Set.of("row-group", "footer", "manifest");

    private static final Set<String> KNOWN_KEYS = Set.of(
            KEY_ENABLED,
            KEY_ENDPOINT,
            KEY_TENANT,
            KEY_FALLBACK_ON_ERROR,
            KEY_PREFETCH_ENABLED,
            KEY_GRANULARITY,
            KEY_RPC_TIMEOUT_MS,
            KEY_MEMBERSHIP_REFRESH_INTERVAL_MS,
            KEY_MEMBERSHIP_STATS_TIMEOUT_MS,
            KEY_FOOTER_PREFETCH_KIB);

    private final boolean enabled;
    private final String endpoint;
    private final String tenant;
    private final String fallbackOnError;
    private final boolean prefetchEnabled;
    private final Set<String> granularity;
    private final Duration rpcTimeout;
    private final Duration membershipRefreshInterval;
    private final Duration membershipStatsTimeout;
    private final int footerPrefetchKib;

    private ShelfConfig(
            boolean enabled,
            String endpoint,
            String tenant,
            String fallbackOnError,
            boolean prefetchEnabled,
            Set<String> granularity,
            Duration rpcTimeout,
            Duration membershipRefreshInterval,
            Duration membershipStatsTimeout,
            int footerPrefetchKib)
    {
        this.enabled = enabled;
        this.endpoint = Objects.requireNonNull(endpoint, "endpoint");
        this.tenant = Objects.requireNonNull(tenant, "tenant");
        this.fallbackOnError = Objects.requireNonNull(fallbackOnError, "fallbackOnError");
        this.prefetchEnabled = prefetchEnabled;
        this.granularity = Set.copyOf(Objects.requireNonNull(granularity, "granularity"));
        this.rpcTimeout = Objects.requireNonNull(rpcTimeout, "rpcTimeout");
        this.membershipRefreshInterval = Objects.requireNonNull(membershipRefreshInterval, "membershipRefreshInterval");
        this.membershipStatsTimeout = Objects.requireNonNull(membershipStatsTimeout, "membershipStatsTimeout");
        this.footerPrefetchKib = footerPrefetchKib;
    }

    public static ShelfConfig fromMap(Map<String, String> props)
    {
        Objects.requireNonNull(props, "props");

        Set<String> unknown = new TreeSet<>();
        for (String k : props.keySet()) {
            if (k.startsWith("shelf.") && !KNOWN_KEYS.contains(k)) {
                unknown.add(k);
            }
        }
        if (!unknown.isEmpty()) {
            throw new IllegalArgumentException("Unknown Shelf config keys: " + unknown);
        }

        boolean enabled = parseBool(props, KEY_ENABLED, DEFAULT_ENABLED);
        String endpoint = parseNonEmptyString(props, KEY_ENDPOINT, DEFAULT_ENDPOINT);
        validateEndpoint(endpoint);
        String tenant = parseNonEmptyString(props, KEY_TENANT, DEFAULT_TENANT);

        String fallback = props.getOrDefault(KEY_FALLBACK_ON_ERROR, DEFAULT_FALLBACK_ON_ERROR);
        if (!DEFAULT_FALLBACK_ON_ERROR.equals(fallback)) {
            throw new IllegalArgumentException(
                    KEY_FALLBACK_ON_ERROR + "=" + fallback
                            + " is not a tuneable; the only legal value is '" + DEFAULT_FALLBACK_ON_ERROR
                            + "' (BLUEPRINT §1, §9.5)");
        }

        boolean prefetchEnabled = parseBool(props, KEY_PREFETCH_ENABLED, DEFAULT_PREFETCH_ENABLED);
        Set<String> granularity = parseGranularity(props.getOrDefault(KEY_GRANULARITY, DEFAULT_GRANULARITY));
        Duration rpcTimeout = parseRpcTimeout(props);
        Duration refreshInterval = parsePositiveDurationMs(
                props,
                KEY_MEMBERSHIP_REFRESH_INTERVAL_MS,
                DEFAULT_MEMBERSHIP_REFRESH_INTERVAL,
                MAX_MEMBERSHIP_REFRESH_INTERVAL_MS);
        Duration statsTimeout = parsePositiveDurationMs(
                props,
                KEY_MEMBERSHIP_STATS_TIMEOUT_MS,
                DEFAULT_MEMBERSHIP_STATS_TIMEOUT,
                MAX_MEMBERSHIP_STATS_TIMEOUT_MS);
        int footerPrefetchKib = parseBoundedInt(
                props,
                KEY_FOOTER_PREFETCH_KIB,
                DEFAULT_FOOTER_PREFETCH_KIB,
                MIN_FOOTER_PREFETCH_KIB,
                MAX_FOOTER_PREFETCH_KIB);

        return new ShelfConfig(
                enabled,
                endpoint,
                tenant,
                fallback,
                prefetchEnabled,
                granularity,
                rpcTimeout,
                refreshInterval,
                statsTimeout,
                footerPrefetchKib);
    }

    public static ShelfConfig defaults()
    {
        return new ShelfConfig(
                DEFAULT_ENABLED,
                DEFAULT_ENDPOINT,
                DEFAULT_TENANT,
                DEFAULT_FALLBACK_ON_ERROR,
                DEFAULT_PREFETCH_ENABLED,
                Set.of("row-group", "footer", "manifest"),
                DEFAULT_RPC_TIMEOUT,
                DEFAULT_MEMBERSHIP_REFRESH_INTERVAL,
                DEFAULT_MEMBERSHIP_STATS_TIMEOUT,
                DEFAULT_FOOTER_PREFETCH_KIB);
    }

    private static boolean parseBool(Map<String, String> props, String key, boolean fallback)
    {
        String raw = props.get(key);
        if (raw == null) {
            return fallback;
        }
        String normalized = raw.trim().toLowerCase(Locale.ROOT);
        return switch (normalized) {
            case "true" -> true;
            case "false" -> false;
            default -> throw new IllegalArgumentException(
                    key + "=" + raw + " must be 'true' or 'false'");
        };
    }

    private static String parseNonEmptyString(Map<String, String> props, String key, String fallback)
    {
        String raw = props.getOrDefault(key, fallback);
        if (raw == null || raw.isBlank()) {
            throw new IllegalArgumentException(key + " must be non-empty");
        }
        return raw.trim();
    }

    private static void validateEndpoint(String endpoint)
    {
        int colon = endpoint.lastIndexOf(':');
        if (colon <= 0 || colon == endpoint.length() - 1) {
            throw new IllegalArgumentException(
                    KEY_ENDPOINT + "=" + endpoint + " must be 'host:port'");
        }
        String portStr = endpoint.substring(colon + 1);
        int port;
        try {
            port = Integer.parseInt(portStr);
        }
        catch (NumberFormatException e) {
            throw new IllegalArgumentException(
                    KEY_ENDPOINT + " port is not an integer: " + portStr, e);
        }
        if (port <= 0 || port > 65535) {
            throw new IllegalArgumentException(
                    KEY_ENDPOINT + " port out of range 1..65535: " + port);
        }
    }

    private static Set<String> parseGranularity(String raw)
    {
        Set<String> out = new LinkedHashSet<>();
        for (String piece : raw.split(",")) {
            String trimmed = piece.trim();
            if (trimmed.isEmpty()) {
                continue;
            }
            if (!LEGAL_GRANULARITY.contains(trimmed)) {
                throw new IllegalArgumentException(
                        KEY_GRANULARITY + " has illegal token '" + trimmed
                                + "'; legal tokens are " + new TreeSet<>(LEGAL_GRANULARITY));
            }
            out.add(trimmed);
        }
        if (out.isEmpty()) {
            throw new IllegalArgumentException(
                    KEY_GRANULARITY + " must list at least one of " + Arrays.asList("row-group", "footer", "manifest"));
        }
        return out;
    }

    private static Duration parseRpcTimeout(Map<String, String> props)
    {
        String raw = props.get(KEY_RPC_TIMEOUT_MS);
        if (raw == null) {
            return DEFAULT_RPC_TIMEOUT;
        }
        long ms;
        try {
            ms = Long.parseLong(raw.trim());
        }
        catch (NumberFormatException e) {
            throw new IllegalArgumentException(
                    KEY_RPC_TIMEOUT_MS + "=" + raw + " must be a positive integer (milliseconds)", e);
        }
        if (ms <= 0) {
            throw new IllegalArgumentException(
                    KEY_RPC_TIMEOUT_MS + "=" + ms + " must be > 0");
        }
        return Duration.ofMillis(ms);
    }

    private static int parseBoundedInt(
            Map<String, String> props,
            String key,
            int fallback,
            int min,
            int max)
    {
        String raw = props.get(key);
        if (raw == null) {
            return fallback;
        }
        int value;
        try {
            value = Integer.parseInt(raw.trim());
        }
        catch (NumberFormatException e) {
            throw new IllegalArgumentException(
                    key + "=" + raw + " must be an integer in [" + min + ".." + max + "]", e);
        }
        if (value < min || value > max) {
            throw new IllegalArgumentException(
                    key + "=" + value + " must be in [" + min + ".." + max + "]");
        }
        return value;
    }

    private static Duration parsePositiveDurationMs(
            Map<String, String> props,
            String key,
            Duration fallback,
            long maxMs)
    {
        String raw = props.get(key);
        if (raw == null) {
            return fallback;
        }
        long ms;
        try {
            ms = Long.parseLong(raw.trim());
        }
        catch (NumberFormatException e) {
            throw new IllegalArgumentException(
                    key + "=" + raw + " must be a positive integer (milliseconds)", e);
        }
        if (ms <= 0) {
            throw new IllegalArgumentException(key + "=" + ms + " must be > 0");
        }
        if (ms > maxMs) {
            throw new IllegalArgumentException(
                    key + "=" + ms + " exceeds the upper bound of " + maxMs + " ms");
        }
        return Duration.ofMillis(ms);
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

    public Duration getRpcTimeout()
    {
        return rpcTimeout;
    }

    public Duration getMembershipRefreshInterval()
    {
        return membershipRefreshInterval;
    }

    public Duration getMembershipStatsTimeout()
    {
        return membershipStatsTimeout;
    }

    /**
     * Footer prefetch window in KiB. See
     * {@link io.shelf.client.FooterPrefetcher} and BLUEPRINT §7.3. A
     * value of {@value #DEFAULT_FOOTER_PREFETCH_KIB} KiB covers the
     * Parquet footer (metadata length + magic, footer thrift payload)
     * for essentially every Iceberg file in the penpencil fleet.
     */
    public int getFooterPrefetchKib()
    {
        return footerPrefetchKib;
    }
}
