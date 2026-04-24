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
import java.net.InetAddress;
import java.net.URI;
import java.net.UnknownHostException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Keeps the Shelf {@link HashRing} fresh against the live K8s headless
 * service (see BLUEPRINT §6.3 and SHELF-20).
 *
 * <p>Responsibilities:
 * <ul>
 *   <li>Resolve a DNS name (the Shelf headless service) every
 *       {@code refreshInterval} on a daemon {@link ScheduledExecutorService}.</li>
 *   <li>For each resolved IP, poll {@code GET /stats} with an independent
 *       {@code statsTimeout} (distinct from the 200 ms hot-path
 *       {@link ShelfHttpClient#DEFAULT_TIMEOUT}).</li>
 *   <li>Compute per-pod weight as
 *       {@code max(0, capacity_bytes - used_bytes)} and publish a new
 *       {@link Snapshot} atomically.</li>
 *   <li>Maintain a stable {@link CircuitBreaker} per pod id across
 *       refreshes; a pod that disappears and reappears keeps its breaker
 *       state.</li>
 * </ul>
 *
 * <p><b>Fail-open invariant.</b> No code path here ever throws to a
 * caller. DNS failures, connection refused, HTTP non-2xx, JSON parse
 * failure — all produce either a degraded snapshot (pod dropped /
 * weight zero) or retain the previous snapshot on total DNS failure.
 * If the current snapshot is empty, {@link #ownerFor(byte[])} returns
 * {@link Optional#empty()} and the caller (typically
 * {@code ShelfFileSystem}) must fall through to direct-S3.
 *
 * <p><b>DNS cache note.</b> The JVM caches successful DNS lookups
 * forever by default. Operators must set
 * {@code networkaddress.cache.ttl=0} (or a small positive value)
 * via the security property or
 * {@code -Dsun.net.inetaddr.ttl=0} for this class to observe
 * pod-rotation. The resolver deliberately does <em>not</em> mutate
 * JVM-wide security properties at class init: that behaviour belongs
 * in the Helm chart / Trino launch flags (SHELF-21). This class
 * re-invokes {@link InetAddress#getAllByName(String)} on every
 * refresh; if the JVM decides to answer from its own cache, that is a
 * deployment bug, not a resolver bug.
 *
 * <p>Thread-safe. Readers on the hot path are wait-free; only the
 * single refresh thread mutates the snapshot.
 */
public final class MembershipResolver
        implements AutoCloseable
{
    private static final Logger log = Logger.getLogger(MembershipResolver.class.getName());

    /** Target pod as resolved for a given content-addressed key. */
    public record Target(String podId, URI endpoint, CircuitBreaker breaker)
    {
        public Target
        {
            Objects.requireNonNull(podId, "podId");
            Objects.requireNonNull(endpoint, "endpoint");
            Objects.requireNonNull(breaker, "breaker");
        }
    }

    /**
     * Immutable point-in-time view published by the resolver. Consumers
     * capture whichever snapshot they read; readers during a refresh
     * never see a torn state.
     */
    public static final class Snapshot
    {
        private final HashRing ring;
        private final Map<String, URI> endpoints;
        private final Map<String, CircuitBreaker> breakers;

        public Snapshot(
                HashRing ring,
                Map<String, URI> endpoints,
                Map<String, CircuitBreaker> breakers)
        {
            this.ring = Objects.requireNonNull(ring, "ring");
            this.endpoints = Map.copyOf(Objects.requireNonNull(endpoints, "endpoints"));
            this.breakers = Map.copyOf(Objects.requireNonNull(breakers, "breakers"));
        }

        public HashRing ring()
        {
            return ring;
        }

        public Map<String, URI> endpoints()
        {
            return endpoints;
        }

        public Map<String, CircuitBreaker> breakers()
        {
            return breakers;
        }

        public boolean isEmpty()
        {
            return ring.members().isEmpty();
        }

        static Snapshot empty()
        {
            return new Snapshot(new HashRing(List.of()), Map.of(), Map.of());
        }
    }

    /**
     * Test seam. Production implementation performs
     * {@link InetAddress#getAllByName(String)} over the headless DNS
     * name. Tests inject a fixed or scripted list of URIs.
     */
    @FunctionalInterface
    public interface EndpointSource
    {
        /**
         * Return the current set of Shelf pod endpoint URIs. Order is
         * not significant; duplicates are collapsed by the resolver.
         *
         * @throws IOException if the underlying lookup failed.
         *         The resolver catches this and keeps its last good
         *         snapshot rather than propagating.
         */
        List<URI> currentEndpoints()
                throws IOException;

        static EndpointSource forHeadlessService(String dnsName, int port)
        {
            Objects.requireNonNull(dnsName, "dnsName");
            if (port <= 0 || port > 65535) {
                throw new IllegalArgumentException("port out of range: " + port);
            }
            return () -> {
                InetAddress[] addrs;
                try {
                    addrs = InetAddress.getAllByName(dnsName);
                }
                catch (UnknownHostException e) {
                    throw new IOException("DNS resolution failed for " + dnsName, e);
                }
                List<URI> uris = new ArrayList<>(addrs.length);
                for (InetAddress a : addrs) {
                    // getHostAddress() returns numeric form; avoids a reverse lookup.
                    uris.add(URI.create("http://" + hostForUri(a) + ":" + port));
                }
                return uris;
            };
        }

        private static String hostForUri(InetAddress a)
        {
            String h = a.getHostAddress();
            if (h.indexOf(':') >= 0 && h.charAt(0) != '[') {
                // IPv6 literal — wrap in brackets per RFC 3986.
                int scope = h.indexOf('%');
                if (scope >= 0) {
                    h = h.substring(0, scope);
                }
                return "[" + h + "]";
            }
            return h;
        }
    }

    /**
     * Default stats-poll deadline. Deliberately larger than the hot-path
     * read deadline because {@code /stats} runs on the resolver's
     * background scheduler, never on a Trino worker read.
     */
    public static final Duration DEFAULT_STATS_TIMEOUT = Duration.ofMillis(2000);

    /** BLUEPRINT §6.3 ("every 5 s") default refresh cadence. */
    public static final Duration DEFAULT_REFRESH_INTERVAL = Duration.ofMillis(5000);

    private final EndpointSource source;
    private final HttpClient http;
    private final Duration refreshInterval;
    private final Duration statsTimeout;
    private final ScheduledExecutorService scheduler;
    private final AtomicReference<Snapshot> snapshot = new AtomicReference<>(Snapshot.empty());
    private final Map<String, CircuitBreaker> breakersByPodId = new ConcurrentHashMap<>();
    private final AtomicInteger dnsFailures = new AtomicInteger();
    private final boolean frozen;
    private volatile boolean started;
    private volatile boolean closed;

    /**
     * Build a resolver that watches the given headless service. Call
     * {@link #start()} before use.
     */
    public MembershipResolver(
            String dnsName,
            int port,
            Duration refreshInterval,
            Duration statsTimeout,
            ShelfHttpClient httpClient)
    {
        this(
                EndpointSource.forHeadlessService(dnsName, port),
                Objects.requireNonNull(httpClient, "httpClient").httpClient(),
                refreshInterval,
                statsTimeout);
    }

    /**
     * Build a resolver with a caller-supplied {@link EndpointSource} and
     * {@link HttpClient}. Useful for unit tests, and for deployments
     * that want a custom membership lookup (e.g. a flat file list
     * during bring-up) without standing up a headless service.
     *
     * <p>The {@code http} client is shared with {@link ShelfHttpClient}
     * in production to keep the shaded JAR small and to reuse the
     * HTTP/2 connection pool.
     */
    public MembershipResolver(
            EndpointSource source,
            HttpClient http,
            Duration refreshInterval,
            Duration statsTimeout)
    {
        this(source, http, refreshInterval, statsTimeout, false);
    }

    private MembershipResolver(
            EndpointSource source,
            HttpClient http,
            Duration refreshInterval,
            Duration statsTimeout,
            boolean frozen)
    {
        this.source = Objects.requireNonNull(source, "source");
        this.http = Objects.requireNonNull(http, "http");
        this.refreshInterval = requirePositive(refreshInterval, "refreshInterval");
        this.statsTimeout = requirePositive(statsTimeout, "statsTimeout");
        this.frozen = frozen;
        this.scheduler = frozen
                ? null
                : Executors.newSingleThreadScheduledExecutor(daemonThreadFactory());
    }

    /**
     * Return a resolver frozen to a single known target. Useful for
     * unit tests that need a deterministic single-pod ring. The
     * returned resolver's {@link #start()} / {@link #close()} are
     * no-ops.
     */
    public static MembershipResolver fixed(String podId, URI endpoint, CircuitBreaker breaker)
    {
        Objects.requireNonNull(podId, "podId");
        Objects.requireNonNull(endpoint, "endpoint");
        Objects.requireNonNull(breaker, "breaker");
        MembershipResolver r = new MembershipResolver(
                () -> List.of(endpoint),
                HttpClient.newHttpClient(),
                Duration.ofSeconds(1),
                Duration.ofSeconds(1),
                /*frozen=*/ true);
        r.breakersByPodId.put(podId, breaker);
        r.snapshot.set(new Snapshot(
                new HashRing(List.of(new HashRing.Node(podId, 1.0))),
                Map.of(podId, endpoint),
                Map.of(podId, breaker)));
        r.started = true;
        return r;
    }

    /** Start the background refresh loop. Idempotent. No-op for frozen resolvers built via {@link #fixed}. */
    public void start()
    {
        if (closed) {
            throw new IllegalStateException("resolver closed");
        }
        if (started) {
            return;
        }
        started = true;
        if (frozen) {
            return;
        }
        scheduler.scheduleWithFixedDelay(
                this::safeRefresh,
                0L,
                refreshInterval.toMillis(),
                TimeUnit.MILLISECONDS);
    }

    /** @return the current snapshot; never null. */
    public Snapshot snapshot()
    {
        return snapshot.get();
    }

    /**
     * HRW-select the owning pod for a content-addressed key.
     *
     * @return the target, or {@link Optional#empty()} if the ring is
     *         currently empty (caller must fall through to S3).
     */
    public Optional<Target> ownerFor(byte[] contentKey)
    {
        Objects.requireNonNull(contentKey, "contentKey");
        Snapshot s = snapshot.get();
        return s.ring.ownerFor(contentKey).flatMap(node -> {
            URI ep = s.endpoints.get(node.podId());
            CircuitBreaker br = s.breakers.get(node.podId());
            if (ep == null || br == null) {
                // Snapshot consistency invariant violated — treat as
                // empty ring to preserve fail-open.
                return Optional.empty();
            }
            return Optional.of(new Target(node.podId(), ep, br));
        });
    }

    /**
     * Drive a single refresh tick synchronously. Exposed for tests so
     * they do not have to wait on the scheduler. Never throws.
     */
    public void refreshNow()
    {
        if (frozen) {
            return;
        }
        safeRefresh();
    }

    @Override
    public void close()
    {
        if (closed) {
            return;
        }
        closed = true;
        if (scheduler == null) {
            return;
        }
        scheduler.shutdownNow();
        try {
            scheduler.awaitTermination(2, TimeUnit.SECONDS);
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /** Test-visible hook: fetch {@code /stats} once. Never throws. */
    Optional<PodStats> pollStats(URI endpoint)
    {
        Objects.requireNonNull(endpoint, "endpoint");
        URI statsUri;
        try {
            statsUri = endpoint.resolve("/stats");
        }
        catch (IllegalArgumentException e) {
            return Optional.empty();
        }
        HttpRequest req = HttpRequest.newBuilder(statsUri)
                .timeout(statsTimeout)
                .GET()
                .build();
        HttpResponse<String> resp;
        try {
            resp = http.sendAsync(req, HttpResponse.BodyHandlers.ofString())
                    .get(statsTimeout.toNanos(), TimeUnit.NANOSECONDS);
        }
        catch (TimeoutException e) {
            log.log(Level.FINE, () -> "shelfd /stats timed out at " + endpoint);
            return Optional.empty();
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return Optional.empty();
        }
        catch (Exception e) {
            // ExecutionException + any transport-level unchecked wrap.
            log.log(Level.FINE, e, () -> "shelfd /stats transport error at " + endpoint);
            return Optional.empty();
        }
        if (resp.statusCode() / 100 != 2) {
            log.log(Level.FINE, () -> "shelfd /stats HTTP " + resp.statusCode() + " at " + endpoint);
            return Optional.empty();
        }
        return StatsParser.parse(resp.body());
    }

    private void safeRefresh()
    {
        try {
            doRefresh();
        }
        catch (Throwable t) {
            // Defence in depth. The refresh body below already catches
            // every expected failure type; any leak here is a bug but
            // we still must not kill the scheduler thread.
            log.log(Level.WARNING, t, () -> "unexpected error in membership refresh");
        }
    }

    private void doRefresh()
    {
        List<URI> endpoints;
        try {
            endpoints = source.currentEndpoints();
        }
        catch (IOException e) {
            int n = dnsFailures.incrementAndGet();
            if (n == 1 || n % 12 == 0) {
                log.log(Level.WARNING,
                        "Shelf DNS resolution failed (consecutive=" + n + ", cause="
                                + e.getClass().getSimpleName() + ": " + e.getMessage()
                                + "); keeping last-good snapshot");
                log.log(Level.FINE, e, () -> "DNS failure detail");
            }
            return;
        }
        dnsFailures.set(0);

        // Deduplicate while preserving first-seen order.
        LinkedHashMap<URI, Boolean> dedup = new LinkedHashMap<>();
        for (URI u : endpoints) {
            dedup.put(u, Boolean.TRUE);
        }

        List<HashRing.Node> nodes = new ArrayList<>();
        Map<String, URI> epMap = new HashMap<>();
        Map<String, CircuitBreaker> brMap = new HashMap<>();

        for (URI endpoint : dedup.keySet()) {
            Optional<PodStats> stats = pollStats(endpoint);
            if (stats.isEmpty()) {
                // Skip: pod unreachable or returned a malformed /stats.
                // The breaker for this pod (if it existed before) is
                // retained in breakersByPodId so its state survives a
                // transient outage.
                continue;
            }
            PodStats s = stats.get();
            double weight = Math.max(0L, s.capacityBytes - s.usedBytes);
            CircuitBreaker breaker = breakersByPodId.computeIfAbsent(s.podId, CircuitBreaker::new);
            nodes.add(new HashRing.Node(s.podId, weight));
            epMap.put(s.podId, endpoint);
            brMap.put(s.podId, breaker);
        }

        Snapshot next = new Snapshot(new HashRing(nodes), epMap, brMap);
        snapshot.set(next);
    }

    private static Duration requirePositive(Duration d, String name)
    {
        Objects.requireNonNull(d, name);
        if (d.isNegative() || d.isZero()) {
            throw new IllegalArgumentException(name + " must be positive, got " + d);
        }
        return d;
    }

    private static ThreadFactory daemonThreadFactory()
    {
        final AtomicInteger n = new AtomicInteger();
        return r -> {
            Thread t = new Thread(r, "shelf-membership-resolver-" + n.incrementAndGet());
            t.setDaemon(true);
            return t;
        };
    }

    /** Parsed subset of the {@code /stats} response. */
    record PodStats(String podId, long capacityBytes, long usedBytes)
    {
        PodStats
        {
            Objects.requireNonNull(podId, "podId");
        }
    }

    /**
     * Narrow JSON parser for the {@code /stats} schema owned by
     * {@code shelfd} (SHELF-19, agent 4). Extracts only the three
     * top-level fields we care about: {@code pod_id},
     * {@code capacity_bytes}, {@code used_bytes}. Everything else
     * (pool-level sub-objects, counters, future additions) is ignored
     * by design.
     *
     * <p>Zero dependencies — Trino's plugin classloader does not
     * expose a stable Jackson version, and shading Jackson would
     * inflate the plugin jar well beyond the size budget in the
     * shaded-jar comment on the pom.
     *
     * <p>Depth-aware: a field called {@code capacity_bytes} nested
     * inside {@code metadata_pool} is NOT confused with the top-level
     * one.
     */
    static final class StatsParser
    {
        private StatsParser() {}

        static Optional<PodStats> parse(String json)
        {
            if (json == null) {
                return Optional.empty();
            }
            try {
                return Optional.ofNullable(parseOrNull(json));
            }
            catch (RuntimeException e) {
                // Any malformed-input surprise maps to "pod unreachable".
                return Optional.empty();
            }
        }

        private static PodStats parseOrNull(String json)
        {
            int i = skipWs(json, 0);
            if (i >= json.length() || json.charAt(i) != '{') {
                return null;
            }
            i++;
            String podId = null;
            Long capacity = null;
            Long used = null;
            while (true) {
                i = skipWs(json, i);
                if (i >= json.length()) {
                    return null;
                }
                char c = json.charAt(i);
                if (c == '}') {
                    break;
                }
                if (c == ',') {
                    i++;
                    continue;
                }
                if (c != '"') {
                    return null;
                }
                int[] keyEnd = new int[1];
                String key = readString(json, i, keyEnd);
                if (key == null) {
                    return null;
                }
                i = keyEnd[0];
                i = skipWs(json, i);
                if (i >= json.length() || json.charAt(i) != ':') {
                    return null;
                }
                i++;
                i = skipWs(json, i);
                if (i >= json.length()) {
                    return null;
                }
                char v = json.charAt(i);
                if (v == '"') {
                    int[] end = new int[1];
                    String value = readString(json, i, end);
                    if (value == null) {
                        return null;
                    }
                    i = end[0];
                    if ("pod_id".equals(key)) {
                        podId = value;
                    }
                }
                else if (v == '{' || v == '[') {
                    i = skipStructured(json, i);
                    if (i < 0) {
                        return null;
                    }
                }
                else if (v == '-' || (v >= '0' && v <= '9')) {
                    int numStart = i;
                    if (v == '-') {
                        i++;
                    }
                    while (i < json.length()) {
                        char nc = json.charAt(i);
                        if ((nc >= '0' && nc <= '9') || nc == '.' || nc == 'e' || nc == 'E' || nc == '+' || nc == '-') {
                            i++;
                        }
                        else {
                            break;
                        }
                    }
                    String num = json.substring(numStart, i);
                    if ("capacity_bytes".equals(key)) {
                        capacity = parseLongOrNull(num);
                        if (capacity == null) {
                            return null;
                        }
                    }
                    else if ("used_bytes".equals(key)) {
                        used = parseLongOrNull(num);
                        if (used == null) {
                            return null;
                        }
                    }
                }
                else {
                    // true / false / null — skip letters.
                    while (i < json.length() && Character.isLetter(json.charAt(i))) {
                        i++;
                    }
                }
            }
            if (podId == null || capacity == null || used == null) {
                return null;
            }
            if (capacity < 0 || used < 0) {
                return null;
            }
            return new PodStats(podId, capacity, used);
        }

        private static Long parseLongOrNull(String s)
        {
            try {
                return Long.parseLong(s);
            }
            catch (NumberFormatException e) {
                return null;
            }
        }

        private static int skipWs(String json, int i)
        {
            while (i < json.length() && Character.isWhitespace(json.charAt(i))) {
                i++;
            }
            return i;
        }

        /**
         * Read a JSON string starting at {@code i} (pointing at the
         * opening quote). Returns the unescaped-enough-for-us contents
         * and writes the index of the first char past the closing
         * quote into {@code end[0]}.
         */
        private static String readString(String json, int i, int[] end)
        {
            if (json.charAt(i) != '"') {
                return null;
            }
            StringBuilder sb = new StringBuilder();
            i++;
            while (i < json.length()) {
                char c = json.charAt(i);
                if (c == '"') {
                    end[0] = i + 1;
                    return sb.toString();
                }
                if (c == '\\') {
                    if (i + 1 >= json.length()) {
                        return null;
                    }
                    char esc = json.charAt(i + 1);
                    switch (esc) {
                        case '"' -> sb.append('"');
                        case '\\' -> sb.append('\\');
                        case '/' -> sb.append('/');
                        case 'b' -> sb.append('\b');
                        case 'f' -> sb.append('\f');
                        case 'n' -> sb.append('\n');
                        case 'r' -> sb.append('\r');
                        case 't' -> sb.append('\t');
                        case 'u' -> {
                            if (i + 5 >= json.length()) {
                                return null;
                            }
                            sb.append((char) Integer.parseInt(json.substring(i + 2, i + 6), 16));
                            i += 4;
                        }
                        default -> {
                            return null;
                        }
                    }
                    i += 2;
                    continue;
                }
                sb.append(c);
                i++;
            }
            return null;
        }

        /**
         * Skip a balanced JSON object or array starting at {@code i}
         * (pointing at the opening brace or bracket). Returns the
         * index of the first char past the balanced close, or
         * {@code -1} on malformed input.
         */
        private static int skipStructured(String json, int i)
        {
            char open = json.charAt(i);
            char close = open == '{' ? '}' : ']';
            int depth = 1;
            i++;
            while (i < json.length() && depth > 0) {
                char c = json.charAt(i);
                if (c == '"') {
                    int[] end = new int[1];
                    String s = readString(json, i, end);
                    if (s == null) {
                        return -1;
                    }
                    i = end[0];
                    continue;
                }
                if (c == '{' || c == '[') {
                    depth++;
                }
                else if (c == '}' || c == ']') {
                    depth--;
                }
                i++;
            }
            if (depth != 0) {
                return -1;
            }
            return i;
        }
    }

    /**
     * Allow tests and other code to directly install a snapshot
     * without going through the scheduler. Not part of the stable
     * API.
     */
    void setSnapshotForTesting(Snapshot s)
    {
        snapshot.set(Objects.requireNonNull(s, "s"));
    }

    /** Read-only view of the current pod-id to breaker map. */
    public Map<String, CircuitBreaker> allBreakers()
    {
        return Collections.unmodifiableMap(breakersByPodId);
    }
}
