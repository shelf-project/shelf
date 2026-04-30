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
package io.shelf.eventlistener;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * SHELF-G5 — coordinator-side client for
 * {@code POST /filter/probe} on shelfd.
 *
 * <p>Groups a batch of (file, column, predicate) probes and asks
 * shelfd which row groups might match. Returning {@code
 * fail_open=true} for any probe means the engine should assume
 * the whole universe matches — the caller keeps those splits.
 *
 * <p>This is the Java half of BLUEPRINT §7.4's Track G. The
 * actual split-source wrapping (intercepting
 * {@code IcebergSplitSource.splits}) ships with SHELF-29 / the
 * upstream cache SPI; until then the client is callable from
 * {@link ShelfPrefetchListener#queryCreated} for tests and dry
 * runs.
 *
 * <p><b>Coordinator-thread safety.</b> Every probe is bounded by
 * a 5 ms hard deadline (matches the G4 service budget). On
 * timeout we return {@code fail_open=true} — split filtering is
 * strictly opportunistic.
 */
public class ShelfFilterClient
{
    public static final Duration DEFAULT_TIMEOUT = Duration.ofMillis(5);

    private final HttpClient http;
    private final URI endpoint;
    private final Duration timeout;

    public ShelfFilterClient(HttpClient http, URI endpoint, Duration timeout)
    {
        this.http = Objects.requireNonNull(http, "http");
        this.endpoint = Objects.requireNonNull(endpoint, "endpoint");
        this.timeout = Objects.requireNonNull(timeout, "timeout");
    }

    public ShelfFilterClient(URI endpoint)
    {
        this(HttpClient.newHttpClient(), endpoint, DEFAULT_TIMEOUT);
    }

    /**
     * Probe a single (table, column, predicate) tuple. Returns
     * {@link ProbeResult#failOpen()} if the service has no
     * signal, or on any error — see class docs.
     */
    public ProbeResult probe(ProbeRequest request)
    {
        Objects.requireNonNull(request, "request");
        HttpRequest http = HttpRequest.newBuilder(endpoint)
                .timeout(timeout)
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(request.toJson()))
                .build();
        try {
            HttpResponse<String> resp = this.http.send(
                    http, HttpResponse.BodyHandlers.ofString());
            if (resp.statusCode() != 200) {
                return ProbeResult.unfiltered();
            }
            return ProbeResult.parse(resp.body());
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return ProbeResult.unfiltered();
        }
        catch (Exception e) {
            return ProbeResult.unfiltered();
        }
    }

    /** Probe many independent tuples; missing/failed probes fail open. */
    public List<ProbeResult> probeBatch(List<ProbeRequest> requests)
    {
        List<ProbeResult> out = new ArrayList<>(requests.size());
        for (ProbeRequest r : requests) {
            out.add(probe(r));
        }
        return Collections.unmodifiableList(out);
    }

    /** Wire-shape request. Serialised by hand to avoid pulling in Jackson. */
    public record ProbeRequest(
            String tableFqn,
            String column,
            Predicate predicate,
            List<String> manifestFiles)
    {
        public ProbeRequest
        {
            Objects.requireNonNull(tableFqn, "tableFqn");
            Objects.requireNonNull(column, "column");
            Objects.requireNonNull(predicate, "predicate");
            manifestFiles = manifestFiles == null
                    ? Collections.emptyList()
                    : List.copyOf(manifestFiles);
        }

        String toJson()
        {
            StringBuilder sb = new StringBuilder(256);
            sb.append("{\"table_fqn\":").append(jsonString(tableFqn));
            sb.append(",\"column\":").append(jsonString(column));
            sb.append(",\"predicate\":").append(predicate.toJson());
            sb.append(",\"manifest_files\":[");
            boolean first = true;
            for (String m : manifestFiles) {
                if (!first) {
                    sb.append(',');
                }
                sb.append(jsonString(m));
                first = false;
            }
            sb.append("]}");
            return sb.toString();
        }
    }

    /**
     * Predicate shape matching the Rust enum. The default
     * implementations of {@link #equals} / {@link #hashCode} on
     * Java records compare {@code byte[]} components by
     * reference; the concrete records here override that with
     * value semantics so {@link SkippableSplitFilter} can
     * group-by-predicate without an external canonicalisation
     * pass.
     */
    public sealed interface Predicate
    {
        String toJson();

        record Equal(byte[] value) implements Predicate
        {
            @Override public String toJson()
            {
                return "{\"kind\":\"equal\",\"value\":" + base64(value) + "}";
            }

            @Override public boolean equals(Object o)
            {
                return o instanceof Equal other && java.util.Arrays.equals(value, other.value);
            }

            @Override public int hashCode()
            {
                return java.util.Arrays.hashCode(value);
            }
        }

        record Range(byte[] minInclusive, byte[] maxInclusive) implements Predicate
        {
            @Override public String toJson()
            {
                return "{\"kind\":\"range\",\"min_inclusive\":" + base64(minInclusive)
                        + ",\"max_inclusive\":" + base64(maxInclusive) + "}";
            }

            @Override public boolean equals(Object o)
            {
                return o instanceof Range other
                        && java.util.Arrays.equals(minInclusive, other.minInclusive)
                        && java.util.Arrays.equals(maxInclusive, other.maxInclusive);
            }

            @Override public int hashCode()
            {
                return 31 * java.util.Arrays.hashCode(minInclusive)
                        + java.util.Arrays.hashCode(maxInclusive);
            }
        }

        record InList(List<byte[]> values) implements Predicate
        {
            @Override public String toJson()
            {
                StringBuilder sb = new StringBuilder("{\"kind\":\"in_list\",\"values\":[");
                boolean first = true;
                for (byte[] v : values) {
                    if (!first) {
                        sb.append(',');
                    }
                    sb.append(base64(v));
                    first = false;
                }
                sb.append("]}");
                return sb.toString();
            }

            @Override public boolean equals(Object o)
            {
                if (!(o instanceof InList other) || other.values.size() != values.size()) {
                    return false;
                }
                for (int i = 0; i < values.size(); i++) {
                    if (!java.util.Arrays.equals(values.get(i), other.values.get(i))) {
                        return false;
                    }
                }
                return true;
            }

            @Override public int hashCode()
            {
                int h = 1;
                for (byte[] v : values) {
                    h = 31 * h + java.util.Arrays.hashCode(v);
                }
                return h;
            }
        }

        static String base64(byte[] raw)
        {
            // Bytes are encoded as raw JSON arrays rather than
            // base64 so the Rust side (serde_bytes) can consume
            // them without bringing in base64 decode on the hot
            // path. Same shape the proto generator emits.
            StringBuilder sb = new StringBuilder("[");
            for (int i = 0; i < raw.length; i++) {
                if (i > 0) {
                    sb.append(',');
                }
                sb.append(raw[i] & 0xff);
            }
            sb.append(']');
            return sb.toString();
        }
    }

    /** Outcome of a single probe. */
    public record ProbeResult(boolean failOpen, List<RowGroupRef> maybeMatch)
    {
        public static ProbeResult unfiltered()
        {
            return new ProbeResult(true, Collections.emptyList());
        }

        public static ProbeResult parse(String body)
        {
            // Minimal parser — no Jackson, no yaml. The shape is
            // fixed and documented; any deviation means
            // fail-open.
            boolean failOpen = body.contains("\"fail_open\":true");
            if (failOpen) {
                return new ProbeResult(true, Collections.emptyList());
            }
            List<RowGroupRef> rows = new ArrayList<>();
            int idx = body.indexOf("\"maybe_match\"");
            if (idx < 0) {
                return new ProbeResult(true, Collections.emptyList());
            }
            int start = body.indexOf('[', idx);
            int end = start < 0 ? -1 : body.indexOf(']', start);
            if (start < 0 || end < 0) {
                return new ProbeResult(true, Collections.emptyList());
            }
            String slice = body.substring(start + 1, end);
            int p = 0;
            while (p < slice.length()) {
                int open = slice.indexOf('{', p);
                if (open < 0) break;
                int close = slice.indexOf('}', open);
                if (close < 0) break;
                String obj = slice.substring(open, close + 1);
                RowGroupRef ref = RowGroupRef.parse(obj);
                if (ref != null) {
                    rows.add(ref);
                }
                p = close + 1;
            }
            return new ProbeResult(false, Collections.unmodifiableList(rows));
        }
    }

    public record RowGroupRef(String fileEtag, int rowGroupOrdinal)
    {
        static RowGroupRef parse(String obj)
        {
            String etag = extract(obj, "\"file_etag\":");
            String ord = extract(obj, "\"row_group_ordinal\":");
            if (etag == null || ord == null) {
                return null;
            }
            try {
                return new RowGroupRef(etag, Integer.parseInt(ord));
            }
            catch (NumberFormatException e) {
                return null;
            }
        }

        private static String extract(String obj, String key)
        {
            int at = obj.indexOf(key);
            if (at < 0) return null;
            int after = at + key.length();
            while (after < obj.length() && Character.isWhitespace(obj.charAt(after))) {
                after++;
            }
            if (after >= obj.length()) return null;
            if (obj.charAt(after) == '"') {
                int end = obj.indexOf('"', after + 1);
                if (end < 0) return null;
                return obj.substring(after + 1, end);
            }
            int end = after;
            while (end < obj.length() && "-0123456789".indexOf(obj.charAt(end)) >= 0) {
                end++;
            }
            return obj.substring(after, end);
        }
    }

    private static String jsonString(String raw)
    {
        StringBuilder sb = new StringBuilder(raw.length() + 2);
        sb.append('"');
        for (int i = 0; i < raw.length(); i++) {
            char c = raw.charAt(i);
            switch (c) {
                case '"' -> sb.append("\\\"");
                case '\\' -> sb.append("\\\\");
                case '\n' -> sb.append("\\n");
                case '\r' -> sb.append("\\r");
                case '\t' -> sb.append("\\t");
                default -> sb.append(c);
            }
        }
        sb.append('"');
        return sb.toString();
    }

    @Override
    public String toString()
    {
        return "ShelfFilterClient" + Map.of("endpoint", endpoint, "timeout", timeout);
    }
}
