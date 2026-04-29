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
package io.shelf.listener.extract;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.trino.spi.ErrorCode;
import io.trino.spi.eventlistener.QueryCompletedEvent;
import io.trino.spi.eventlistener.QueryContext;
import io.trino.spi.eventlistener.QueryFailureInfo;
import io.trino.spi.eventlistener.QueryIOMetadata;
import io.trino.spi.eventlistener.QueryInputMetadata;
import io.trino.spi.eventlistener.QueryMetadata;
import io.trino.spi.eventlistener.QueryOutputMetadata;
import io.trino.spi.eventlistener.QueryStatistics;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Pure {@code QueryCompletedEvent} → {@link ExtractedRow} projection.
 * Stateless except for the per-instance Jackson mapper. Easy to unit-test:
 * see {@code EventExtractorTest}.
 *
 * <p>The {@code shelf.tag.*} session-property contract for SHELF-42:
 * any session property whose key starts with {@code shelf.tag.} ends up
 * in {@code tags_json} as the bare suffix → value (e.g. session prop
 * {@code shelf.tag.experiment=foo} → {@code {"experiment":"foo"}}).
 * The contract is intentionally narrow so the SHELF-37 row schema does
 * not have to track every future tag dimension; consumers parse the JSON.
 */
public final class EventExtractor
{
    /** Session-property prefix that enrolls a value into {@code tags_json}. */
    public static final String SHELF_TAG_PREFIX = "shelf.tag.";

    private final int queryTextMaxBytes;
    private final ObjectMapper json;

    public EventExtractor(int queryTextMaxBytes)
    {
        if (queryTextMaxBytes < 256) {
            throw new IllegalArgumentException("queryTextMaxBytes must be >= 256");
        }
        this.queryTextMaxBytes = queryTextMaxBytes;
        this.json = new ObjectMapper();
    }

    public ExtractedRow extract(QueryCompletedEvent event)
    {
        QueryMetadata md = event.getMetadata();
        QueryStatistics stats = event.getStatistics();
        QueryContext ctx = event.getContext();
        QueryIOMetadata io = event.getIoMetadata();
        Optional<QueryFailureInfo> failure = event.getFailureInfo();

        ExtractedRow.Builder b = ExtractedRow.builder();
        b.queryId = nullToEmpty(md.getQueryId());
        b.queryState = nullToEmpty(md.getQueryState());

        failure.ifPresent(f -> {
            ErrorCode ec = f.getErrorCode();
            if (ec != null) {
                b.errorCode = ec.getName();
                if (ec.getType() != null) {
                    b.errorType = ec.getType().name();
                }
            }
            b.errorMessage = f.getFailureMessage().orElse(null);
        });

        b.principal = ctx.getPrincipal().orElse(null);
        b.user = nullToEmpty(ctx.getUser());
        b.source = ctx.getSource().orElse(null);
        b.catalog = ctx.getCatalog().orElse(null);
        b.schemaName = ctx.getSchema().orElse(null);
        b.resourceGroupId = ctx.getResourceGroupId()
                .map(rg -> String.join(".", rg.getSegments()))
                .orElse(null);
        b.serverAddress = ctx.getServerAddress();

        String rawText = nullToEmpty(md.getQuery());
        b.queryText = truncateUtf8(rawText, queryTextMaxBytes);
        b.queryHash = sha256Hex(rawText);

        b.createTime = toOffsetDateTime(event.getCreateTime());
        b.endTime = toOffsetDateTime(event.getEndTime());

        b.queuedTimeMillis = toMillis(stats.getQueuedTime());
        b.planningTimeMillis = stats.getPlanningTime().map(EventExtractor::toMillis).orElse(0L);
        b.executeTimeMillis = stats.getExecutionTime().map(EventExtractor::toMillis).orElse(0L);
        b.wallTimeMillis = toMillis(stats.getWallTime());
        b.cpuTimeMillis = toMillis(stats.getCpuTime());

        b.physicalInputBytes = stats.getPhysicalInputBytes();
        b.physicalInputReadTimeMillis = stats.getPhysicalInputReadTime()
                .map(EventExtractor::toMillis).orElse(0L);
        b.physicalInputRows = stats.getPhysicalInputRows();
        b.processedInputBytes = stats.getProcessedInputBytes();
        b.processedInputRows = stats.getProcessedInputRows();
        b.outputBytes = stats.getOutputBytes();
        b.outputRows = stats.getOutputRows();
        b.peakUserMemoryBytes = stats.getPeakUserMemoryBytes();
        b.peakTotalMemoryBytes = stats.getPeakTaskTotalMemory();

        b.inputsJson = inputsJson(io);
        b.outputsJson = outputsJson(io);
        b.tagsJson = tagsJson(ctx.getSessionProperties());

        return b.build();
    }

    private String inputsJson(QueryIOMetadata io)
    {
        List<Map<String, Object>> rows = new ArrayList<>();
        for (QueryInputMetadata in : io.getInputs()) {
            Map<String, Object> row = new LinkedHashMap<>();
            row.put("catalog", in.getCatalogName());
            row.put("schema", in.getSchema());
            row.put("table", in.getTable());
            in.getConnectorName().ifPresent(c -> row.put("connector", c));
            in.getPhysicalInputBytes().ifPresent(v -> row.put("physical_input_bytes", v));
            in.getPhysicalInputRows().ifPresent(v -> row.put("physical_input_rows", v));
            rows.add(row);
        }
        return writeJson(rows, "[]");
    }

    private String outputsJson(QueryIOMetadata io)
    {
        Optional<QueryOutputMetadata> out = io.getOutput();
        if (out.isEmpty()) {
            return "[]";
        }
        QueryOutputMetadata o = out.get();
        Map<String, Object> row = new LinkedHashMap<>();
        row.put("catalog", o.getCatalogName());
        row.put("schema", o.getSchema());
        row.put("table", o.getTable());
        return writeJson(List.of(row), "[]");
    }

    private String tagsJson(Map<String, String> sessionProperties)
    {
        if (sessionProperties == null || sessionProperties.isEmpty()) {
            return "{}";
        }
        Map<String, String> tags = new LinkedHashMap<>();
        for (Map.Entry<String, String> e : sessionProperties.entrySet()) {
            String k = e.getKey();
            if (k != null && k.startsWith(SHELF_TAG_PREFIX) && k.length() > SHELF_TAG_PREFIX.length()) {
                tags.put(k.substring(SHELF_TAG_PREFIX.length()), e.getValue());
            }
        }
        if (tags.isEmpty()) {
            return "{}";
        }
        return writeJson(tags, "{}");
    }

    private String writeJson(Object value, String fallback)
    {
        try {
            return json.writeValueAsString(value);
        }
        catch (JsonProcessingException ex) {
            return fallback;
        }
    }

    static String truncateUtf8(String s, int maxBytes)
    {
        if (s == null) {
            return "";
        }
        byte[] bytes = s.getBytes(StandardCharsets.UTF_8);
        if (bytes.length <= maxBytes) {
            return s;
        }
        // Walk back to a valid UTF-8 boundary so we never hand the writer
        // a half-encoded codepoint (would otherwise corrupt the column on
        // any reader that strict-validates UTF-8).
        int cut = maxBytes;
        while (cut > 0 && (bytes[cut] & 0xC0) == 0x80) {
            cut--;
        }
        return new String(bytes, 0, cut, StandardCharsets.UTF_8);
    }

    static String sha256Hex(String s)
    {
        if (s == null) {
            s = "";
        }
        try {
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            byte[] digest = md.digest(s.getBytes(StandardCharsets.UTF_8));
            return HexFormat.of().formatHex(digest);
        }
        catch (NoSuchAlgorithmException e) {
            // SHA-256 is mandated by every JDK since Java 1.4. Treat as fatal.
            throw new IllegalStateException("SHA-256 unavailable", e);
        }
    }

    private static long toMillis(Duration d)
    {
        return d == null ? 0L : d.toMillis();
    }

    private static OffsetDateTime toOffsetDateTime(java.time.Instant instant)
    {
        return instant == null ? null : instant.atOffset(ZoneOffset.UTC);
    }

    private static String nullToEmpty(String s)
    {
        return s == null ? "" : s;
    }
}
