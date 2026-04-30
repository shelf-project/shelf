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
package io.shelf.tag;

import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.regex.Pattern;

/**
 * SHELF-42 — A/B query tag set, Java side.
 *
 * <p>Mirrors the Rust {@code shelfd::ab_tag::TagSet} byte-for-byte at the
 * wire level. Both sides agree on:
 * <ul>
 *   <li>JSON object root with {@code String}-typed values; numbers and
 *       booleans are coerced to their JSON literal text.</li>
 *   <li>Keys sorted lexicographically before serialisation.</li>
 *   <li>At most {@value #MAX_KEYS} keys per tag set.</li>
 *   <li>Each value is at most {@value #MAX_VALUE_BYTES} UTF-8 bytes.</li>
 *   <li>Decoded payload is at most {@value #MAX_DECODED_BYTES} bytes.</li>
 *   <li>Wire form is the URL-encoded JSON object (RFC 3986 unreserved
 *       set; reserved JSON delimiters {@code {} :",}{@code }} are
 *       percent-encoded).</li>
 * </ul>
 *
 * <p>The {@link #SHELF_TAG_PREFIX} constant captures the agreed
 * session-property naming convention: any session-property whose name
 * starts with {@code shelf.tag.} is consumed by {@link
 * #fromSessionProperties(Map)} and yields a key equal to the suffix.
 *
 * <p>This class is intentionally JSON-parser-free for the build path so
 * the production plugin JAR remains zero-runtime-dependency. Parsing the
 * wire form back into a TagSet is supplied only as a test helper because
 * the Java side does not consume {@code X-Shelf-Tag} in production —
 * shelfd does.
 *
 * <p>See {@code docs/contracts/ab-tag.md} for the canonical contract and
 * {@code tests/fixtures/ab-tag-vectors.json} for the parity vectors.
 */
public final class TagSet
{
    /** HTTP header name used on every shelf-bound request. */
    public static final String HEADER_NAME = "X-Shelf-Tag";

    /** Trino session-property prefix consumed by this plugin. */
    public static final String SHELF_TAG_PREFIX = "shelf.tag.";

    /** Maximum decoded payload size in bytes. */
    public static final int MAX_DECODED_BYTES = 4096;

    /** Maximum number of {@code (key, value)} entries. */
    public static final int MAX_KEYS = 8;

    /** Maximum length of any single value, in UTF-8 bytes. */
    public static final int MAX_VALUE_BYTES = 128;

    private static final Pattern KEY_PATTERN =
            Pattern.compile("^[A-Za-z_][A-Za-z0-9_]{0,63}$");

    private static final TagSet EMPTY = new TagSet(Collections.emptySortedMap());

    private final SortedMap<String, String> entries;

    private TagSet(SortedMap<String, String> entries)
    {
        this.entries = entries;
    }

    /** The canonical empty tag set; {@code .toWire()} returns an empty {@link java.util.Optional}. */
    public static TagSet empty()
    {
        return EMPTY;
    }

    /**
     * Build a TagSet from already-validated {@code (key, value)} pairs.
     * Iteration order of {@code pairs} is irrelevant; the implementation
     * sorts and dedups (last write wins).
     *
     * @throws TagValidationException if any key fails the pattern, any
     *     value exceeds the byte cap, or the map exceeds {@link #MAX_KEYS}.
     */
    public static TagSet fromMap(Map<String, ?> pairs)
    {
        Objects.requireNonNull(pairs, "pairs");
        if (pairs.isEmpty()) {
            return EMPTY;
        }
        if (pairs.size() > MAX_KEYS) {
            throw new TagValidationException(
                    "tag set has " + pairs.size() + " keys; cap is " + MAX_KEYS);
        }
        SortedMap<String, String> sorted = new TreeMap<>();
        for (Map.Entry<String, ?> e : pairs.entrySet()) {
            String key = e.getKey();
            if (key == null || !KEY_PATTERN.matcher(key).matches()) {
                throw new TagValidationException("rejected key " + safe(key));
            }
            String value = coerce(e.getValue());
            int byteLen = value.getBytes(StandardCharsets.UTF_8).length;
            if (byteLen > MAX_VALUE_BYTES) {
                throw new TagValidationException(
                        "value for " + safe(key) + " is " + byteLen
                                + " B; cap is " + MAX_VALUE_BYTES);
            }
            sorted.put(key, value);
        }
        return new TagSet(Collections.unmodifiableSortedMap(sorted));
    }

    /**
     * Convenience builder for callers that already have a Trino
     * {@code Map<String,String>} of session properties (or
     * {@code clientTags}-derived flat map). Keys not starting with
     * {@link #SHELF_TAG_PREFIX} are silently ignored. The resulting tag
     * keys are the suffixes (e.g. {@code shelf.tag.experiment} ⇒ {@code
     * experiment}).
     */
    public static TagSet fromSessionProperties(Map<String, String> session)
    {
        Objects.requireNonNull(session, "session");
        Map<String, String> filtered = new LinkedHashMap<>();
        for (Map.Entry<String, String> e : session.entrySet()) {
            String name = e.getKey();
            if (name == null || !name.startsWith(SHELF_TAG_PREFIX)) {
                continue;
            }
            String suffix = name.substring(SHELF_TAG_PREFIX.length());
            if (suffix.isEmpty()) {
                continue;
            }
            String value = e.getValue();
            if (value == null) {
                continue;
            }
            filtered.put(suffix, value);
        }
        if (filtered.isEmpty()) {
            return EMPTY;
        }
        return fromMap(filtered);
    }

    /** Sorted view over the tag entries. */
    public Map<String, String> asMap()
    {
        return entries;
    }

    public Set<String> keys()
    {
        return entries.keySet();
    }

    public boolean isEmpty()
    {
        return entries.isEmpty();
    }

    public int size()
    {
        return entries.size();
    }

    /**
     * Render the canonical JSON object literal — keys sorted, values
     * always quoted strings. This is the body that the wire form
     * URL-encodes.
     */
    public String toJson()
    {
        if (entries.isEmpty()) {
            return "{}";
        }
        StringBuilder sb = new StringBuilder(32 + entries.size() * 24);
        sb.append('{');
        boolean first = true;
        for (Map.Entry<String, String> e : entries.entrySet()) {
            if (!first) {
                sb.append(',');
            }
            sb.append('"');
            jsonEscape(sb, e.getKey());
            sb.append('"').append(':').append('"');
            jsonEscape(sb, e.getValue());
            sb.append('"');
            first = false;
        }
        sb.append('}');
        return sb.toString();
    }

    /**
     * Render the wire form — URL-encoded JSON object literal — that ships
     * as the {@link #HEADER_NAME} header value. Returns {@code null} for
     * an empty tag set so the caller can omit the header entirely.
     */
    public String toWire()
    {
        if (entries.isEmpty()) {
            return null;
        }
        return percentEncode(toJson());
    }

    /**
     * Test seam: parse a wire form back into a TagSet. Production code
     * does NOT call this — shelfd parses {@code X-Shelf-Tag}; this
     * method exists so the parity test can round-trip Rust-emitted
     * fixtures through Java.
     *
     * @throws TagValidationException for any contract violation; callers
     *     in production paths would map this to "header absent".
     */
    public static TagSet fromWire(String wire)
    {
        Objects.requireNonNull(wire, "wire");
        if (wire.isEmpty()) {
            throw new TagValidationException("empty X-Shelf-Tag payload");
        }
        if (wire.length() > MAX_DECODED_BYTES * 4) {
            throw new TagValidationException(
                    "X-Shelf-Tag payload " + wire.length() + " B exceeds cap");
        }
        String decoded = percentDecode(wire);
        if (decoded.isEmpty()) {
            throw new TagValidationException("empty X-Shelf-Tag payload");
        }
        if (decoded.getBytes(StandardCharsets.UTF_8).length > MAX_DECODED_BYTES) {
            throw new TagValidationException(
                    "decoded X-Shelf-Tag exceeds " + MAX_DECODED_BYTES + " B cap");
        }
        Map<String, String> parsed = MinimalJsonObjectParser.parse(decoded);
        return fromMap(parsed);
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (!(o instanceof TagSet other)) {
            return false;
        }
        return entries.equals(other.entries);
    }

    @Override
    public int hashCode()
    {
        return entries.hashCode();
    }

    @Override
    public String toString()
    {
        return "TagSet" + entries;
    }

    private static String coerce(Object value)
    {
        if (value == null) {
            return "";
        }
        if (value instanceof String s) {
            return s;
        }
        if (value instanceof Boolean || value instanceof Number) {
            return value.toString();
        }
        return value.toString();
    }

    private static String safe(String s)
    {
        return s == null ? "<null>" : "\"" + s + "\"";
    }

    private static void jsonEscape(StringBuilder sb, String s)
    {
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            switch (c) {
                case '\\' -> sb.append("\\\\");
                case '"' -> sb.append("\\\"");
                case '\n' -> sb.append("\\n");
                case '\r' -> sb.append("\\r");
                case '\t' -> sb.append("\\t");
                default -> {
                    if (c < 0x20) {
                        sb.append(String.format("\\u%04x", (int) c));
                    }
                    else {
                        sb.append(c);
                    }
                }
            }
        }
    }

    /**
     * Percent-encode a JSON literal so it survives in a single HTTP
     * header value. Must agree byte-for-byte with the Rust
     * {@code TAG_ENCODE_SET} in {@code shelfd::ab_tag} for parity with
     * the golden vectors.
     */
    static String percentEncode(String input)
    {
        StringBuilder sb = new StringBuilder(input.length() * 2);
        byte[] bytes = input.getBytes(StandardCharsets.UTF_8);
        for (byte b : bytes) {
            int c = b & 0xff;
            if (isUnreserved(c)) {
                sb.append((char) c);
            }
            else {
                sb.append('%');
                sb.append(hex(c >>> 4));
                sb.append(hex(c & 0x0f));
            }
        }
        return sb.toString();
    }

    /**
     * Strict mirror of the Rust {@code TAG_ENCODE_SET}. Anything not
     * marked here is percent-encoded.
     */
    private static boolean isUnreserved(int c)
    {
        if (c <= 0x20) {
            return false;
        }
        if ((c >= '0' && c <= '9') || (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z')) {
            return true;
        }
        // RFC 3986 unreserved minus: anything in TAG_ENCODE_SET stays out.
        return switch (c) {
            case '-', '.', '_', '~' -> true;
            default -> false;
        };
    }

    private static char hex(int nib)
    {
        return (char) (nib < 10 ? '0' + nib : 'A' + nib - 10);
    }

    /**
     * Lenient percent-decoder for the test seam {@link #fromWire}. Does
     * not enforce upper/lower hex case — accepts both. Rejects truncated
     * escapes by throwing {@link TagValidationException}.
     */
    static String percentDecode(String input)
    {
        byte[] in = input.getBytes(StandardCharsets.US_ASCII);
        byte[] out = new byte[in.length];
        int o = 0;
        for (int i = 0; i < in.length; i++) {
            int c = in[i] & 0xff;
            if (c == '%') {
                if (i + 2 >= in.length) {
                    throw new TagValidationException("truncated %-escape in X-Shelf-Tag");
                }
                int hi = fromHex(in[i + 1] & 0xff);
                int lo = fromHex(in[i + 2] & 0xff);
                out[o++] = (byte) ((hi << 4) | lo);
                i += 2;
            }
            else {
                out[o++] = (byte) c;
            }
        }
        return new String(out, 0, o, StandardCharsets.UTF_8);
    }

    private static int fromHex(int c)
    {
        if (c >= '0' && c <= '9') {
            return c - '0';
        }
        if (c >= 'A' && c <= 'F') {
            return 10 + c - 'A';
        }
        if (c >= 'a' && c <= 'f') {
            return 10 + c - 'a';
        }
        throw new TagValidationException("bad hex digit in X-Shelf-Tag");
    }

    /** Thrown by tag construction or decode when the contract is violated. */
    public static final class TagValidationException
            extends IllegalArgumentException
    {
        private static final long serialVersionUID = 1L;

        public TagValidationException(String message)
        {
            super(message);
        }
    }
}
