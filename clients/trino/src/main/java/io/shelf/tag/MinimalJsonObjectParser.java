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

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Strict, allocation-light parser for the narrow JSON shape that the
 * SHELF-42 contract permits inside an {@code X-Shelf-Tag} header:
 *
 * <pre>
 *   '{' (string ':' (string|number|true|false) (',' string ':' value)*)? '}'
 * </pre>
 *
 * <p>Anything richer — nested objects, arrays, {@code null} values,
 * trailing commas, comments — is rejected via
 * {@link TagSet.TagValidationException}.
 *
 * <p>Kept package-private so the production runtime jar inherits zero
 * JSON-parser dependencies. The Trino plugin ships only this 200-line
 * walker, not Jackson.
 */
final class MinimalJsonObjectParser
{
    private final String src;
    private int pos;

    private MinimalJsonObjectParser(String src)
    {
        this.src = src;
    }

    static Map<String, String> parse(String src)
    {
        MinimalJsonObjectParser p = new MinimalJsonObjectParser(src);
        Map<String, String> out = p.parseRootObject();
        p.skipWs();
        if (p.pos != p.src.length()) {
            throw fail("trailing data after JSON object", p.pos);
        }
        return out;
    }

    private Map<String, String> parseRootObject()
    {
        skipWs();
        expect('{');
        skipWs();
        Map<String, String> out = new LinkedHashMap<>();
        if (peek() == '}') {
            pos++;
            return out;
        }
        while (true) {
            skipWs();
            String key = parseString();
            skipWs();
            expect(':');
            skipWs();
            String value = parseValue();
            out.put(key, value);
            skipWs();
            char c = peek();
            if (c == ',') {
                pos++;
            }
            else if (c == '}') {
                pos++;
                break;
            }
            else {
                throw fail("expected ',' or '}'", pos);
            }
        }
        return out;
    }

    private String parseString()
    {
        expect('"');
        StringBuilder sb = new StringBuilder();
        while (pos < src.length()) {
            char c = src.charAt(pos++);
            if (c == '"') {
                return sb.toString();
            }
            if (c == '\\') {
                if (pos >= src.length()) {
                    throw fail("truncated escape", pos);
                }
                char esc = src.charAt(pos++);
                switch (esc) {
                    case '"' -> sb.append('"');
                    case '\\' -> sb.append('\\');
                    case '/' -> sb.append('/');
                    case 'n' -> sb.append('\n');
                    case 'r' -> sb.append('\r');
                    case 't' -> sb.append('\t');
                    case 'b' -> sb.append('\b');
                    case 'f' -> sb.append('\f');
                    case 'u' -> {
                        if (pos + 4 > src.length()) {
                            throw fail("truncated \\u escape", pos);
                        }
                        int cp = Integer.parseInt(src.substring(pos, pos + 4), 16);
                        sb.append((char) cp);
                        pos += 4;
                    }
                    default -> throw fail("bad string escape \\" + esc, pos);
                }
            }
            else if (c < 0x20) {
                throw fail("unescaped control char in string", pos);
            }
            else {
                sb.append(c);
            }
        }
        throw fail("unterminated string", pos);
    }

    /**
     * Parse a JSON value and coerce it to a string per the contract:
     * strings stay as-is; numbers and booleans are stringified.
     * Anything else (object, array, {@code null}) is rejected.
     */
    private String parseValue()
    {
        if (pos >= src.length()) {
            throw fail("unexpected end of input where value expected", pos);
        }
        char c = src.charAt(pos);
        if (c == '"') {
            return parseString();
        }
        if (c == 't') {
            expectLiteral("true");
            return "true";
        }
        if (c == 'f') {
            expectLiteral("false");
            return "false";
        }
        if (c == '-' || (c >= '0' && c <= '9')) {
            return parseNumber();
        }
        if (c == 'n') {
            throw fail("null is not a permitted value", pos);
        }
        if (c == '{') {
            throw fail("nested object not permitted", pos);
        }
        if (c == '[') {
            throw fail("array not permitted", pos);
        }
        throw fail("unexpected character '" + c + "'", pos);
    }

    private String parseNumber()
    {
        int start = pos;
        if (peek() == '-') {
            pos++;
        }
        while (pos < src.length()) {
            char c = src.charAt(pos);
            if (c == '.' || c == '+' || c == '-' || c == 'e' || c == 'E' || (c >= '0' && c <= '9')) {
                pos++;
            }
            else {
                break;
            }
        }
        if (start == pos) {
            throw fail("expected number", pos);
        }
        return src.substring(start, pos);
    }

    private void expectLiteral(String literal)
    {
        if (!src.regionMatches(pos, literal, 0, literal.length())) {
            throw fail("expected literal " + literal, pos);
        }
        pos += literal.length();
    }

    private void expect(char c)
    {
        if (pos >= src.length() || src.charAt(pos) != c) {
            throw fail("expected '" + c + "'", pos);
        }
        pos++;
    }

    private char peek()
    {
        return pos < src.length() ? src.charAt(pos) : '\0';
    }

    private void skipWs()
    {
        while (pos < src.length()) {
            char c = src.charAt(pos);
            if (c == ' ' || c == '\t' || c == '\n' || c == '\r') {
                pos++;
            }
            else {
                break;
            }
        }
    }

    private static TagSet.TagValidationException fail(String why, int pos)
    {
        return new TagSet.TagValidationException(why + " (pos=" + pos + ")");
    }
}
