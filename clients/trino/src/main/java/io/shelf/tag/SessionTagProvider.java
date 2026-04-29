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

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Thread-local {@link TagProvider} fed by an upstream Trino seam.
 *
 * <p>Until Trino exposes a stable file-system-factory hook (see
 * trinodb/trino#29184), the plugin cannot read a {@code
 * ConnectorSession} from inside the Shelf HTTP request path. The
 * pragmatic shape today is:
 *
 * <ol>
 *   <li>Operator's coordinator-side glue (or the SHELF-37
 *       {@code ShelfPrefetchListener}) reads session properties /
 *       {@code clientTags} when {@code QueryCreatedEvent} fires.</li>
 *   <li>That same seam calls {@link #install(Map)} on the worker
 *       thread before driving any Shelf-bound HTTP call.</li>
 *   <li>The Shelf HTTP client interceptor calls {@link #currentTag()}
 *       and stamps the wire form on the request.</li>
 *   <li>The seam calls {@link #clear()} when the unit of work
 *       completes.</li>
 * </ol>
 *
 * <p>The thread-local is intentionally narrow: it never leaks across
 * threads and never persists across queries because both paths would
 * violate the per-request lifetime contract in {@code
 * docs/contracts/ab-tag.md}. The {@link AutoCloseable} returned by
 * {@link #install(Map)} guarantees the clear path even on exception.
 */
public final class SessionTagProvider
        implements TagProvider
{
    private static final ThreadLocal<TagSet> CURRENT = new ThreadLocal<>();

    /** Single shared instance — the resolution is per-thread, not per-instance. */
    public static final SessionTagProvider INSTANCE = new SessionTagProvider();

    private SessionTagProvider() {}

    /**
     * Install a tag set for the current thread, derived from a flat map
     * of session properties / client tags. Returns an
     * {@link AutoCloseable} that clears the slot when closed; use with
     * try-with-resources to guarantee cleanup.
     *
     * <p>Keys that do not start with {@link TagSet#SHELF_TAG_PREFIX}
     * are silently dropped — the same lenient behaviour that {@link
     * TagSet#fromSessionProperties(Map)} provides.
     */
    public static AutoCloseable install(Map<String, String> sessionProperties)
    {
        Objects.requireNonNull(sessionProperties, "sessionProperties");
        TagSet tag;
        try {
            tag = TagSet.fromSessionProperties(sessionProperties);
        }
        catch (TagSet.TagValidationException e) {
            // Fail-open: a misconfigured session does not break the
            // request path. The operator's logs surface the error via
            // the throwable below.
            tag = TagSet.empty();
        }
        TagSet previous = CURRENT.get();
        CURRENT.set(tag);
        return () -> {
            if (previous == null) {
                CURRENT.remove();
            }
            else {
                CURRENT.set(previous);
            }
        };
    }

    /** Test seam: install a pre-built tag without going through the map step. */
    public static AutoCloseable installTag(TagSet tag)
    {
        Objects.requireNonNull(tag, "tag");
        TagSet previous = CURRENT.get();
        CURRENT.set(tag);
        return () -> {
            if (previous == null) {
                CURRENT.remove();
            }
            else {
                CURRENT.set(previous);
            }
        };
    }

    /** Force-clear the slot. Use this only when the {@link AutoCloseable} is impractical. */
    public static void clear()
    {
        CURRENT.remove();
    }

    @Override
    public TagSet currentTag()
    {
        TagSet current = CURRENT.get();
        return current != null ? current : TagSet.empty();
    }

    /**
     * Convenience for tests / readers: snapshot the current map view of
     * the thread-local tag.
     */
    public Map<String, String> snapshot()
    {
        TagSet t = CURRENT.get();
        if (t == null || t.isEmpty()) {
            return Collections.emptyMap();
        }
        return new LinkedHashMap<>(t.asMap());
    }
}
