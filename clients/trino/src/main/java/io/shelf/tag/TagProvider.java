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

/**
 * SHELF-42 — extension point for resolving a {@link TagSet} per Shelf
 * request.
 *
 * <p>The Trino plugin's hot path is the file-system / range-fetcher
 * pair. Once
 * <a href="https://github.com/trinodb/trino/issues/29184">trinodb/trino#29184</a>
 * lands and Shelf can register a real
 * {@code TrinoFileSystemFactory}, those calls will run on a Trino
 * worker thread that has access to a {@code ConnectorSession} (or the
 * lower-level {@code ConnectorIdentity}). The session carries:
 *
 * <ul>
 *   <li>session properties (e.g. {@code shelf.tag.experiment});</li>
 *   <li>{@code clientTags} (free-form Set&lt;String&gt; that survives
 *       across cluster hops);</li>
 *   <li>{@code QueryId} and {@code QueryContext} — the SHELF-37
 *       listener consumes the same identity to populate the
 *       {@code tags_json} column.</li>
 * </ul>
 *
 * <p>This interface abstracts that resolution: the operator wires up a
 * {@code TagProvider} that knows how to read whichever surface their
 * Trino setup uses, the plugin calls {@link #currentTag()} on every
 * outbound HTTP request, and the resulting wire form is attached as
 * {@link TagSet#HEADER_NAME}.
 *
 * <p>Implementations MUST be thread-safe — Trino calls plugins from
 * many worker threads concurrently. Implementations MUST be fail-open:
 * any internal exception MUST surface as {@link TagSet#empty()} rather
 * than be propagated. The plugin's HTTP interceptor catches
 * {@link RuntimeException} defensively, but contract first.
 */
@FunctionalInterface
public interface TagProvider
{
    /** Default {@link TagProvider} that always returns {@link TagSet#empty()}. */
    TagProvider EMPTY = () -> TagSet.empty();

    /**
     * Resolve the tag set for the current Shelf request. Implementations
     * typically pull from a thread-local set by an upstream Trino seam
     * (see {@link io.shelf.tag.SessionTagProvider#install(java.util.Map)}).
     */
    TagSet currentTag();
}
