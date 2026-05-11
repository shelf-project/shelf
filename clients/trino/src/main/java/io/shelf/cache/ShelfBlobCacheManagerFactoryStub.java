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
package io.shelf.cache;

/**
 * Placeholder for the future {@code BlobCacheManagerFactory} hook proposed in
 * <a href="https://github.com/trinodb/trino/pull/29184">trinodb/trino#29184</a>.
 *
 * <p>The Trino SPI in the 480 line does not yet expose blob-cache registration on
 * {@link io.trino.spi.Plugin}; Shelf continues to use the S3 endpoint shim (ADR-0012).
 * When #29184 merges, replace this stub with a real implementation that:
 * <ul>
 *   <li>delegates cold reads to the native S3/Trino file-system stack;</li>
 *   <li>routes cacheable ranges through shelfd (HTTP + optional Flight);</li>
 *   <li>fails open with backoff on shelfd errors (same contract as {@code ShelfFileSystemFactory}).</li>
 * </ul>
 *
 * <p>This class intentionally references no draft {@code io.trino.spi.cache} types so
 * {@code mvn -pl clients/trino} keeps compiling against released Trino BOMs.
 */
public final class ShelfBlobCacheManagerFactoryStub
{
    private ShelfBlobCacheManagerFactoryStub() {}

    public static String spIssue()
    {
        return "29184";
    }
}
