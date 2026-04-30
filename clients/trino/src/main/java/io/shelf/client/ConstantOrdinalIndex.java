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

/**
 * Stateless {@link RowGroupIndex} that returns ordinal {@code 0} for
 * every byte range. This is the permissive default the plugin uses
 * when no footer has been parsed (non-Parquet files, Parquet files
 * whose footer hasn't been fetched yet, Parquet files whose parse
 * failed).
 *
 * <p>Returning {@code 0} means every key for a given file falls into
 * the "unknown ordinal" namespace — which is exactly the pre-SHELF-16
 * key shape, so the fallback is behaviour-preserving. Singleton by
 * design; callers should prefer {@link RowGroupIndex#constantZero()}.
 */
public final class ConstantOrdinalIndex
        implements RowGroupIndex
{
    /** Single shared instance. The type is stateless and immutable. */
    public static final ConstantOrdinalIndex INSTANCE = new ConstantOrdinalIndex();

    private ConstantOrdinalIndex()
    {
    }

    @Override
    public int ordinalFor(long offset, long length)
    {
        return 0;
    }

    @Override
    public boolean hasKnownOrdinals()
    {
        return false;
    }

    @Override
    public String toString()
    {
        return "ConstantOrdinalIndex(ordinal=0)";
    }
}
