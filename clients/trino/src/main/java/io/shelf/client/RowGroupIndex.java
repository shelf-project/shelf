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
 * Maps a byte-range {@code [offset, offset + length)} within a single
 * S3 object to the Parquet row-group {@code ordinal} that covers it.
 *
 * <p><b>Why (SHELF-16).</b> The cache key is
 * {@code sha256(etag || le_u64(offset) || le_u64(length) || le_u32(rg_ordinal))}.
 * Making {@code rg_ordinal} part of the preimage lets the plugin
 * distinguish reads of different row groups that happen to share a
 * byte range — for example, when Trino asks for the same 4 KiB page
 * from two different files whose layout happens to align. Without
 * the ordinal, the two reads would collide on one key, one of them
 * would win the cache slot, and the other would silently get wrong
 * bytes on the next read. With the ordinal, the two reads are
 * distinct keys and the cache behaves correctly.
 *
 * <p><b>Permissive default.</b> When no footer has been parsed (the
 * file is not Parquet, the footer isn't available yet, or parsing
 * failed), {@link #constantZero()} returns an index that maps every
 * range to ordinal {@code 0}. Ordinal {@code 0} is the plugin's
 * canonical "unknown" marker; non-Parquet paths therefore keep the
 * pre-SHELF-16 key shape and there is no correctness cost as long as
 * a single file is never read both under a real {@link RowGroupIndex}
 * and under the constant-zero fallback. That invariant is upheld by
 * {@code ShelfInputStream} binding the index for the life of the
 * stream.
 *
 * <p><b>Thread safety.</b> Implementations must be safe to share
 * across all Trino worker threads; the plugin passes one index per
 * input file and reads it under per-split concurrency. The bundled
 * implementations ({@link ConstantOrdinalIndex}, the scaffolded
 * {@code ParquetFooterIndex}) are effectively immutable.
 */
public interface RowGroupIndex
{
    /**
     * Return the row-group ordinal covering
     * {@code [offset, offset + length)}.
     *
     * <p>If the range spans multiple row groups, the implementation
     * SHOULD return the ordinal of the first (lowest file-offset) row
     * group covered; the caller's read will then be keyed under that
     * ordinal. This is a conservative choice — multi-row-group reads
     * are rare in practice (Trino's Parquet reader issues per
     * row-group range GETs), and when they do happen a single key
     * is strictly better than a key that doesn't exist in the cache
     * at all. Implementations MAY return {@code 0} instead and
     * degrade that read to the unknown-ordinal namespace.
     *
     * <p>Returns {@code 0} if the range does not map to any known
     * row group (e.g. a footer read, or a file without parsed row
     * groups). Callers must treat {@code 0} as "unknown" rather than
     * "row group zero with high confidence" — which is why the key
     * derivation uses {@code 0} as the sentinel too.
     */
    int ordinalFor(long offset, long length);

    /**
     * @return {@code true} if this index has parsed row-group metadata
     *         and can return non-zero ordinals for at least some
     *         ranges. {@link #constantZero()} returns {@code false};
     *         a populated {@code ParquetFooterIndex} returns {@code true}.
     */
    boolean hasKnownOrdinals();

    /**
     * Sentinel index that maps every range to ordinal {@code 0}. Use
     * this for non-Parquet files and for Parquet files whose footer
     * has not been parsed yet. See {@link ConstantOrdinalIndex} for
     * the (stateless) concrete type.
     */
    static RowGroupIndex constantZero()
    {
        return ConstantOrdinalIndex.INSTANCE;
    }
}
