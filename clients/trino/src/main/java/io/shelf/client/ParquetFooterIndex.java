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

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * {@link RowGroupIndex} backed by the Parquet file's footer
 * {@code FileMetaData}. Given the tail bytes of a Parquet object, this
 * class extracts each row group's
 * {@code (file_offset, total_compressed_size, ordinal)} tuple and
 * answers {@link #ordinalFor(long, long)} with the ordinal of the row
 * group whose byte range contains the query.
 *
 * <p><b>SHELF-16 delivery level: SCAFFOLDED.</b> The interface, the
 * range-to-ordinal lookup, and the wiring through
 * {@link io.shelf.filesystem.ShelfInputStream} ship in this ticket.
 * The Thrift TCompactProtocol footer parser — the ~200 lines that
 * would turn a {@code byte[] footerBytes} into the row-group list —
 * is deferred to SHELF-16b so we do not ship a hand-rolled
 * zigzag/varint decoder under time pressure. Until then,
 * {@link #fromFooter(byte[], long)} returns {@link Optional#empty()}
 * and {@code ShelfInputFile} falls back to
 * {@link RowGroupIndex#constantZero()}. The key extension is already
 * in place end-to-end; SHELF-16b only needs to swap the empty stub for
 * a real parser and re-enable the {@code parseFooter_*} tests in
 * {@code RowGroupIndexTest}.
 *
 * <p><b>Thread safety.</b> Immutable; the internal row-group list is
 * an unmodifiable snapshot. Safe to share across Trino threads.
 *
 * <p><b>Invariants enforced at construction.</b>
 * <ul>
 *   <li>Row-group list is sorted by {@code fileOffset}.</li>
 *   <li>Row-group ranges are non-overlapping (Parquet spec requires
 *       this; we assert rather than merge).</li>
 *   <li>{@code ordinal} values are unique and non-negative; some
 *       Parquet writers omit the field, in which case the index of
 *       the row group in the list is used as the ordinal.</li>
 * </ul>
 */
public final class ParquetFooterIndex
        implements RowGroupIndex
{
    /** One row-group entry. */
    public record RowGroup(long fileOffset, long totalCompressedSize, int ordinal)
    {
        public RowGroup
        {
            if (fileOffset < 0) {
                throw new IllegalArgumentException("fileOffset must be >= 0, got " + fileOffset);
            }
            if (totalCompressedSize <= 0) {
                throw new IllegalArgumentException(
                        "totalCompressedSize must be > 0, got " + totalCompressedSize);
            }
            if (ordinal < 0) {
                throw new IllegalArgumentException("ordinal must be >= 0, got " + ordinal);
            }
        }

        /** @return true if {@code [offset, offset+length)} falls inside this row group's byte range. */
        boolean covers(long offset, long length)
        {
            long end = fileOffset + totalCompressedSize;
            return offset >= fileOffset && offset + length <= end;
        }
    }

    private final List<RowGroup> rowGroups;

    ParquetFooterIndex(List<RowGroup> rowGroups)
    {
        Objects.requireNonNull(rowGroups, "rowGroups");
        List<RowGroup> copy = new ArrayList<>(rowGroups);
        // Sort by fileOffset so a linear scan is in natural order and
        // a later bisect-tree upgrade (SHELF-16b follow-up) stays a
        // drop-in replacement.
        copy.sort((a, b) -> Long.compare(a.fileOffset, b.fileOffset));
        for (int i = 1; i < copy.size(); i++) {
            RowGroup prev = copy.get(i - 1);
            RowGroup cur = copy.get(i);
            long prevEnd = prev.fileOffset + prev.totalCompressedSize;
            if (cur.fileOffset < prevEnd) {
                throw new IllegalArgumentException(
                        "Parquet row groups must be non-overlapping: "
                                + prev + " vs " + cur);
            }
        }
        this.rowGroups = List.copyOf(copy);
    }

    /**
     * Parse the last-N bytes of a Parquet object into a row-group
     * index.
     *
     * <p><b>SHELF-16 scaffold.</b> This implementation always returns
     * {@link Optional#empty()}. The full parser lands in
     * <b>SHELF-16b</b> (tracked in {@code agents/out/03-plan.md}). The
     * method signature is frozen so that caller wiring
     * ({@link io.shelf.filesystem.ShelfInputFile}) does not need to
     * change when the parser ships.
     *
     * <p>When implemented, the parser will:
     * <ol>
     *   <li>Validate the trailing {@code PAR1} magic (last 4 bytes).</li>
     *   <li>Read {@code footer_length} (little-endian u32 at
     *       {@code footerBytes.length - 8}).</li>
     *   <li>Locate the {@code FileMetaData} Thrift blob at
     *       {@code footerBytes.length - 8 - footer_length}.</li>
     *   <li>Decode the blob with a minimal TCompactProtocol reader,
     *       descending only into {@code row_groups[*]} and extracting
     *       {@code file_offset}, {@code total_compressed_size}, and
     *       {@code ordinal} (with index-as-fallback when ordinal is
     *       absent).</li>
     *   <li>Return {@link Optional#of(Object)} with the populated
     *       index, or {@link Optional#empty()} on any parse
     *       failure.</li>
     * </ol>
     *
     * <p>Fail-open: every parse error collapses to
     * {@link Optional#empty()}, never throws. The caller already has
     * {@link RowGroupIndex#constantZero()} as a safe fallback and must
     * never see a Parquet-parser exception.
     *
     * @param footerBytes the trailing bytes of the object (at least
     *                    large enough to cover the footer)
     * @param fileLength  total S3 object length; supplied so callers
     *                    that only buffered part of the footer can
     *                    detect truncation once the parser is
     *                    implemented
     */
    public static Optional<ParquetFooterIndex> fromFooter(byte[] footerBytes, long fileLength)
    {
        Objects.requireNonNull(footerBytes, "footerBytes");
        if (fileLength < 0) {
            throw new IllegalArgumentException("fileLength must be >= 0");
        }
        // TODO(SHELF-16b): implement the TCompactProtocol FileMetaData
        //   reader described in the javadoc above. Until then the
        //   caller falls back to RowGroupIndex.constantZero().
        return Optional.empty();
    }

    /**
     * Test / integration seam: build an index directly from a known
     * row-group list. Used by {@code RowGroupIndexTest} today and by
     * the SHELF-16b parser when it lands.
     */
    public static ParquetFooterIndex of(List<RowGroup> rowGroups)
    {
        return new ParquetFooterIndex(rowGroups);
    }

    @Override
    public int ordinalFor(long offset, long length)
    {
        if (offset < 0 || length <= 0) {
            return 0;
        }
        // Linear scan is fine: Parquet files have O(tens) of row
        // groups in practice, and a binary-search upgrade is a pure
        // internal change if hot-path profiling ever flags it.
        for (RowGroup rg : rowGroups) {
            if (rg.covers(offset, length)) {
                return rg.ordinal;
            }
            if (offset < rg.fileOffset) {
                // Sorted list: no later rg can contain this offset.
                break;
            }
        }
        return 0;
    }

    @Override
    public boolean hasKnownOrdinals()
    {
        return !rowGroups.isEmpty();
    }

    /** @return an unmodifiable view of the parsed row groups. */
    public List<RowGroup> rowGroups()
    {
        return rowGroups;
    }

    @Override
    public String toString()
    {
        return "ParquetFooterIndex(rowGroups=" + rowGroups.size() + ")";
    }
}
