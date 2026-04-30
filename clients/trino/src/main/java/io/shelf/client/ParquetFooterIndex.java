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
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * {@link RowGroupIndex} backed by the Parquet file's footer
 * {@code FileMetaData}. Given the tail bytes of a Parquet object, this
 * class extracts each row group's
 * {@code (file_offset, total_compressed_size, ordinal)} tuple and
 * answers {@link #ordinalFor(long, long)} with the ordinal of the row
 * group whose byte range contains the query.
 *
 * <p><b>SHELF-16b delivery level: IMPLEMENTED.</b> The earlier
 * SHELF-16a ticket shipped the scaffold (interface + range-to-ordinal
 * lookup + wiring through {@link io.shelf.filesystem.ShelfInputStream})
 * and deferred the TCompactProtocol parser to this follow-up. The
 * parser lives in {@link CompactProtocolReader} (no external Thrift
 * runtime — the reader understands only the subset of the Thrift
 * compact protocol that Parquet footers actually use).
 *
 * <p><b>Thread safety.</b> Immutable; the internal row-group list is
 * an unmodifiable snapshot. Safe to share across Trino threads.
 *
 * <p><b>Invariants enforced at construction.</b>
 * <ul>
 *   <li>Row-group list is sorted by {@code fileOffset}.</li>
 *   <li>Row-group ranges are non-overlapping (Parquet spec requires
 *       this; we assert rather than merge).</li>
 *   <li>{@code ordinal} values are non-negative; Parquet writers that
 *       omit the field get the list index (post-sort by offset) as
 *       their ordinal.</li>
 * </ul>
 */
public final class ParquetFooterIndex
        implements RowGroupIndex
{
    private static final Logger LOGGER = Logger.getLogger(ParquetFooterIndex.class.getName());

    // Parquet FileMetaData field ids (see parquet.thrift).
    private static final short FIELD_FM_ROW_GROUPS = 4;

    // Parquet RowGroup field ids.
    private static final short FIELD_RG_COLUMNS = 1;
    private static final short FIELD_RG_FILE_OFFSET = 5;
    private static final short FIELD_RG_TOTAL_COMPRESSED_SIZE = 6;
    private static final short FIELD_RG_ORDINAL = 7;

    // Parquet ColumnChunk field ids.
    private static final short FIELD_CC_FILE_OFFSET = 2;
    private static final short FIELD_CC_META_DATA = 3;

    // Parquet ColumnMetaData field ids.
    private static final short FIELD_CMD_TOTAL_COMPRESSED_SIZE = 7;
    private static final short FIELD_CMD_DATA_PAGE_OFFSET = 9;
    private static final short FIELD_CMD_DICTIONARY_PAGE_OFFSET = 11;

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
        // a later bisect-tree upgrade stays a drop-in replacement.
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
     * Parse the tail of a Parquet object into a row-group index.
     *
     * <p>The tail must end with {@code "PAR1"} (unencrypted) — the
     * {@code "PARE"} (encrypted footer) case is detected and returns
     * {@link Optional#empty()} because we cannot decrypt. The last 8
     * bytes of the tail are laid out as
     * {@code [footer_length:u32_le][magic:4B]}; the Thrift-encoded
     * {@code FileMetaData} occupies the {@code footer_length} bytes
     * immediately before that trailer.
     *
     * <p>The parser walks the {@code FileMetaData} struct, descends
     * only into {@code row_groups[*]} (field id 4, LIST&lt;STRUCT&gt;),
     * and extracts per-row-group:
     * <ul>
     *   <li>{@code file_offset} (field 5), falling back to the first
     *       column chunk's {@code file_offset}, then to the minimum
     *       of its {@code data_page_offset} and
     *       {@code dictionary_page_offset}. Older Parquet writers
     *       (pre-2020) omit {@code row_group.file_offset}.</li>
     *   <li>{@code total_compressed_size} (field 6), falling back to
     *       the sum of {@code columns[*].meta_data.total_compressed_size}.</li>
     *   <li>{@code ordinal} (field 7), falling back to the index of
     *       the row group in the parsed list.</li>
     * </ul>
     *
     * <p><b>Fail-open contract.</b> Any parse error — malformed
     * magic, truncated blob, unexpected Thrift type, varint overflow,
     * arithmetic overflow, row-group overlap, or a row group whose
     * byte range extends past {@code fileLength} — collapses to
     * {@link Optional#empty()}. The caller ({@code ShelfInputFile})
     * already has {@link RowGroupIndex#constantZero()} as a safe
     * fallback and must never see a Parquet-parser exception.
     *
     * @param footerBytes the trailing bytes of the object (at minimum
     *                    8 bytes, and large enough to cover the
     *                    Thrift footer blob)
     * @param fileLength  total S3 object length; used to reject
     *                    footers whose row groups claim to extend
     *                    past the object
     */
    public static Optional<ParquetFooterIndex> fromFooter(byte[] footerBytes, long fileLength)
    {
        Objects.requireNonNull(footerBytes, "footerBytes");
        if (fileLength < 0) {
            throw new IllegalArgumentException("fileLength must be >= 0");
        }
        try {
            int tailLen = footerBytes.length;
            // 4 bytes footer_length + 4 bytes magic = 8 bytes minimum.
            if (tailLen < 8) {
                return Optional.empty();
            }
            byte m0 = footerBytes[tailLen - 4];
            byte m1 = footerBytes[tailLen - 3];
            byte m2 = footerBytes[tailLen - 2];
            byte m3 = footerBytes[tailLen - 1];
            if (m0 != (byte) 'P' || m1 != (byte) 'A' || m2 != (byte) 'R') {
                return Optional.empty();
            }
            // "PARE" = encrypted footer (Parquet Modular Encryption).
            // We cannot parse it, so the caller falls through to the
            // constant-zero fallback. This is an expected, silent
            // branch — not a parse failure.
            if (m3 == (byte) 'E') {
                return Optional.empty();
            }
            if (m3 != (byte) '1') {
                return Optional.empty();
            }

            int footerLen = readU32Le(footerBytes, tailLen - 8);
            if (footerLen <= 0 || footerLen > tailLen - 8) {
                return Optional.empty();
            }
            int blobStart = tailLen - 8 - footerLen;

            CompactProtocolReader reader = new CompactProtocolReader(footerBytes, blobStart, footerLen);
            List<RowGroup> rowGroups = parseFileMetaData(reader);

            // Sanity: every row group must fit inside the declared file length.
            for (RowGroup rg : rowGroups) {
                long end = Math.addExact(rg.fileOffset(), rg.totalCompressedSize());
                if (end > fileLength) {
                    return Optional.empty();
                }
            }

            return Optional.of(new ParquetFooterIndex(rowGroups));
        }
        catch (RuntimeException e) {
            // Blanket catch is load-bearing. Every failure shape the
            // parser can produce — ThriftParseException, AIOOBE from
            // a buggy header, ArithmeticException from addExact,
            // IllegalArgumentException from the RowGroup or
            // ParquetFooterIndex constructor bounds checks — collapses
            // here to Optional.empty(). The caller's
            // constant-zero fallback is the single escape hatch and
            // must never see a plugin-level exception.
            LOGGER.log(Level.FINE, "Parquet footer parse failed; falling back to constant-zero index", e);
            return Optional.empty();
        }
    }

    private static int readU32Le(byte[] b, int off)
    {
        return (b[off] & 0xff)
                | ((b[off + 1] & 0xff) << 8)
                | ((b[off + 2] & 0xff) << 16)
                | ((b[off + 3] & 0xff) << 24);
    }

    private static List<RowGroup> parseFileMetaData(CompactProtocolReader reader)
    {
        reader.enterStruct();
        List<RowGroup> result = new ArrayList<>();
        boolean sawRowGroups = false;
        while (true) {
            CompactProtocolReader.FieldHeader fh = reader.readFieldHeader();
            if (fh.isStop()) {
                break;
            }
            if (fh.id() == FIELD_FM_ROW_GROUPS && fh.type() == CompactProtocolReader.TYPE_LIST) {
                sawRowGroups = true;
                CompactProtocolReader.ListHeader lh = reader.enterList();
                if (lh.size() > 0 && lh.elementType() != CompactProtocolReader.TYPE_STRUCT) {
                    throw new CompactProtocolReader.ThriftParseException(
                            "row_groups must be LIST<STRUCT>, got element type " + lh.elementType());
                }
                for (int i = 0; i < lh.size(); i++) {
                    result.add(parseRowGroup(reader, i));
                }
            }
            else {
                reader.skipField(fh.type());
            }
        }
        reader.exitStruct();
        if (!sawRowGroups) {
            throw new CompactProtocolReader.ThriftParseException("FileMetaData missing row_groups field");
        }
        return result;
    }

    private static RowGroup parseRowGroup(CompactProtocolReader reader, int listIndex)
    {
        reader.enterStruct();
        List<ColumnInfo> columns = new ArrayList<>();
        Long fileOffset = null;
        Long totalCompressedSize = null;
        Integer ordinal = null;
        while (true) {
            CompactProtocolReader.FieldHeader fh = reader.readFieldHeader();
            if (fh.isStop()) {
                break;
            }
            switch (fh.id()) {
                case FIELD_RG_COLUMNS -> {
                    if (fh.type() != CompactProtocolReader.TYPE_LIST) {
                        reader.skipField(fh.type());
                        break;
                    }
                    CompactProtocolReader.ListHeader lh = reader.enterList();
                    if (lh.size() > 0 && lh.elementType() != CompactProtocolReader.TYPE_STRUCT) {
                        throw new CompactProtocolReader.ThriftParseException(
                                "columns must be LIST<STRUCT>, got element type " + lh.elementType());
                    }
                    for (int i = 0; i < lh.size(); i++) {
                        columns.add(parseColumnChunk(reader));
                    }
                }
                case FIELD_RG_FILE_OFFSET -> {
                    if (fh.type() == CompactProtocolReader.TYPE_I64) {
                        fileOffset = reader.readZigzag64();
                    }
                    else {
                        reader.skipField(fh.type());
                    }
                }
                case FIELD_RG_TOTAL_COMPRESSED_SIZE -> {
                    if (fh.type() == CompactProtocolReader.TYPE_I64) {
                        totalCompressedSize = reader.readZigzag64();
                    }
                    else {
                        reader.skipField(fh.type());
                    }
                }
                case FIELD_RG_ORDINAL -> {
                    // Ordinal is spec'd as i16 but be liberal if a
                    // writer promotes it to i32. Anything larger than
                    // Integer.MAX_VALUE is nonsense and falls through
                    // to the list-index fallback.
                    if (fh.type() == CompactProtocolReader.TYPE_I16
                            || fh.type() == CompactProtocolReader.TYPE_I32) {
                        long v = reader.readZigzag64();
                        if (v >= 0 && v <= Integer.MAX_VALUE) {
                            ordinal = (int) v;
                        }
                    }
                    else {
                        reader.skipField(fh.type());
                    }
                }
                default -> reader.skipField(fh.type());
            }
        }
        reader.exitStruct();

        long rgFileOffset;
        if (fileOffset != null && fileOffset >= 0) {
            rgFileOffset = fileOffset;
        }
        else if (!columns.isEmpty()) {
            rgFileOffset = columns.get(0).effectiveFileOffset();
        }
        else {
            throw new CompactProtocolReader.ThriftParseException(
                    "row group has no file_offset and no columns");
        }

        long rgSize;
        if (totalCompressedSize != null && totalCompressedSize > 0) {
            rgSize = totalCompressedSize;
        }
        else {
            long sum = 0;
            boolean any = false;
            for (ColumnInfo c : columns) {
                if (c.totalCompressedSize != null && c.totalCompressedSize > 0) {
                    sum = Math.addExact(sum, c.totalCompressedSize);
                    any = true;
                }
            }
            if (!any || sum <= 0) {
                throw new CompactProtocolReader.ThriftParseException(
                        "row group has no total_compressed_size and column sizes unavailable");
            }
            rgSize = sum;
        }

        int rgOrdinal = (ordinal != null && ordinal >= 0) ? ordinal : listIndex;
        return new RowGroup(rgFileOffset, rgSize, rgOrdinal);
    }

    private static ColumnInfo parseColumnChunk(CompactProtocolReader reader)
    {
        reader.enterStruct();
        ColumnInfo info = new ColumnInfo();
        while (true) {
            CompactProtocolReader.FieldHeader fh = reader.readFieldHeader();
            if (fh.isStop()) {
                break;
            }
            switch (fh.id()) {
                case FIELD_CC_FILE_OFFSET -> {
                    if (fh.type() == CompactProtocolReader.TYPE_I64) {
                        info.fileOffset = reader.readZigzag64();
                    }
                    else {
                        reader.skipField(fh.type());
                    }
                }
                case FIELD_CC_META_DATA -> {
                    if (fh.type() == CompactProtocolReader.TYPE_STRUCT) {
                        parseColumnMetaData(reader, info);
                    }
                    else {
                        reader.skipField(fh.type());
                    }
                }
                default -> reader.skipField(fh.type());
            }
        }
        reader.exitStruct();
        return info;
    }

    private static void parseColumnMetaData(CompactProtocolReader reader, ColumnInfo info)
    {
        reader.enterStruct();
        while (true) {
            CompactProtocolReader.FieldHeader fh = reader.readFieldHeader();
            if (fh.isStop()) {
                break;
            }
            switch (fh.id()) {
                case FIELD_CMD_TOTAL_COMPRESSED_SIZE -> {
                    if (fh.type() == CompactProtocolReader.TYPE_I64) {
                        info.totalCompressedSize = reader.readZigzag64();
                    }
                    else {
                        reader.skipField(fh.type());
                    }
                }
                case FIELD_CMD_DATA_PAGE_OFFSET -> {
                    if (fh.type() == CompactProtocolReader.TYPE_I64) {
                        info.dataPageOffset = reader.readZigzag64();
                    }
                    else {
                        reader.skipField(fh.type());
                    }
                }
                case FIELD_CMD_DICTIONARY_PAGE_OFFSET -> {
                    if (fh.type() == CompactProtocolReader.TYPE_I64) {
                        info.dictPageOffset = reader.readZigzag64();
                    }
                    else {
                        reader.skipField(fh.type());
                    }
                }
                default -> reader.skipField(fh.type());
            }
        }
        reader.exitStruct();
    }

    private static final class ColumnInfo
    {
        Long fileOffset;
        Long totalCompressedSize;
        Long dataPageOffset;
        Long dictPageOffset;

        long effectiveFileOffset()
        {
            if (fileOffset != null && fileOffset >= 0) {
                return fileOffset;
            }
            Long min = null;
            if (dataPageOffset != null && dataPageOffset >= 0) {
                min = dataPageOffset;
            }
            if (dictPageOffset != null && dictPageOffset >= 0
                    && (min == null || dictPageOffset < min)) {
                min = dictPageOffset;
            }
            if (min == null) {
                throw new CompactProtocolReader.ThriftParseException(
                        "column has no file_offset and no page offsets");
            }
            return min;
        }
    }

    /**
     * Test / integration seam: build an index directly from a known
     * row-group list. Used by {@code RowGroupIndexTest} and by the
     * SHELF-16b footer parser when it constructs the result.
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
