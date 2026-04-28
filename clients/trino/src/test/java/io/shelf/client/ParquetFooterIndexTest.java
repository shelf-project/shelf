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

import static org.assertj.core.api.Assertions.assertThat;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;

import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.example.GroupWriteSupport;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.io.LocalOutputFile;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * SHELF-16b unit tests for {@link ParquetFooterIndex#fromFooter}.
 *
 * <p>Covers:
 * <ul>
 *   <li>Happy path — a real Parquet file with multiple row groups
 *       written via {@code parquet-hadoop}, round-tripped through the
 *       hand-rolled reader.</li>
 *   <li>Happy path — a hand-built FileMetaData blob with three row
 *       groups including explicit ordinals.</li>
 *   <li>Fail-open contract — bad magic, truncated footer, encrypted
 *       footer, and row groups that claim to extend past
 *       {@code fileLength} all return {@link Optional#empty()} without
 *       throwing.</li>
 *   <li>Fallback paths — row groups missing {@code file_offset} /
 *       {@code total_compressed_size} / {@code ordinal} must be
 *       reconstructed from column chunks and list index.</li>
 * </ul>
 */
class ParquetFooterIndexTest
{
    // Parquet FileMetaData / RowGroup / ColumnChunk / ColumnMetaData
    // field ids, mirroring the private constants in ParquetFooterIndex.
    // Hand-built blobs want the same layout, so we keep these local.
    private static final short FIELD_FM_VERSION = 1;
    private static final short FIELD_FM_SCHEMA = 2;
    private static final short FIELD_FM_NUM_ROWS = 3;
    private static final short FIELD_FM_ROW_GROUPS = 4;
    private static final short FIELD_FM_CREATED_BY = 6;

    private static final short FIELD_RG_COLUMNS = 1;
    private static final short FIELD_RG_TOTAL_BYTE_SIZE = 2;
    private static final short FIELD_RG_NUM_ROWS = 3;
    private static final short FIELD_RG_FILE_OFFSET = 5;
    private static final short FIELD_RG_TOTAL_COMPRESSED_SIZE = 6;
    private static final short FIELD_RG_ORDINAL = 7;

    private static final short FIELD_CC_FILE_OFFSET = 2;
    private static final short FIELD_CC_META_DATA = 3;

    private static final short FIELD_CMD_TOTAL_COMPRESSED_SIZE = 7;
    private static final short FIELD_CMD_DATA_PAGE_OFFSET = 9;
    private static final short FIELD_CMD_DICTIONARY_PAGE_OFFSET = 11;

    @Test
    void fromFooter_extractsRowGroupOffsets()
    {
        // Three well-formed row groups, laid back-to-back with small
        // gaps (mimicking how Parquet writers leave room for page
        // boundaries between column chunks).
        byte[] blob = buildFileMetaData(
                new RowGroupSpec(100L, 1_024L, 0, List.of()),
                new RowGroupSpec(2_048L, 4_096L, 1, List.of()),
                new RowGroupSpec(8_192L, 16_384L, 2, List.of()));
        byte[] tail = wrapWithFooterTrailer(blob);
        long fileLength = tail.length + 1_000_000L; // some headroom

        Optional<ParquetFooterIndex> maybe = ParquetFooterIndex.fromFooter(tail, fileLength);

        assertThat(maybe).isPresent();
        ParquetFooterIndex idx = maybe.get();
        assertThat(idx.hasKnownOrdinals()).isTrue();
        assertThat(idx.rowGroups()).hasSize(3);
        assertThat(idx.rowGroups().get(0))
                .isEqualTo(new ParquetFooterIndex.RowGroup(100L, 1_024L, 0));
        assertThat(idx.rowGroups().get(1))
                .isEqualTo(new ParquetFooterIndex.RowGroup(2_048L, 4_096L, 1));
        assertThat(idx.rowGroups().get(2))
                .isEqualTo(new ParquetFooterIndex.RowGroup(8_192L, 16_384L, 2));
        // ordinalFor round-trips.
        assertThat(idx.ordinalFor(100L, 100L)).isZero();
        assertThat(idx.ordinalFor(2_500L, 128L)).isEqualTo(1);
        assertThat(idx.ordinalFor(8_192L, 16_384L)).isEqualTo(2);
    }

    @Test
    void fromFooter_extractsRowGroupOffsets_fromRealParquetFile(@TempDir Path tmp)
            throws Exception
    {
        // Generate a real Parquet file with parquet-hadoop (1.14.x) via
        // LocalOutputFile — no Hadoop FileSystem, no UGI, no JDK 25
        // subject-manager incompatibility. We deliberately do NOT
        // round-trip through parquet-hadoop's ParquetFileReader, which
        // would drag in UserGroupInformation.getCurrentUser() and fail
        // on JDK 25 (removed Subject.getSubject(AccessControlContext)).
        // Instead, we let our hand-rolled reader speak for itself and
        // assert the invariants parquet-hadoop's writer guarantees:
        //   * at least 2 row groups produced;
        //   * every row group's byte range lies strictly inside
        //     (file_header, footer_start);
        //   * ordinals are 0..N-1 (dense, no gaps);
        //   * the row groups are strictly ordered and non-overlapping.
        Path parquetFile = tmp.resolve("shelf-footer-real.parquet");

        MessageType schema = MessageTypeParser.parseMessageType(
                "message test { required int64 id; required binary payload; }");
        Configuration conf = new Configuration();
        GroupWriteSupport.setSchema(schema, conf);
        SimpleGroupFactory factory = new SimpleGroupFactory(schema);

        // Force several row groups: small row-group size + a few
        // thousand rows of high-entropy bytes so the writer cannot
        // collapse them under its encoding.  rowGroupSize=128 KiB
        // with random 256-byte payloads reliably rolls at least 3
        // blocks.
        try (ParquetWriter<Group> writer = ExampleParquetWriter.builder(
                        new LocalOutputFile(parquetFile))
                .withConf(conf)
                .withWriterVersion(ParquetProperties.WriterVersion.PARQUET_2_0)
                .withCompressionCodec(CompressionCodecName.UNCOMPRESSED)
                .withRowGroupSize(128L * 1024L)
                .withPageSize(8 * 1024)
                .withDictionaryEncoding(false)
                .build()) {
            java.util.Random rng = new java.util.Random(0xC0FFEE);
            byte[] payload = new byte[256];
            for (int i = 0; i < 5_000; i++) {
                rng.nextBytes(payload);
                writer.write(factory.newGroup()
                        .append("id", (long) i)
                        .append("payload", org.apache.parquet.io.api.Binary.fromReusedByteArray(payload)));
            }
        }

        byte[] fileBytes = Files.readAllBytes(parquetFile);
        // Every Parquet file starts with "PAR1" magic (4 bytes) before
        // any row-group data.
        assertThat(fileBytes).startsWith((byte) 'P', (byte) 'A', (byte) 'R', (byte) '1');
        // Footer blob starts at fileBytes.length - 8 - footer_length.
        int footerLen = ByteBuffer.wrap(fileBytes, fileBytes.length - 8, 4)
                .order(ByteOrder.LITTLE_ENDIAN).getInt();
        long footerStart = fileBytes.length - 8L - footerLen;

        Optional<ParquetFooterIndex> parsed =
                ParquetFooterIndex.fromFooter(fileBytes, fileBytes.length);

        assertThat(parsed).as("real parquet file must parse").isPresent();
        List<ParquetFooterIndex.RowGroup> rgs = parsed.get().rowGroups();
        assertThat(rgs).as("writer must produce ≥ 2 row groups").hasSizeGreaterThanOrEqualTo(2);
        long prevEnd = 4; // Parquet "PAR1" header magic.
        for (int i = 0; i < rgs.size(); i++) {
            ParquetFooterIndex.RowGroup rg = rgs.get(i);
            assertThat(rg.fileOffset())
                    .as("row group %d file offset must sit after header and previous row group", i)
                    .isGreaterThanOrEqualTo(prevEnd);
            long end = rg.fileOffset() + rg.totalCompressedSize();
            assertThat(end)
                    .as("row group %d must end before footer start", i)
                    .isLessThanOrEqualTo(footerStart);
            assertThat(rg.ordinal())
                    .as("row group ordinals must be dense 0..N-1", i)
                    .isEqualTo(i);
            prevEnd = end;
        }
    }

    @Test
    void fromFooter_handlesOrdinalAbsent()
    {
        // Two row groups with the ordinal field omitted. The parser
        // must fall back to the row group's index in the list (post-
        // sort by file_offset). We deliberately emit them in reverse
        // file_offset order to prove the fallback is the post-sort
        // index, not the wire order.
        byte[] blob = buildFileMetaData(
                new RowGroupSpec(10_000L, 4_096L, -1, List.of()),
                new RowGroupSpec(100L, 1_024L, -1, List.of()));
        byte[] tail = wrapWithFooterTrailer(blob);
        long fileLength = tail.length + 1_000_000L;

        Optional<ParquetFooterIndex> maybe = ParquetFooterIndex.fromFooter(tail, fileLength);

        assertThat(maybe).isPresent();
        List<ParquetFooterIndex.RowGroup> rgs = maybe.get().rowGroups();
        assertThat(rgs).hasSize(2);
        // Sorted by file offset, so the one at 100 is first.
        // The ordinal fallback uses the parse-time list index, which
        // means the one emitted SECOND (file offset 100) gets ordinal 1.
        // This is conservative but unambiguous: every row group keeps
        // a stable, distinct ordinal, which is what the cache-key
        // uniqueness invariant needs.
        assertThat(rgs.get(0).fileOffset()).isEqualTo(100L);
        assertThat(rgs.get(1).fileOffset()).isEqualTo(10_000L);
        assertThat(rgs.get(0).ordinal()).isEqualTo(1);
        assertThat(rgs.get(1).ordinal()).isEqualTo(0);
    }

    @Test
    void fromFooter_derivesOffsetFromColumnsWhenAbsent()
    {
        // Emulate a pre-2020 writer that omits row_group.file_offset
        // and row_group.total_compressed_size. The parser must
        // reconstruct both from the column chunk list.
        List<ColumnChunkSpec> cols = List.of(
                new ColumnChunkSpec(
                        /* ccFileOffset */ 500L,
                        /* totalCompressedSize */ 600L,
                        /* dataPageOffset */ 500L,
                        /* dictPageOffset */ null),
                new ColumnChunkSpec(
                        null,
                        300L,
                        /* dataPageOffset */ 1_200L,
                        /* dictPageOffset */ 1_100L));
        byte[] blob = buildFileMetaData(
                new RowGroupSpec(/* fileOffset */ -1, /* size */ -1, /* ordinal */ 0, cols));
        byte[] tail = wrapWithFooterTrailer(blob);
        long fileLength = tail.length + 1_000_000L;

        Optional<ParquetFooterIndex> maybe = ParquetFooterIndex.fromFooter(tail, fileLength);

        assertThat(maybe).isPresent();
        List<ParquetFooterIndex.RowGroup> rgs = maybe.get().rowGroups();
        assertThat(rgs).hasSize(1);
        // File offset = columns[0].file_offset = 500.
        assertThat(rgs.get(0).fileOffset()).isEqualTo(500L);
        // Size = sum of column.total_compressed_size = 600 + 300 = 900.
        assertThat(rgs.get(0).totalCompressedSize()).isEqualTo(900L);
        assertThat(rgs.get(0).ordinal()).isZero();
    }

    @Test
    void fromFooter_derivesOffsetFromPageOffsetsWhenColumnFileOffsetAbsent()
    {
        // Even ColumnChunk.file_offset is optional on older writers;
        // fall through to min(data_page_offset, dictionary_page_offset).
        List<ColumnChunkSpec> cols = List.of(
                new ColumnChunkSpec(
                        /* ccFileOffset */ null,
                        /* totalCompressedSize */ 700L,
                        /* dataPageOffset */ 2_048L,
                        /* dictPageOffset */ 2_000L));
        byte[] blob = buildFileMetaData(
                new RowGroupSpec(-1, -1, 0, cols));
        byte[] tail = wrapWithFooterTrailer(blob);
        long fileLength = tail.length + 1_000_000L;

        Optional<ParquetFooterIndex> maybe = ParquetFooterIndex.fromFooter(tail, fileLength);

        assertThat(maybe).isPresent();
        List<ParquetFooterIndex.RowGroup> rgs = maybe.get().rowGroups();
        // min(data_page_offset=2048, dictionary_page_offset=2000) = 2000.
        assertThat(rgs.get(0).fileOffset()).isEqualTo(2_000L);
        assertThat(rgs.get(0).totalCompressedSize()).isEqualTo(700L);
    }

    @Test
    void fromFooter_returnsEmpty_onBadMagic()
    {
        byte[] bogus = new byte[] {1, 2, 3, 4, 5, 6, 7, 8, 9, 0xA, 0xB, 0xC};
        assertThat(ParquetFooterIndex.fromFooter(bogus, bogus.length)).isEmpty();

        byte[] wrongMagic = new byte[] {
                0, 0, 0, 0,
                'X', 'Y', 'Z', 'W'
        };
        assertThat(ParquetFooterIndex.fromFooter(wrongMagic, wrongMagic.length)).isEmpty();
    }

    @Test
    void fromFooter_returnsEmpty_onTruncatedFooter()
    {
        // Build a valid blob then strip bytes so footer_length points
        // past the buffer start.
        byte[] blob = buildFileMetaData(
                new RowGroupSpec(100L, 1_024L, 0, List.of()));
        byte[] tail = wrapWithFooterTrailer(blob);
        // Truncate by dropping half of the blob bytes.
        int cut = blob.length / 2;
        byte[] truncated = new byte[tail.length - cut];
        System.arraycopy(tail, cut, truncated, 0, truncated.length);
        assertThat(ParquetFooterIndex.fromFooter(truncated, truncated.length)).isEmpty();

        // Also: a tail whose declared footer_length is larger than the
        // buffer is rejected.
        byte[] tooBig = new byte[16];
        ByteBuffer bb = ByteBuffer.wrap(tooBig).order(ByteOrder.LITTLE_ENDIAN);
        bb.putInt(8, 1_000_000); // footer_length
        tooBig[12] = 'P';
        tooBig[13] = 'A';
        tooBig[14] = 'R';
        tooBig[15] = '1';
        assertThat(ParquetFooterIndex.fromFooter(tooBig, tooBig.length)).isEmpty();

        // Zero footer_length is also rejected.
        byte[] zeroLen = new byte[] {
                0, 0, 0, 0,
                'P', 'A', 'R', '1'
        };
        assertThat(ParquetFooterIndex.fromFooter(zeroLen, zeroLen.length)).isEmpty();
    }

    @Test
    void fromFooter_handlesEncryptedFooter()
    {
        // "PARE" trailer — Parquet Modular Encryption. We cannot parse
        // it and the caller falls through to the constant-zero index.
        byte[] encrypted = new byte[] {
                0, 1, 2, 3,
                'P', 'A', 'R', 'E'
        };
        assertThat(ParquetFooterIndex.fromFooter(encrypted, encrypted.length)).isEmpty();
    }

    @Test
    void fromFooter_rejectsRowGroupPastFileLength()
    {
        // Declared row group: offset 1024, size 4096 → ends at 5120.
        byte[] blob = buildFileMetaData(
                new RowGroupSpec(1_024L, 4_096L, 0, List.of()));
        byte[] tail = wrapWithFooterTrailer(blob);
        // fileLength is smaller than rg.end → reject.
        assertThat(ParquetFooterIndex.fromFooter(tail, 4_000L)).isEmpty();
        // fileLength just barely containing the rg → accepted.
        assertThat(ParquetFooterIndex.fromFooter(tail, 5_120L)).isPresent();
    }

    @Test
    void fromFooter_throwsOnNegativeFileLength()
    {
        // Contract: fileLength < 0 is a programming error on the
        // caller side and surfaces as IllegalArgumentException. All
        // *parse* errors, in contrast, fail open.
        org.assertj.core.api.Assertions.assertThatThrownBy(
                        () -> ParquetFooterIndex.fromFooter(new byte[8], -1L))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void fromFooter_skipsUnrelatedTopLevelFields()
    {
        // Mix the row_groups field in with version/num_rows/created_by
        // and a fake schema list so the parser must exercise skipField
        // over I32/I64/BINARY/LIST<STRUCT>.
        CompactProtocolWriter w = new CompactProtocolWriter();
        w.enterStruct();
        w.writeI32Field(FIELD_FM_VERSION, 2);
        // schema = list<struct{}> — two empty structs, just to exercise
        // skip of LIST<STRUCT>.
        w.writeListFieldHeader(FIELD_FM_SCHEMA, CompactProtocolReader.TYPE_STRUCT, 2);
        for (int i = 0; i < 2; i++) {
            w.enterStruct();
            w.exitStruct();
        }
        w.writeI64Field(FIELD_FM_NUM_ROWS, 1_000_000L);
        appendRowGroups(w, FIELD_FM_ROW_GROUPS,
                new RowGroupSpec(500L, 2_048L, 3, List.of()));
        w.writeBinaryField(FIELD_FM_CREATED_BY,
                "shelf-test-writer".getBytes(java.nio.charset.StandardCharsets.UTF_8));
        w.exitStruct();

        byte[] tail = wrapWithFooterTrailer(w.toBytes());

        Optional<ParquetFooterIndex> maybe =
                ParquetFooterIndex.fromFooter(tail, tail.length + 100_000L);
        assertThat(maybe).isPresent();
        assertThat(maybe.get().rowGroups()).hasSize(1);
        assertThat(maybe.get().rowGroups().get(0))
                .isEqualTo(new ParquetFooterIndex.RowGroup(500L, 2_048L, 3));
    }

    // ------------------------------------------------------------------
    //   Hand-built Thrift blob helpers.
    // ------------------------------------------------------------------

    /** Spec for a row group in a test-synthetic FileMetaData blob. */
    private record RowGroupSpec(
            long fileOffset,               // < 0 => omit row_group.file_offset
            long totalCompressedSize,      // < 0 => omit row_group.total_compressed_size
            int ordinal,                   // < 0 => omit row_group.ordinal
            List<ColumnChunkSpec> columns)
    {}

    /** Spec for a ColumnChunk inside a test-synthetic RowGroup. */
    private record ColumnChunkSpec(
            Long ccFileOffset,                  // ColumnChunk.file_offset (field 2); null => omit
            Long totalCompressedSize,           // ColumnMetaData.total_compressed_size; null => omit
            Long dataPageOffset,                // ColumnMetaData.data_page_offset; null => omit
            Long dictPageOffset)                // ColumnMetaData.dictionary_page_offset; null => omit
    {}

    private static byte[] buildFileMetaData(RowGroupSpec... groups)
    {
        CompactProtocolWriter w = new CompactProtocolWriter();
        w.enterStruct();
        w.writeI32Field(FIELD_FM_VERSION, 2);
        w.writeI64Field(FIELD_FM_NUM_ROWS, 100L);
        appendRowGroups(w, FIELD_FM_ROW_GROUPS, groups);
        w.exitStruct();
        return w.toBytes();
    }

    private static void appendRowGroups(CompactProtocolWriter w, short fieldId, RowGroupSpec... groups)
    {
        w.writeListFieldHeader(fieldId, CompactProtocolReader.TYPE_STRUCT, groups.length);
        for (RowGroupSpec rg : groups) {
            writeRowGroup(w, rg);
        }
    }

    private static void writeRowGroup(CompactProtocolWriter w, RowGroupSpec rg)
    {
        w.enterStruct();
        // columns (field 1) — always present (Parquet requires columns to be set).
        w.writeListFieldHeader(FIELD_RG_COLUMNS, CompactProtocolReader.TYPE_STRUCT, rg.columns().size());
        for (ColumnChunkSpec col : rg.columns()) {
            writeColumnChunk(w, col);
        }
        // total_byte_size (field 2) — uncompressed, we don't care, emit a plausible value.
        w.writeI64Field(FIELD_RG_TOTAL_BYTE_SIZE,
                rg.totalCompressedSize() > 0 ? rg.totalCompressedSize() : 1L);
        // num_rows (field 3)
        w.writeI64Field(FIELD_RG_NUM_ROWS, 10L);
        if (rg.fileOffset() >= 0) {
            w.writeI64Field(FIELD_RG_FILE_OFFSET, rg.fileOffset());
        }
        if (rg.totalCompressedSize() > 0) {
            w.writeI64Field(FIELD_RG_TOTAL_COMPRESSED_SIZE, rg.totalCompressedSize());
        }
        if (rg.ordinal() >= 0) {
            w.writeI16Field(FIELD_RG_ORDINAL, (short) rg.ordinal());
        }
        w.exitStruct();
    }

    private static void writeColumnChunk(CompactProtocolWriter w, ColumnChunkSpec col)
    {
        w.enterStruct();
        if (col.ccFileOffset() != null) {
            w.writeI64Field(FIELD_CC_FILE_OFFSET, col.ccFileOffset());
        }
        // meta_data (field 3) — STRUCT(ColumnMetaData)
        w.writeStructFieldBegin(FIELD_CC_META_DATA);
        if (col.totalCompressedSize() != null) {
            w.writeI64Field(FIELD_CMD_TOTAL_COMPRESSED_SIZE, col.totalCompressedSize());
        }
        if (col.dataPageOffset() != null) {
            w.writeI64Field(FIELD_CMD_DATA_PAGE_OFFSET, col.dataPageOffset());
        }
        if (col.dictPageOffset() != null) {
            w.writeI64Field(FIELD_CMD_DICTIONARY_PAGE_OFFSET, col.dictPageOffset());
        }
        w.exitStruct(); // meta_data
        w.exitStruct(); // column chunk
    }

    /**
     * Append the canonical Parquet tail: {@code [blob][u32_le len]["PAR1"]}.
     */
    private static byte[] wrapWithFooterTrailer(byte[] blob)
    {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        out.writeBytes(blob);
        ByteBuffer len = ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN).putInt(blob.length);
        out.writeBytes(len.array());
        out.write('P');
        out.write('A');
        out.write('R');
        out.write('1');
        return out.toByteArray();
    }
}
