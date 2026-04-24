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

import java.util.ArrayDeque;
import java.util.Deque;

/**
 * Minimal, dependency-free reader for Apache Thrift TCompactProtocol.
 * Scoped to exactly the subset needed to walk a Parquet
 * {@code FileMetaData} struct and pull out
 * {@code row_groups[*].(file_offset, total_compressed_size, ordinal)}
 * for {@link ParquetFooterIndex}. Explicitly out of scope: T-JSON /
 * T-Binary framing, Thrift services, and any reflection-driven Thrift
 * runtime.
 *
 * <p><b>Bounds &amp; failure contract.</b> Any read past the supplied
 * slice raises {@link ThriftParseException}. The caller
 * ({@code ParquetFooterIndex.fromFooter}) catches all runtime exceptions
 * and returns {@link java.util.Optional#empty()} — no parse error is
 * allowed to escape to Trino.
 *
 * <p><b>Thread safety.</b> Not thread-safe; the cursor is mutable and a
 * small struct-stack is maintained internally. The intended use is one
 * instance per footer parse, on the calling thread.
 *
 * <p>Layout references:
 * <ul>
 *   <li>Thrift TCompactProtocol — the field header nibble encoding,
 *       zigzag varints, and list/struct/map framing all follow the
 *       Apache Thrift compact protocol spec v1.</li>
 *   <li>Apache Parquet {@code parquet.thrift} — the canonical schema
 *       for {@code FileMetaData} and {@code RowGroup}.</li>
 * </ul>
 */
final class CompactProtocolReader
{
    /** TCompactProtocol type codes (4 bits, packed into field headers). */
    static final byte TYPE_STOP = 0;
    static final byte TYPE_BOOL_TRUE = 1;
    static final byte TYPE_BOOL_FALSE = 2;
    static final byte TYPE_BYTE = 3;
    static final byte TYPE_I16 = 4;
    static final byte TYPE_I32 = 5;
    static final byte TYPE_I64 = 6;
    static final byte TYPE_DOUBLE = 7;
    static final byte TYPE_BINARY = 8;
    static final byte TYPE_LIST = 9;
    static final byte TYPE_SET = 10;
    static final byte TYPE_MAP = 11;
    static final byte TYPE_STRUCT = 12;

    /**
     * Thrown when the input slice runs out, an encoded length is
     * negative, or an unknown type code appears. Unchecked on purpose
     * so the hot path stays lean; {@code ParquetFooterIndex} catches
     * this in its fail-open envelope.
     */
    static final class ThriftParseException
            extends RuntimeException
    {
        ThriftParseException(String message)
        {
            super(message);
        }
    }

    /** Decoded field header: {@code (fieldId, typeCode)}. */
    record FieldHeader(short id, byte type)
    {
        boolean isStop()
        {
            return type == TYPE_STOP;
        }
    }

    /** Decoded list/set header. */
    record ListHeader(byte elementType, int size) {}

    private final byte[] buffer;
    private final int end;
    private int pos;
    // Per TCompactProtocol, field ids are delta-encoded against the
    // previous id in the current struct. We reset this to 0 on
    // enterStruct() and restore it to the outer frame on exitStruct().
    private short lastFieldId;
    private final Deque<Short> structStack = new ArrayDeque<>();

    CompactProtocolReader(byte[] buffer, int offset, int length)
    {
        if (buffer == null) {
            throw new ThriftParseException("null buffer");
        }
        if (offset < 0 || length < 0) {
            throw new ThriftParseException("negative offset/length: offset=" + offset + " length=" + length);
        }
        if ((long) offset + (long) length > buffer.length) {
            throw new ThriftParseException(
                    "slice extends past buffer: offset=" + offset
                            + " length=" + length + " bufferLength=" + buffer.length);
        }
        this.buffer = buffer;
        this.pos = offset;
        this.end = offset + length;
    }

    int position()
    {
        return pos;
    }

    int bytesRemaining()
    {
        return end - pos;
    }

    private void ensureAvailable(int n)
    {
        if (n < 0) {
            throw new ThriftParseException("negative length: " + n);
        }
        // Use long math so a malicious huge length can't overflow.
        if ((long) pos + (long) n > (long) end) {
            throw new ThriftParseException(
                    "read past buffer end: need=" + n + " remaining=" + (end - pos));
        }
    }

    byte readByte()
    {
        ensureAvailable(1);
        return buffer[pos++];
    }

    /**
     * Raw unsigned varint up to 64 bits. Every byte contributes 7
     * payload bits; the top bit is the continuation flag. A varint
     * that does not terminate within 10 bytes (max for u64) is
     * rejected as malformed.
     */
    long readVarint64()
    {
        long result = 0;
        int shift = 0;
        while (true) {
            if (shift >= 64) {
                throw new ThriftParseException("varint64 overflow");
            }
            byte b = readByte();
            result |= ((long) (b & 0x7f)) << shift;
            if ((b & 0x80) == 0) {
                return result;
            }
            shift += 7;
        }
    }

    /** Raw unsigned varint up to 32 bits. */
    int readVarint32()
    {
        int result = 0;
        int shift = 0;
        while (true) {
            if (shift >= 32) {
                throw new ThriftParseException("varint32 overflow");
            }
            byte b = readByte();
            result |= (b & 0x7f) << shift;
            if ((b & 0x80) == 0) {
                return result;
            }
            shift += 7;
        }
    }

    /** Signed zigzag varint, 64-bit. */
    long readZigzag64()
    {
        long v = readVarint64();
        return (v >>> 1) ^ -(v & 1L);
    }

    /** Signed zigzag varint, 32-bit. */
    int readZigzag32()
    {
        int v = readVarint32();
        return (v >>> 1) ^ -(v & 1);
    }

    /** Raw binary payload ({@code varint32 length || length bytes}). */
    byte[] readBytes(int n)
    {
        if (n < 0) {
            throw new ThriftParseException("negative binary length: " + n);
        }
        ensureAvailable(n);
        byte[] out = new byte[n];
        System.arraycopy(buffer, pos, out, 0, n);
        pos += n;
        return out;
    }

    void skipBytes(int n)
    {
        if (n < 0) {
            throw new ThriftParseException("negative skip length: " + n);
        }
        ensureAvailable(n);
        pos += n;
    }

    /**
     * Begin reading a nested struct. Saves the outer frame's last
     * field id and resets the delta reference to 0 for the new
     * struct. Must be paired with {@link #exitStruct()}.
     */
    void enterStruct()
    {
        structStack.push(lastFieldId);
        lastFieldId = 0;
    }

    /**
     * End the current struct. Pops the outer frame's last field id so
     * subsequent reads in the parent struct resume with correct delta
     * decoding.
     */
    void exitStruct()
    {
        if (structStack.isEmpty()) {
            throw new ThriftParseException("exitStruct without enterStruct");
        }
        lastFieldId = structStack.pop();
    }

    /**
     * Read the next field header in the current struct. Callers check
     * {@link FieldHeader#isStop()} to detect the struct terminator and
     * must {@code break} the read loop when it appears.
     */
    FieldHeader readFieldHeader()
    {
        byte h = readByte();
        byte type = (byte) (h & 0x0f);
        if (type == TYPE_STOP) {
            return new FieldHeader((short) 0, TYPE_STOP);
        }
        int delta = (h >> 4) & 0x0f;
        short fieldId;
        if (delta == 0) {
            // Long-form field id: the next zigzag i16 is the absolute id.
            int z = readZigzag32();
            if (z < Short.MIN_VALUE || z > Short.MAX_VALUE) {
                throw new ThriftParseException("field id out of i16 range: " + z);
            }
            fieldId = (short) z;
        }
        else {
            fieldId = (short) (lastFieldId + delta);
        }
        lastFieldId = fieldId;
        return new FieldHeader(fieldId, type);
    }

    /**
     * Begin reading a list/set. Caller must consume exactly
     * {@code size} elements of {@code elementType} (or call
     * {@link #skipField(byte)} on each).
     */
    ListHeader enterList()
    {
        byte h = readByte();
        byte elementType = (byte) (h & 0x0f);
        int size = (h >> 4) & 0x0f;
        if (size == 15) {
            size = readVarint32();
        }
        if (size < 0) {
            throw new ThriftParseException("negative list size: " + size);
        }
        return new ListHeader(elementType, size);
    }

    /**
     * Skip a field of the given wire type. Handles all container
     * types recursively (struct, list, set, map). Used for every
     * top-level {@code FileMetaData} field the parser does not care
     * about (schema, created_by, column_orders, ...).
     */
    void skipField(byte type)
    {
        switch (type) {
            case TYPE_BOOL_TRUE, TYPE_BOOL_FALSE -> {
                // Value is packed into the field header type; no payload.
            }
            case TYPE_BYTE -> skipBytes(1);
            case TYPE_I16, TYPE_I32, TYPE_I64 -> readVarint64();
            case TYPE_DOUBLE -> skipBytes(8);
            case TYPE_BINARY -> {
                int n = readVarint32();
                skipBytes(n);
            }
            case TYPE_LIST, TYPE_SET -> skipListElements(enterList());
            case TYPE_MAP -> skipMap();
            case TYPE_STRUCT -> {
                enterStruct();
                while (true) {
                    FieldHeader fh = readFieldHeader();
                    if (fh.isStop()) {
                        break;
                    }
                    skipField(fh.type());
                }
                exitStruct();
            }
            default -> throw new ThriftParseException("unknown type for skip: " + type);
        }
    }

    private void skipListElements(ListHeader lh)
    {
        byte et = lh.elementType();
        // Per TCompactProtocol, booleans inside list/set/map are a
        // single raw byte (0 or 1), *not* encoded into the type nibble
        // like struct fields. Parquet key_value_metadata and
        // sorting_columns don't use bool lists in practice, but the
        // reader has to be correct if any upstream rev adds one.
        if (et == TYPE_BOOL_TRUE || et == TYPE_BOOL_FALSE) {
            skipBytes(lh.size());
            return;
        }
        for (int i = 0; i < lh.size(); i++) {
            skipField(et);
        }
    }

    private void skipMap()
    {
        int size = readVarint32();
        if (size < 0) {
            throw new ThriftParseException("negative map size: " + size);
        }
        if (size == 0) {
            return;
        }
        byte kv = readByte();
        byte keyType = (byte) ((kv >> 4) & 0x0f);
        byte valType = (byte) (kv & 0x0f);
        for (int i = 0; i < size; i++) {
            skipField(keyType);
            skipField(valType);
        }
    }
}
