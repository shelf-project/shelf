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

import java.io.ByteArrayOutputStream;
import java.util.ArrayDeque;
import java.util.Deque;

/**
 * Minimal TCompactProtocol writer used by {@link ParquetFooterIndexTest}
 * to hand-build FileMetaData blobs. Mirrors the subset of the
 * compact-protocol spec that {@link CompactProtocolReader} understands.
 * Not a production component — {@code src/test/java} only.
 */
final class CompactProtocolWriter
{
    private final ByteArrayOutputStream out = new ByteArrayOutputStream();
    private short lastFieldId;
    private final Deque<Short> stack = new ArrayDeque<>();

    void writeFieldBegin(byte type, short id)
    {
        int delta = id - lastFieldId;
        if (delta > 0 && delta <= 15) {
            out.write((delta << 4) | (type & 0x0f));
        }
        else {
            out.write(type & 0x0f);
            writeZigzag32(id);
        }
        lastFieldId = id;
    }

    void writeStop()
    {
        out.write(0);
    }

    void enterStruct()
    {
        stack.push(lastFieldId);
        lastFieldId = 0;
    }

    /** Writes the struct STOP byte and pops the outer field-id frame. */
    void exitStruct()
    {
        writeStop();
        lastFieldId = stack.pop();
    }

    void writeI64Field(short id, long value)
    {
        writeFieldBegin(CompactProtocolReader.TYPE_I64, id);
        writeZigzag64(value);
    }

    void writeI32Field(short id, int value)
    {
        writeFieldBegin(CompactProtocolReader.TYPE_I32, id);
        writeZigzag32(value);
    }

    void writeI16Field(short id, short value)
    {
        writeFieldBegin(CompactProtocolReader.TYPE_I16, id);
        writeZigzag32(value);
    }

    void writeBinaryField(short id, byte[] bytes)
    {
        writeFieldBegin(CompactProtocolReader.TYPE_BINARY, id);
        writeVarint32(bytes.length);
        out.writeBytes(bytes);
    }

    /**
     * Writes the list field header and list framing. Caller is
     * responsible for emitting exactly {@code size} elements of
     * {@code elementType} afterwards.
     */
    void writeListFieldHeader(short id, byte elementType, int size)
    {
        writeFieldBegin(CompactProtocolReader.TYPE_LIST, id);
        writeListHeader(elementType, size);
    }

    void writeListHeader(byte elementType, int size)
    {
        if (size < 15) {
            out.write((size << 4) | (elementType & 0x0f));
        }
        else {
            out.write(0xf0 | (elementType & 0x0f));
            writeVarint32(size);
        }
    }

    /** Writes a STRUCT-typed field header, then enters the nested struct. */
    void writeStructFieldBegin(short id)
    {
        writeFieldBegin(CompactProtocolReader.TYPE_STRUCT, id);
        enterStruct();
    }

    void writeZigzag32(int v)
    {
        writeVarint32((v << 1) ^ (v >> 31));
    }

    void writeZigzag64(long v)
    {
        writeVarint64((v << 1) ^ (v >> 63));
    }

    void writeVarint32(int v)
    {
        while ((v & ~0x7f) != 0) {
            out.write((v & 0x7f) | 0x80);
            v >>>= 7;
        }
        out.write(v & 0x7f);
    }

    void writeVarint64(long v)
    {
        while ((v & ~0x7fL) != 0) {
            out.write((int) ((v & 0x7f) | 0x80));
            v >>>= 7;
        }
        out.write((int) (v & 0x7f));
    }

    byte[] toBytes()
    {
        return out.toByteArray();
    }
}
