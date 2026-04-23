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

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.Objects;

/**
 * A content-addressed Shelf cache key (SHELF-04).
 *
 * <p>The 32-byte digest is computed as
 * {@code sha256(etag || le_u64(offset) || le_u64(length) || le_u32(rg_ordinal))}
 * where the integer encodings are <b>little-endian</b> so Rust's
 * {@code .to_le_bytes()} and Java's {@link ByteOrder#LITTLE_ENDIAN} agree
 * byte-for-byte.
 *
 * <p><b>Multipart ETag caveat.</b> S3's ETag is the MD5 of a single-PUT
 * object but {@code md5(parts)-N} for multipart objects; neither form is
 * a cryptographic hash of the object. Shelf never treats ETag as an
 * integrity token &mdash; only as an opaque version string that changes
 * whenever S3 observes a new version. The content-addressed property
 * comes from the SHA-256 over the concatenated inputs.
 *
 * <p>The fixture file
 * {@code ../../shelfd/tests/fixtures/shelf04_golden_vectors.txt} holds
 * the expected hex digests for the golden inputs; both the Rust
 * {@code store::key_tests} and the Java {@link KeyTest} suite diff
 * against that file, so any divergence fails CI instantly.
 */
public final class Key
{
    /** Length of a Shelf cache key, in bytes. */
    public static final int LENGTH = 32;

    private final byte[] bytes;

    private Key(byte[] bytes)
    {
        this.bytes = Objects.requireNonNull(bytes, "bytes");
        if (bytes.length != LENGTH) {
            throw new IllegalArgumentException(
                    "Key must be " + LENGTH + " bytes, got " + bytes.length);
        }
    }

    /**
     * Derive a content-addressed key from the SHELF-04 tuple.
     *
     * @throws IllegalArgumentException if {@code etag} is empty or
     *                                  {@code length} is zero
     */
    public static Key fromTuple(byte[] etag, long offset, long length, int rgOrdinal)
    {
        Objects.requireNonNull(etag, "etag");
        if (etag.length == 0) {
            throw new IllegalArgumentException("etag must be non-empty");
        }
        if (length == 0L) {
            throw new IllegalArgumentException("length must be > 0");
        }

        MessageDigest digest;
        try {
            digest = MessageDigest.getInstance("SHA-256");
        }
        catch (NoSuchAlgorithmException impossible) {
            // Every JRE ships SHA-256; this is not a recoverable situation.
            throw new IllegalStateException("SHA-256 unavailable", impossible);
        }

        ByteBuffer header = ByteBuffer.allocate(8 + 8 + 4).order(ByteOrder.LITTLE_ENDIAN);
        header.putLong(offset);
        header.putLong(length);
        header.putInt(rgOrdinal);

        digest.update(etag);
        digest.update(header.array());
        return new Key(digest.digest());
    }

    /** Convenience for string ETags (values returned by S3 include the quotes). */
    public static Key fromTuple(String etag, long offset, long length, int rgOrdinal)
    {
        return fromTuple(etag.getBytes(StandardCharsets.UTF_8), offset, length, rgOrdinal);
    }

    /** @return a defensive copy of the 32-byte digest. */
    public byte[] asBytes()
    {
        return Arrays.copyOf(bytes, bytes.length);
    }

    /** @return the lowercase hex rendering of this key (64 characters). */
    public String toHex()
    {
        return HexFormat.of().formatHex(bytes);
    }

    /** Parse a 64-char lowercase-hex string into a key. */
    public static Key fromHex(String hex)
    {
        Objects.requireNonNull(hex, "hex");
        if (hex.length() != LENGTH * 2) {
            throw new IllegalArgumentException(
                    "Hex key must be " + (LENGTH * 2) + " chars, got " + hex.length());
        }
        return new Key(HexFormat.of().parseHex(hex));
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (!(o instanceof Key that)) {
            return false;
        }
        return Arrays.equals(this.bytes, that.bytes);
    }

    @Override
    public int hashCode()
    {
        return Arrays.hashCode(bytes);
    }

    @Override
    public String toString()
    {
        return "Key(" + toHex() + ")";
    }
}
