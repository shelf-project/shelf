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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

import org.junit.jupiter.api.Test;

/**
 * SHELF-04: cross-language key-derivation invariants.
 *
 * <p>The golden inputs here must mirror {@code GOLDEN_INPUTS} in
 * {@code shelfd/src/store.rs}. The expected hex digests live in the
 * shared fixture file
 * {@code shelfd/tests/fixtures/shelf04_golden_vectors.txt} so that any
 * algorithm divergence between Rust and Java fails in CI on both
 * sides. See ADR-0011 for the invariant.
 */
class KeyTest
{
    /**
     * Inputs: {@code (etag, offset, length, rg_ordinal)}.
     *
     * <p>Kept byte-for-byte in lockstep with {@code GOLDEN_INPUTS} in
     * {@code shelfd/src/store.rs} and {@code tools/gen_shelf04_golden.py}.
     * Any drift is caught by {@link #goldenVectorsMatchSharedFixture}
     * because all three consume the exact same fixture file.
     */
    private static final Object[][] GOLDEN_INPUTS = new Object[][] {
            // -- SHELF-04 baseline --
            {"\"9f8e6e48a1f7e2c3b5d41234567890ab\"",  0L,                          8_192L,           0},
            {"\"aa11bb22cc33dd44ee55ff6677889900\"",  536_854_528L,                65_536L,          0},
            {"\"aa11bb22cc33dd44ee55ff6677889900\"",  536_854_528L,                65_536L,          3},
            {"\"d41d8cd98f00b204e9800998ecf8427e-7\"", 1L,                         1L,               42},
            // -- SHELF-16: row-group ordinal variants --
            // Same (etag, offset, length), three distinct rg ordinals.
            {"\"rg-ordinal-sweep\"",                  4_096L,                      131_072L,         0},
            {"\"rg-ordinal-sweep\"",                  4_096L,                      131_072L,         1},
            {"\"rg-ordinal-sweep\"",                  4_096L,                      131_072L,         7},
            // Offset = u64::MAX / 2 = Long.MAX_VALUE = 9_223_372_036_854_775_807.
            {"\"big-offset\"",                        Long.MAX_VALUE,              16L,              0},
            {"\"big-offset\"",                        Long.MAX_VALUE,              16L,              255},
            // Length = 1 byte, ordinal = u16 ceiling.
            {"\"single-byte\"",                       0L,                          1L,               65_535},
            // Length = 16 MiB, ordinal = 4_096.
            {"\"row-group-xl\"",                      0L,                          (long) 16 * 1024 * 1024, 4_096},
            // Multipart-form ETag with ordinals 0 and 2.
            {"\"\"-multipart\"",                    0L,                          4_096L,           0},
            {"\"\"-multipart\"",                    0L,                          4_096L,           2},
            // ASCII-only 8-byte ETag (no outer quotes — exactly 8 bytes),
            // every ordinal in 0..=3.
            {"shelf16b",                                  2_048L,                      8_192L,           0},
            {"shelf16b",                                  2_048L,                      8_192L,           1},
            {"shelf16b",                                  2_048L,                      8_192L,           2},
            {"shelf16b",                                  2_048L,                      8_192L,           3},
    };

    @Test
    void goldenVectorsMatchSharedFixture() throws IOException
    {
        List<String> expected = loadFixture();
        assertThat(expected)
                .as("fixture must have one hex line per golden input")
                .hasSize(GOLDEN_INPUTS.length);

        for (int i = 0; i < GOLDEN_INPUTS.length; i++) {
            Object[] row = GOLDEN_INPUTS[i];
            String etag = (String) row[0];
            long offset = (long) row[1];
            long length = (long) row[2];
            int ordinal = (int) row[3];

            String got = Key.fromTuple(etag, offset, length, ordinal).toHex();
            assertThat(got)
                    .as("golden vector %d (etag=%s offset=%d length=%d ordinal=%d)",
                            i, etag, offset, length, ordinal)
                    .isEqualTo(expected.get(i));
        }
    }

    @Test
    void roundtripIsDeterministic()
    {
        for (Object[] row : GOLDEN_INPUTS) {
            Key a = Key.fromTuple((String) row[0], (long) row[1], (long) row[2], (int) row[3]);
            Key b = Key.fromTuple((String) row[0], (long) row[1], (long) row[2], (int) row[3]);
            assertThat(a).isEqualTo(b);
            assertThat(a.hashCode()).isEqualTo(b.hashCode());
        }
    }

    @Test
    void ordinalChangesKey()
    {
        Key a = Key.fromTuple("etag", 0L, 1L, 0);
        Key b = Key.fromTuple("etag", 0L, 1L, 1);
        assertThat(a).isNotEqualTo(b);
    }

    /**
     * SHELF-16 acceptance line (verbatim from
     * {@code agents/out/03-plan.md}):
     * "Unit test: (file X, rg 2) and (file X, rg 3) produce distinct
     * keys". This test pins that acceptance against a concrete
     * {@code (offset, length)} — the same row-group byte range viewed
     * through two different ordinals must hash differently, because
     * {@code rg_ordinal} is part of the SHA-256 preimage.
     */
    @Test
    void keysDifferByRowGroupOrdinal()
    {
        String etag = "\"file-X\"";
        long offset = 1_000L;
        long length = 1_024L;
        Key rg2 = Key.fromTuple(etag, offset, length, 2);
        Key rg3 = Key.fromTuple(etag, offset, length, 3);
        assertThat(rg2)
                .as("(file X, rg 2) and (file X, rg 3) must produce distinct keys")
                .isNotEqualTo(rg3);
    }

    @Test
    void offsetAndLengthChangeKey()
    {
        Key base = Key.fromTuple("etag", 0L, 1L, 0);
        Key shifted = Key.fromTuple("etag", 1L, 1L, 0);
        Key longer = Key.fromTuple("etag", 0L, 2L, 0);
        assertThat(base).isNotEqualTo(shifted);
        assertThat(base).isNotEqualTo(longer);
    }

    @Test
    void etagChangesKey()
    {
        Key a = Key.fromTuple("etag-a", 0L, 1L, 0);
        Key b = Key.fromTuple("etag-b", 0L, 1L, 0);
        assertThat(a).isNotEqualTo(b);
    }

    @Test
    void rejectsEmptyEtag()
    {
        assertThatThrownBy(() -> Key.fromTuple(new byte[0], 0L, 1L, 0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("etag");
    }

    @Test
    void rejectsZeroLength()
    {
        assertThatThrownBy(() -> Key.fromTuple("etag", 0L, 0L, 0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("length");
    }

    @Test
    void hexRoundtrip()
    {
        Key k = Key.fromTuple("etag", 123L, 456L, 7);
        Key parsed = Key.fromHex(k.toHex());
        assertThat(parsed).isEqualTo(k);
        assertThat(k.toHex()).hasSize(64).matches("^[0-9a-f]+$");
    }

    @Test
    void fromHexRejectsWrongLength()
    {
        assertThatThrownBy(() -> Key.fromHex("abc"))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> Key.fromHex("a".repeat(63)))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> Key.fromHex("a".repeat(65)))
                .isInstanceOf(IllegalArgumentException.class);
    }

    private static List<String> loadFixture() throws IOException
    {
        // Maven runs surefire with user.dir = module root (clients/trino).
        // The shared fixture lives two levels up in the repo.
        Path fixture = Paths.get(System.getProperty("user.dir"))
                .resolve("../../shelfd/tests/fixtures/shelf04_golden_vectors.txt")
                .toAbsolutePath()
                .normalize();
        assertThat(fixture)
                .as("shared SHELF-04 fixture must exist at %s", fixture)
                .exists();
        return Files.readAllLines(fixture).stream()
                .filter(l -> !l.isEmpty() && !l.startsWith("#"))
                .toList();
    }
}
