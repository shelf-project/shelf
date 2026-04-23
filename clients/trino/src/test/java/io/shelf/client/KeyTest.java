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
    /** Inputs: {@code (etag, offset, length, rg_ordinal)}. */
    private static final Object[][] GOLDEN_INPUTS = new Object[][] {
            {"\"9f8e6e48a1f7e2c3b5d41234567890ab\"", 0L,           8_192L,  0},
            {"\"aa11bb22cc33dd44ee55ff6677889900\"", 536_854_528L, 65_536L, 0},
            {"\"aa11bb22cc33dd44ee55ff6677889900\"", 536_854_528L, 65_536L, 3},
            {"\"d41d8cd98f00b204e9800998ecf8427e-7\"", 1L,         1L,      42},
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
