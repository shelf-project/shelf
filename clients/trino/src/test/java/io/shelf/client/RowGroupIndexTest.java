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

import java.util.List;
import java.util.Optional;

import org.junit.jupiter.api.Test;

/**
 * SHELF-16 unit tests for {@link RowGroupIndex} and its two
 * implementations. The Parquet TCompactProtocol footer parser itself
 * has its own exhaustive suite in {@link ParquetFooterIndexTest} (see
 * SHELF-16b); the {@code fromFooter_*} cases here are kept as a
 * regression fence for the fail-open contract — garbage in must
 * always yield {@link Optional#empty()} with no throw.
 */
class RowGroupIndexTest
{
    @Test
    void constantZero_returnsZeroEverywhere()
    {
        RowGroupIndex index = RowGroupIndex.constantZero();
        assertThat(index.hasKnownOrdinals()).isFalse();
        assertThat(index.ordinalFor(0L, 1L)).isZero();
        assertThat(index.ordinalFor(1_000L, 1_024L)).isZero();
        assertThat(index.ordinalFor(Long.MAX_VALUE / 2, 16L)).isZero();
    }

    @Test
    void constantZero_isSingleton()
    {
        assertThat(RowGroupIndex.constantZero())
                .isSameAs(RowGroupIndex.constantZero())
                .isSameAs(ConstantOrdinalIndex.INSTANCE);
    }

    @Test
    void parquetFooterIndex_ordinalForReturnsMatchingRowGroup()
    {
        // Three back-to-back row groups with gaps in between (mimics a
        // real Parquet layout where page data sits between row-group
        // column chunks and footers).
        ParquetFooterIndex index = ParquetFooterIndex.of(List.of(
                new ParquetFooterIndex.RowGroup(100L, 1_024L, 0),
                new ParquetFooterIndex.RowGroup(2_048L, 4_096L, 1),
                new ParquetFooterIndex.RowGroup(8_192L, 16_384L, 2)));

        assertThat(index.hasKnownOrdinals()).isTrue();
        // Inside rg#0.
        assertThat(index.ordinalFor(100L, 100L)).isZero();
        assertThat(index.ordinalFor(500L, 128L)).isZero();
        // Inside rg#1.
        assertThat(index.ordinalFor(2_048L, 4_096L)).isEqualTo(1);
        assertThat(index.ordinalFor(3_000L, 512L)).isEqualTo(1);
        // Inside rg#2 (large row-group, whole range).
        assertThat(index.ordinalFor(8_192L, 16_384L)).isEqualTo(2);
        // Gap between rg#0 and rg#1 — not covered.
        assertThat(index.ordinalFor(1_500L, 16L)).isZero();
        // Range that spans two row groups — unknown.
        assertThat(index.ordinalFor(100L, 10_000L)).isZero();
        // Before the first row group.
        assertThat(index.ordinalFor(0L, 32L)).isZero();
        // After the last row group.
        assertThat(index.ordinalFor(100_000L, 32L)).isZero();
    }

    @Test
    void parquetFooterIndex_sortsUnsortedInput()
    {
        // Out-of-order rg list: the index must sort internally so
        // ordinalFor still works.
        ParquetFooterIndex index = ParquetFooterIndex.of(List.of(
                new ParquetFooterIndex.RowGroup(8_192L, 1_024L, 2),
                new ParquetFooterIndex.RowGroup(0L, 1_024L, 0),
                new ParquetFooterIndex.RowGroup(4_096L, 1_024L, 1)));

        assertThat(index.rowGroups()).hasSize(3);
        assertThat(index.rowGroups().get(0).ordinal()).isZero();
        assertThat(index.rowGroups().get(2).ordinal()).isEqualTo(2);
        assertThat(index.ordinalFor(4_096L, 1_024L)).isEqualTo(1);
    }

    @Test
    void parquetFooterIndex_rejectsOverlappingRowGroups()
    {
        assertThatThrownBy(() -> ParquetFooterIndex.of(List.of(
                new ParquetFooterIndex.RowGroup(0L, 1_024L, 0),
                new ParquetFooterIndex.RowGroup(512L, 1_024L, 1))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("non-overlapping");
    }

    @Test
    void parquetFooterIndex_rejectsNegativeOrZeroSizes()
    {
        assertThatThrownBy(() -> new ParquetFooterIndex.RowGroup(-1L, 1L, 0))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new ParquetFooterIndex.RowGroup(0L, 0L, 0))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new ParquetFooterIndex.RowGroup(0L, 1L, -1))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void parquetFooterIndex_emptyHasNoKnownOrdinals()
    {
        ParquetFooterIndex empty = ParquetFooterIndex.of(List.of());
        assertThat(empty.hasKnownOrdinals()).isFalse();
        assertThat(empty.ordinalFor(0L, 1L)).isZero();
    }

    /**
     * SHELF-16b fail-open regression fence: {@code fromFooter} must
     * never throw, even on obviously invalid input. Valid-footer
     * coverage lives in {@link ParquetFooterIndexTest}; this test
     * pins the no-throw contract that the plugin's error budget
     * depends on.
     */
    @Test
    void fromFooter_returnsEmpty_onInvalidInput()
    {
        // Empty footer.
        assertThat(ParquetFooterIndex.fromFooter(new byte[0], 0L)).isEmpty();
        // Plausible-looking tail bytes with the PAR1 magic but
        // footer_length = 0 — the parser rejects that as malformed.
        byte[] tail = new byte[] {
                0, 0, 0, 0,
                'P', 'A', 'R', '1'
        };
        assertThat(ParquetFooterIndex.fromFooter(tail, 8L)).isEmpty();
    }

    @Test
    void fromFooter_rejectsNegativeFileLength()
    {
        assertThatThrownBy(() -> ParquetFooterIndex.fromFooter(new byte[8], -1L))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
