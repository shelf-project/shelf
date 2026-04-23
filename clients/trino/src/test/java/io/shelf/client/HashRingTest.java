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

import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Cross-language parity suite for HRW routing.
 *
 * <p>The fixture lives in {@code shelfd/tests/fixtures/hrw_golden_vectors.txt}
 * and is regenerated on the Rust side via
 * {@code SHELF_REGEN_FIXTURES=1 cargo test -p shelfd --lib router::tests}.
 * If any assertion here fails, check whether
 * {@link HashRing#score(byte[], HashRing.Node)} still matches
 * {@code shelfd::router::hrw_score} — do NOT regenerate the fixture without
 * making sure both sides still agree.
 */
class HashRingTest
{
    private static final List<HashRing.Node> RING = List.of(
            new HashRing.Node("shelf-0", 1),
            new HashRing.Node("shelf-1", 2),
            new HashRing.Node("shelf-2", 3));

    private static final Path FIXTURE = Path.of("..", "..", "shelfd", "tests", "fixtures", "hrw_golden_vectors.txt");

    private static byte[] goldenKey(int i) throws Exception
    {
        MessageDigest md = MessageDigest.getInstance("SHA-256");
        md.update("shelf-hrw-golden-v1".getBytes(java.nio.charset.StandardCharsets.UTF_8));
        ByteBuffer buf = ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN);
        buf.putInt(i);
        md.update(buf.array());
        return md.digest();
    }

    @Test
    void ownerMatchesGoldenFixture() throws Exception
    {
        assertThat(FIXTURE)
                .as("HRW golden-vector fixture is shared with the Rust side")
                .exists();

        HashRing ring = new HashRing(RING);
        List<String> fixtureLines = Files.readAllLines(FIXTURE);
        int asserted = 0;
        for (String line : fixtureLines) {
            String trimmed = line.trim();
            if (trimmed.isEmpty() || trimmed.startsWith("#")) {
                continue;
            }
            int tab = trimmed.indexOf('\t');
            assertThat(tab).as("fixture line without a tab: %s", trimmed).isPositive();
            int index = Integer.parseInt(trimmed.substring(0, tab));
            String expected = trimmed.substring(tab + 1);

            byte[] key = goldenKey(index);
            Optional<HashRing.Node> owner = ring.ownerFor(key);
            assertThat(owner).isPresent();
            assertThat(owner.get().podId())
                    .as("Java HRW disagrees with Rust for key_%d", index)
                    .isEqualTo(expected);
            asserted++;
        }
        assertThat(asserted)
                .as("expected 1000 golden vectors; HRW fixture may be truncated")
                .isEqualTo(1000);
    }

    @Test
    void emptyRingReturnsEmpty()
    {
        assertThat(new HashRing(List.of()).ownerFor(new byte[]{1, 2, 3})).isEmpty();
    }

    @Test
    void heavierNodeWinsMoreOften() throws Exception
    {
        HashRing ring = new HashRing(RING);
        Map<String, Integer> counts = new HashMap<>();
        for (int i = 0; i < 3000; i++) {
            HashRing.Node owner = ring.ownerFor(goldenKey(i)).orElseThrow();
            counts.merge(owner.podId(), 1, Integer::sum);
        }
        int c0 = counts.getOrDefault("shelf-0", 0);
        int c1 = counts.getOrDefault("shelf-1", 0);
        int c2 = counts.getOrDefault("shelf-2", 0);
        assertThat(c0).isLessThan(c1);
        assertThat(c1).isLessThan(c2);
    }

    @Test
    void singleMemberAddMovesApproximatelyItsShare() throws Exception
    {
        HashRing before = new HashRing(RING);
        List<HashRing.Node> biggerMembers = new ArrayList<>(RING);
        biggerMembers.add(new HashRing.Node("shelf-3", 1));
        HashRing after = new HashRing(biggerMembers);

        int moved = 0;
        for (int i = 0; i < 200; i++) {
            byte[] k = goldenKey(i);
            String a = before.ownerFor(k).orElseThrow().podId();
            String b = after.ownerFor(k).orElseThrow().podId();
            if (!a.equals(b)) {
                moved++;
            }
        }
        assertThat(moved)
                .as("HRW should only move keys that the new node now wins")
                .isLessThanOrEqualTo(60);
    }
}
