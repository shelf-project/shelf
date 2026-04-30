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
package io.shelf.eventlistener;

import io.shelf.eventlistener.ShelfFilterClient.Predicate;
import io.shelf.eventlistener.ShelfFilterClient.ProbeResult;
import io.shelf.eventlistener.ShelfFilterClient.RowGroupRef;
import io.shelf.eventlistener.SkippableSplitFilter.Result;
import io.shelf.eventlistener.SkippableSplitFilter.Split;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.net.http.HttpClient;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class SkippableSplitFilterTest
{
    @Test
    void keepsAllWhenProbeFailsOpen()
    {
        FakeClient client = new FakeClient(ProbeResult.unfiltered());
        SkippableSplitFilter filter = new SkippableSplitFilter(client);
        List<Split> splits = List.of(
                split("etag-a", 0),
                split("etag-a", 1));
        Result result = filter.apply(splits);
        assertEquals(2, result.kept().size());
        assertEquals(0, result.droppedSplits());
    }

    @Test
    void dropsSplitsOutsideMaybeMatch()
    {
        FakeClient client = new FakeClient(new ProbeResult(false, List.of(
                new RowGroupRef("etag-a", 0))));
        SkippableSplitFilter filter = new SkippableSplitFilter(client);
        List<Split> splits = List.of(
                split("etag-a", 0),
                split("etag-a", 1),
                split("etag-b", 0));
        Result result = filter.apply(splits);
        assertEquals(1, result.kept().size());
        assertEquals(2, result.droppedSplits());
    }

    @Test
    void batchesOneProbePerPredicateGroup()
    {
        FakeClient client = new FakeClient(ProbeResult.unfiltered());
        SkippableSplitFilter filter = new SkippableSplitFilter(client);
        List<Split> splits = new ArrayList<>();
        // Two groups: same column, two different predicates.
        for (int i = 0; i < 5; i++) {
            splits.add(split("etag-a", i, "user_id", new Predicate.Equal(new byte[] {1})));
        }
        for (int i = 0; i < 3; i++) {
            splits.add(split("etag-a", i, "user_id", new Predicate.Equal(new byte[] {2})));
        }
        Result result = filter.apply(splits);
        assertEquals(2, client.calls);
        assertEquals(8, result.kept().size());
        assertEquals(2, result.probesIssued());
    }

    @Test
    void unprobedSplitsSurviveUnfiltered()
    {
        FakeClient client = new FakeClient(ProbeResult.unfiltered());
        SkippableSplitFilter filter = new SkippableSplitFilter(client);
        Split unprobed = new Split(
                null, null, null,
                null, "etag-a", 0, null);
        Result result = filter.apply(List.of(unprobed));
        assertEquals(1, result.kept().size());
        assertEquals(0, client.calls);
    }

    private static Split split(String etag, int ordinal)
    {
        return split(etag, ordinal, "user_id", new Predicate.Equal(new byte[] {7}));
    }

    private static Split split(String etag, int ordinal, String column, Predicate pred)
    {
        return new Split(
                "iceberg.analytics.events",
                column,
                pred,
                "s3://bucket/manifest",
                etag,
                ordinal,
                null);
    }

    /** Inline fake that returns a preset outcome for every probe. */
    private static final class FakeClient
            extends ShelfFilterClient
    {
        final ProbeResult canned;
        int calls;

        FakeClient(ProbeResult canned)
        {
            super(HttpClient.newHttpClient(), URI.create("http://localhost:0/unused"),
                    Duration.ofMillis(1));
            this.canned = canned;
        }

        @Override
        public ProbeResult probe(ProbeRequest request)
        {
            calls++;
            return canned;
        }

        @Override
        public List<ProbeResult> probeBatch(List<ProbeRequest> requests)
        {
            calls += requests.size();
            return Collections.nCopies(requests.size(), canned);
        }
    }

    /** Pure JSON-shape assertions isolated from the batching logic. */
    @Test
    void requestJsonIncludesPredicateShape()
    {
        ShelfFilterClient.ProbeRequest req = new ShelfFilterClient.ProbeRequest(
                "iceberg.a.b",
                "user_id",
                new Predicate.Equal(new byte[] {1, 2}),
                List.of("s3://bucket/m1"));
        String json = req.toJson();
        assertTrue(json.contains("\"kind\":\"equal\""), json);
        assertTrue(json.contains("\"manifest_files\":[\"s3://bucket/m1\"]"), json);
    }

    @Test
    void resultParserToleratesMissingFields()
    {
        ProbeResult parsed = ProbeResult.parse("{\"fail_open\":true,\"maybe_match\":[]}");
        assertTrue(parsed.failOpen());

        ProbeResult good = ProbeResult.parse(
                "{\"fail_open\":false,\"maybe_match\":[{\"file_etag\":\"e\",\"row_group_ordinal\":3}]}");
        assertEquals(1, good.maybeMatch().size());
        assertEquals(3, good.maybeMatch().get(0).rowGroupOrdinal());
    }
}
