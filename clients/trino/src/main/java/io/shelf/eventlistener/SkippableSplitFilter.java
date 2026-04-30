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
import io.shelf.eventlistener.ShelfFilterClient.ProbeRequest;
import io.shelf.eventlistener.ShelfFilterClient.ProbeResult;
import io.shelf.eventlistener.ShelfFilterClient.RowGroupRef;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * SHELF-G5 — filter a list of splits by asking shelfd which row
 * groups might match the query's predicates.
 *
 * <p>The {@link IcebergSplitSource} wrapper that will call this
 * ships with SHELF-29 (upstream cache SPI). Until then this
 * class is exercised by unit tests so the batching + grouping
 * logic is locked in ahead of the SPI landing.
 *
 * <p>Semantics:
 * <ul>
 *   <li>Group splits by {@code (tableFqn, column, predicate)}.
 *   <li>Batch one probe per group.
 *   <li>If the probe fails open (shelf has no signal), keep
 *       every split in the group.
 *   <li>Otherwise, keep splits whose {@code (file_etag,
 *       row_group_ordinal)} is in {@code maybe_match}.
 * </ul>
 *
 * <p>Metric emitted upstream: {@code
 * shelf_skipped_rowgroups_total{table, column}} counting every
 * dropped split per probe.
 */
public final class SkippableSplitFilter
{
    private final ShelfFilterClient client;

    public SkippableSplitFilter(ShelfFilterClient client)
    {
        this.client = Objects.requireNonNull(client, "client");
    }

    public Result apply(List<Split> splits)
    {
        Objects.requireNonNull(splits, "splits");
        if (splits.isEmpty()) {
            return new Result(splits, 0, 0);
        }

        // Group by (table, column, predicate). Use a LinkedHashMap
        // so the probe order is deterministic — makes test
        // assertions stable and lets the coordinator log a
        // predictable probe sequence.
        Map<GroupKey, List<Split>> grouped = new LinkedHashMap<>();
        for (Split s : splits) {
            if (s.tableFqn() == null || s.column() == null || s.predicate() == null) {
                grouped.computeIfAbsent(GroupKey.unprobed(), k -> new ArrayList<>()).add(s);
                continue;
            }
            GroupKey key = new GroupKey(s.tableFqn(), s.column(), s.predicate());
            grouped.computeIfAbsent(key, k -> new ArrayList<>()).add(s);
        }

        List<Split> kept = new ArrayList<>();
        int probes = 0;
        int dropped = 0;

        for (Map.Entry<GroupKey, List<Split>> e : grouped.entrySet()) {
            GroupKey key = e.getKey();
            List<Split> group = e.getValue();
            if (key.isUnprobed()) {
                kept.addAll(group);
                continue;
            }
            List<String> manifests = collectManifests(group);
            ProbeRequest req = new ProbeRequest(
                    key.tableFqn, key.column, key.predicate, manifests);
            ProbeResult result = client.probe(req);
            probes++;
            if (result.failOpen()) {
                kept.addAll(group);
                continue;
            }
            Set<String> allowed = new HashMap<String, Boolean>() {{
                for (RowGroupRef r : result.maybeMatch()) {
                    put(rgKey(r.fileEtag(), r.rowGroupOrdinal()), Boolean.TRUE);
                }
            }}.keySet();
            for (Split s : group) {
                if (allowed.contains(rgKey(s.fileEtag(), s.rowGroupOrdinal()))) {
                    kept.add(s);
                }
                else {
                    dropped++;
                }
            }
        }

        return new Result(Collections.unmodifiableList(kept), probes, dropped);
    }

    private static String rgKey(String fileEtag, int ordinal)
    {
        return fileEtag + "#" + ordinal;
    }

    private static List<String> collectManifests(List<Split> group)
    {
        List<String> out = new ArrayList<>();
        for (Split s : group) {
            if (s.manifestFile() != null && !out.contains(s.manifestFile())) {
                out.add(s.manifestFile());
            }
        }
        return out;
    }

    /**
     * A single planning-time split, reduced to the shape the
     * filter needs. Whatever wraps {@code IcebergSplitSource}
     * projects its native type onto this before calling.
     */
    public record Split(
            String tableFqn,
            String column,
            Predicate predicate,
            String manifestFile,
            String fileEtag,
            int rowGroupOrdinal,
            Object payload)
    {
    }

    public record Result(List<Split> kept, int probesIssued, int droppedSplits) {}

    private record GroupKey(String tableFqn, String column, Predicate predicate)
    {
        static GroupKey unprobed()
        {
            return new GroupKey("", "", null);
        }

        boolean isUnprobed()
        {
            return predicate == null;
        }
    }
}
