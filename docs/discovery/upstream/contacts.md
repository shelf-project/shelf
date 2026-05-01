# Trino upstream contacts

Quick reference for the maintainers, Slack channels, and GitHub subscriptions that matter for Shelf's upstream-engagement plan. Update as the political landscape evolves; this doc is meant to drift, not to be timeless.

## Maintainers worth knowing

| Handle | Role | Affiliation | Why they matter |
|---|---|---|---|
| `@wendigo` | Author of [trinodb/trino#29184](https://github.com/trinodb/trino/pull/29184) (the blob-cache SPI) | Starburst | Primary contact for the SPI design. Approves and shepherds the PR. The Slack DM draft at [wendigo-slack-dm.md](./wendigo-slack-dm.md) targets this person. |
| `@losipiuk` | Designated reviewer on #29184 | Starburst | Secondary reviewer; his comments shape the final API. Watch his review pattern on the PR. |
| `@findepi` | Long-standing Trino maintainer | Starburst | Frequent reviewer of SPI / connector PRs. Not directly on #29184 but his feedback often surfaces in adjacent threads. |
| `@martint` | Trino founder / heavy maintainer | Starburst | Strategic-level design calls. Won't typically engage on a single PR but his architectural opinions filter through. |
| `@kasiafi` | Trino reviewer | Starburst | Reviews SPI shape proposals. |

Pattern note: most active Trino maintainers are Starburst employees, which is why the [governance hazards](../trino-upstream-strategy.md#governance-hazards) — particularly [#22827](https://github.com/trinodb/trino/issues/22827) (Starburst-overlap) — matter for an Apache 2.0 OSS plugin's positioning. Stay clearly OSS-shaped, don't propose Warp-Speed-like APIs.

## Slack channels worth watching

The Trino community Slack is at [trino.io/slack](https://trino.io/slack). After joining, the relevant channels for cache integration:

| Channel | What's there | Why we care |
|---|---|---|
| `#dev` | Active maintainer discussion of design / PRs | Where #29184 conversation often surfaces beyond the PR thread |
| `#filesystem-cache` (if it exists; check) | Cache-specific design discussion | Native home for blob-cache SPI design talk |
| `#performance` | Production performance tuning discussion | Where users post Trino-is-slow questions; chance to mention Shelf naturally |
| `#announcements` | Release / SPI / breaking-change announcements | Watch for #29184 merge announcement |
| `#general` | All-comers | Don't post here unless absolutely necessary; signal-to-noise is too low to be productive |

Etiquette: Lurk for at least a week before posting. Don't @-mention maintainers. Reply in-thread, not in a new top-level message.

## GitHub subscriptions to enable manually

These are GitHub-UI actions under the user's account — the agent can't do them remotely. Enable each so notifications arrive without polling:

| URL | What you watch for | Action |
|---|---|---|
| https://github.com/trinodb/trino/pull/29184 | The blob-cache SPI PR — every force-push, comment, review | Click "Subscribe" on the PR page |
| https://github.com/trinodb/trino/issues/22827 | Starburst-overlap concerns — context for governance posture | Click "Subscribe" on the issue |
| https://github.com/trinodb/trino/issues/24737 | Stale-closed external-cache PR — pattern to avoid | Click "Subscribe" |
| https://github.com/trinodb/trino/labels/caching | Repository-wide cache-related issue feed | Watch via filter; add to home dashboard |

Set up an email filter for `notifications@github.com` from `trinodb/trino` to a dedicated label; otherwise these threads drown in the noise of every other repo activity.

## Cross-org adjacency

Other projects whose cache decisions affect Shelf's upstream plan:

| Project | Why we care |
|---|---|
| `Alluxio/alluxio` | The other major Trino blob-cache plugin. Their feedback on #29184 shapes the SPI as much as Shelf's. Watch [Alluxio/alluxio#issues](https://github.com/Alluxio/alluxio/issues) for Trino-related discussion. |
| `apache/iceberg` | If `BlobCache` ever grows Iceberg-aware semantics (snapshot-pinning, manifest-cache hints), the Iceberg side of the conversation lives here. |
| `apache/parquet-format` | Shelf's row-group granularity assumes Parquet's column-chunk byte-range layout. Format-level changes (`PageIndex`, `BloomFilter` placement) ripple into Shelf's prefetch logic. |

## When to update this file

- A new contact engages on #29184 — add them to the Maintainers table
- A Slack channel changes name or new one appears — fix the row
- The SPI merges, ships, or gets superseded — annotate the strategy doc and update the actions in this file
- Shelf gains its own additional contributors who'd be the right reach-out person for some of these channels — add them as alternative contacts so the work isn't bottlenecked on one person

## See also

- [docs/discovery/trino-upstream-strategy.md](../trino-upstream-strategy.md) — the overall engagement plan that uses these contacts
- [docs/discovery/upstream/wendigo-slack-dm.md](./wendigo-slack-dm.md) — paste-ready DM for `@wendigo`
- [docs/discovery/upstream/29184-review-comment.md](./29184-review-comment.md) — paste-ready review comment for the PR
