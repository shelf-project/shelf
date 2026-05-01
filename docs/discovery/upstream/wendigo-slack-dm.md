# Paste-ready Slack DM for `@wendigo` on trino.io/slack

Copy the message between the `=== START ===` and `=== END ===` markers verbatim into a Slack DM to `@wendigo` on https://trino.io/slack. ≤ 200 words. Tone: technical-peer.

> **Pre-send checklist.**
>
> 1. Wait until ≥ 24 h after posting the [GitHub PR review comment](./29184-review-comment.md) — let the public comment land first; the DM acknowledges it.
> 2. Verify the user is signed into trino.io/slack with their normal handle.
> 3. The DM mentions Shelf's GitHub URL and asks no questions that require a meeting — keep it async-friendly.

=== START ===

Hi `@wendigo` 👋 — I'm `@aamir306`, BDFL of [shelf-project/shelf](https://github.com/shelf-project/shelf). It's an Iceberg-native row-group cache for Trino, just released v1.0 (Apache 2.0). On a four-replica production cluster we measured 94 % → 5.7 % infra-failure-rate drop and p50 read wall time 5.74 s → 2.05 s after cutover from a previous cache layer.

Shelf currently integrates via the S3-endpoint-shim path because the SPI you're proposing in [trinodb/trino#29184](https://github.com/trinodb/trino/pull/29184) doesn't exist yet. The unified `BlobCacheManagerFactory` shape solves exactly the problem we've been blocked on.

I left a public review comment on the PR with five concrete pieces of design feedback (`CacheKey` opacity, `CacheTier` granularity, `invalidate` semantics for content-addressed caches, `length(key)` metadata-only path, peer-fetch awareness). All grounded in Shelf's existing architecture; happy to drill into any of them.

Two practical offers:

1. We'd like to be the first OSS third-party plugin consumer of the merged SPI — `plugin/trino-blob-cache-shelf/` would land within a week of #29184 merging, sketch [here](https://github.com/shelf-project/shelf/blob/main/clients/trino/docs/blob-cache-plugin-sketch.md).
2. Happy to draft small fix-up PRs against #29184 addressing CodeRabbit-flagged issues — let me know which would be most useful.

No rush — async is fine.

=== END ===

## Notes on tone

- Opens with one impact number from the README (the same one cited in the GitHub comment, for consistency). Avoids vendor-pitch language ("game-changing", "best-in-class") that's a red flag in OSS-maintainer DMs.
- Acknowledges that the GitHub comment was posted first — the DM is a courtesy heads-up, not a parallel reach-out. Maintainers find it polite when contributors don't double-channel-spam.
- Two concrete offers (be the first plugin consumer; draft fix-up PRs). Either lets `@wendigo` say yes to something useful without committing to a meeting or scope discussion.
- Closes with "no rush — async is fine" so the maintainer doesn't feel pressured to drop everything; matches the tone of `@wendigo`'s own commit cadence (force-pushes mid-week, rest of the time elsewhere).
- Single emoji at the start (a wave) — Slack-native register without being unprofessional. Drop it if `@wendigo`'s public posts skew formal; check his recent #dev posts before sending.

## What to do after sending

1. Don't bump if no response within a week — `@wendigo` works for Starburst and has many irons in the fire.
2. If he responds with "yes please draft fix-up PRs", do that work next session and link the PRs back in the same DM thread.
3. If he responds with "interesting, let's talk on the GitHub PR" — fine, the DM did its job (it surfaced Shelf as an interested OSS consumer) and the conversation moves to the public PR thread.
4. Track outcome on the [shelf-project/shelf tracking issue](https://github.com/shelf-project/shelf/issues).

## What NOT to do

- Don't include the full SPI feedback in the DM — keep it short and link out. Slack walls of text don't get read.
- Don't ask for a meeting / call. The maintainer's time is the constraint; async is the format that respects it.
- Don't pitch Shelf's commercial story (there isn't one — Shelf is OSS) or attempt to draw any analogy to Warp Speed. Apache 2.0 OSS posture is the load-bearing identity here; muddying it loses everything.
- Don't @-mention `@losipiuk` or other Trino maintainers in the DM. One contact at a time.
