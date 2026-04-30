# What's coming to Shelf — explained for humans

> Shelf is a thing that sits between your queries and your data. Think of it like a **pantry** next to your stove. Without it, every time you cook, you have to walk to the **grocery store** (S3) to fetch ingredients. With it, the things you reach for often are already on the shelf, two steps away. Same recipes, same food — just dramatically faster.
>
> This page is what's coming next, written for the people who *use* the data, not the people who run the cluster. No jargon, no ticket numbers. If something here would actually help you on a Tuesday morning, tell us.

---

## 1. Faster queries you'll actually feel

### Your favorite dashboard, ten times faster — even on engines that aren't Trino

**Today.** Your Tuesday-morning dashboard runs on Trino, your data scientist's notebook runs on DuckDB, your training pipeline runs on Spark, and the experimentation team uses Polars. They all read the same Iceberg tables, but the cache only helps Trino. Everyone else still walks to S3 every time.

**Coming.** Same Shelf, same pantry, **six engines** can use it: Spark, DuckDB, Polars, Daft, ClickHouse, StarRocks, and PyIceberg. One line in each engine's config and they're suddenly hitting the cache too.

> *Priya the analyst opens her DuckDB notebook to validate a number from the dashboard. The first query took 47 seconds yesterday. Today it takes 4. She didn't change anything — Shelf just started working for DuckDB.*

---

### "It got slow again on Tuesday" — fixed

**Today.** Every time the data team runs nightly compaction (the housekeeping that merges small files into big ones), the cache effectively forgets *everything* about that table. The next morning, the first analyst to run a dashboard pays the cold-start tax — sometimes minutes per query, until the cache warms up again.

**Coming.** Shelf watches for compaction events. The moment housekeeping finishes, Shelf pre-loads the *new* files for the tables people actually use. By the time you open the dashboard, the pantry is already restocked.

> *Marcus the data engineer used to get a Slack ping every Tuesday at 9:03 AM: "dashboard is slow." Now Tuesdays look like every other day.*

---

### Materialized views that refresh in seconds, not half an hour

**Today.** A materialized view is essentially a saved expensive query — say, "monthly revenue by region." Refreshing it means re-reading every base table behind it, every time. On big tables, that's 30+ seconds of staring at a spinner.

**Coming.** Shelf *knows* which tables feed which materialized views and keeps those tables warm in the pantry. The refresh still has to compute the answer (Shelf doesn't do math), but the I/O part — the slow part — drops to near-zero.

> *Finance refreshes its end-of-week revenue MV at 5pm Friday. It used to take 28 seconds. Now it takes 3.*

---

### "Where col = literal" queries get a bloom filter boost

**Today.** When you search a 200 million row table for `customer_id = 'X-91234'`, the engine has to peek inside many file chunks to find that one row. Parquet has a feature called a "bloom filter" that lets the engine skip past chunks that *definitely* don't contain your value — but Shelf today doesn't cache them properly, so every query re-fetches them from S3.

**Coming.** Shelf caches bloom filters separately so they're always hot. *And* — bonus — Shelf will analyze your query history and tell you which columns *should* have bloom filters added, so even queries that don't have them today get faster after one ALTER TABLE.

> *Customer support runs `SELECT * FROM events WHERE user_id = ?` 200 times a day. After Shelf's bloom advisor flagged `user_id`, those queries dropped from 8 seconds to under 1.*

---

## 2. Real money, real proof

### "How much did we save on S3 this month?" — finally answerable

**Today.** Your CFO asks how much the cache is saving. You squint at AWS Cost Explorer, eyeball the S3 line item, hand-wave. Truth is, no cache product on the market — Alluxio, JuiceFS, anything — actually tells you "this Iceberg table saved you $47.30 yesterday."

**Coming.** Shelf publishes a dollar figure per table, per team, per user. Audit-able formula (every component is shown). The big number subtracts Shelf's own EC2/EBS cost so we never over-claim.

> *The CFO opens shelf.<your-org>/savings on her phone Friday afternoon. "We saved $812 this week on S3. Half of it was the marketing team's campaign-attribution dashboard." She knows exactly what to thank, and exactly what to keep.*

---

### A bot that tells you which tables to fix

**Today.** "This table has 84,000 tiny files, you should run OPTIMIZE." "This column needs a bloom filter." Nobody finds out until something breaks. The platform team writes runbooks, the analytics team doesn't read them, and tables silently degrade.

**Coming.** Shelf's *advisor* watches your real query traffic for a week and emits a JSON file: "here are the 5 tables to OPTIMIZE this weekend, here are the 3 columns that should have bloom filters added, here's a materialized view candidate." You feed that JSON into your dbt repo's PR pipeline. The bot doesn't merge anything — humans still review.

> *The analytics platform team used to spend Friday afternoons hunting for performance issues. Now the advisor opens the PRs on Monday and they spend Friday on actual problems.*

---

### "Was that query slow because of Shelf, or in spite of it?"

**Today.** A query was slow. You stare at the Trino UI. You can't tell whether Shelf served it from cache, fetched from S3, or made it worse.

**Coming.** `shelfctl explain query <id>` — a one-liner that, for any past query, shows you exactly which files were cache hits, which were misses, how many MB were served from the pantry vs the grocery store, and how many dollars that saved. Like an itemized receipt.

> *Priya runs the same dashboard query twice and it gets slower the second time. She runs `shelfctl explain` and sees that two big files were evicted from cache between her runs. Mystery solved in 30 seconds, not 30 minutes.*

---

## 3. You can roll this out without breaking anything

### "I tried Shelf for a week and I have no idea if it helped"

**Today.** You install a cache, you watch dashboards for a week, you can't tell if it's working. Every cache product over-promises and under-explains.

**Coming.** Two commands. `shelfctl tune` reads a week of your real query history and prints a one-page report: "Shelf served 312 GB from cache, saved $84, hit rate 71%, here are the 10 tables you should pin." `shelfctl regret` does the *opposite* — it lists the queries where Shelf made things *worse*, with the reason. We're the first cache project that ships an "anti-bragging" mode.

> *On day 7, Marcus runs `shelfctl tune` and sees Shelf saved $84/day. He runs `shelfctl regret` and sees one tenant's ETL was getting rate-limited; he raises that tenant's quota. Day 8 onward, no regret entries.*

---

### Prove fail-open, in a 30-second GIF

**Today.** Every adopter quietly worries: "what if a Shelf pod dies mid-query?" Vendors say "fail-open, don't worry." Nobody actually demonstrates it.

**Coming.** `shelfctl chaos --kill 50%` randomly kills half the Shelf pods while a load test runs. The video shows queries continue, p99 wobbles for 5 seconds, full recovery — zero query failures. Run it on stage, on demo day, on your skeptical CFO.

> *Internal architecture review, week 3. The principal engineer says "what if half the cache dies?" You play the GIF. Meeting ends.*

---

### A/B-tag every query so cutover analysis is honest

**Today.** When you flip a catalog onto Shelf, traffic patterns change minute-by-minute (people leave for lunch, dashboards refresh on different schedules), so any "before vs after" comparison is contaminated. You announce a win that may or may not be real.

**Coming.** Shelf tags every query with `shelf_arm = A | B` based on a deterministic hash. Half the traffic goes one way, half goes the other, *at the same time*. Now your "wow it got faster" claim is statistically clean.

> *The data platform team flips one Trino replica to Shelf with A/B tagging on. After 48 hours: shelf_arm=B (Shelf) is 2.1× faster on p95 with 95% confidence. They flip the rest of the cluster Monday.*

---

## 4. So your team can actually run it

### From `git clone` to "wow" in 5 minutes

**Today.** Setting up Shelf to evaluate it takes ~30 minutes of Helm + IRSA + Trino config. By minute 12, the prospective adopter is in another tab.

**Coming.** `docker compose up`. Sample dbt project runs twice. First run: 47 seconds. Second run: 4. Dollar figure on the screen. **Five minutes from clone to "I get it."**

> *A data engineer at another company hears about Shelf in a Slack channel, runs the demo on his laptop during stand-up, and brings it to his platform meeting that afternoon.*

---

### "It said something went wrong, can you send me the bundle?"

**Today.** Support asks for kubectl logs, Helm values, Grafana screenshots, Foyer state — across N pods. Half an hour gone.

**Coming.** `shelfctl bundle` produces one tar.gz with everything, with secrets automatically redacted. Like `must-gather` for OpenShift. Drop it in the GitHub issue.

> *Marcus debugs a weird eviction pattern on Friday at 5:50 PM, files an issue with the bundle, goes home. By Monday morning the Shelf maintainers have replied with a one-line config fix.*

---

### One command to install, on any cluster

**Today.** Helm + values.yaml is the OSS standard, but it's also the OSS speed limit. ClickHouse, Bun, and dozens of newer infra projects offer `curl … | sh` because friction kills evaluation.

**Coming.** `curl https://shelf-project.dev/install.sh | sh`. The script auto-detects your Trino catalogs, generates a values.yaml *from your cluster*, shows it to you for review, asks once, and installs. URL prints at the end.

> *On a Saturday afternoon, an engineer at another company decides to "just try it." 7 minutes later they're on a slack thread sharing a hit-rate screenshot.*

---

### A public health URL like Tailscale's

**Today.** "Is Shelf up?" requires VPN + Grafana login. You can't drop a one-line link in a postmortem.

**Coming.** A public, anonymized read-only status page at `https://shelf.<your-org>/`. Capacity, hit rate, savings, current pod count. Same UI as the internal dashboard, no admin actions.

> *During a postmortem, the on-call engineer pastes one URL into the doc. Everyone else sees the same picture without asking for VPN access.*

---

### Shelf gets out of Trino's way — automatically

**Today.** Trino has its own little metadata cache. If both it and Shelf are on, they shadow each other and your hit-rate numbers look wrong. Adopters learn this the hard way.

**Coming.** Shelf detects the conflict at startup and prints a clear, one-line banner: "Hey, you should set `iceberg.metadata-cache.enabled=false` for this catalog so Shelf can do its job." No more silent surprises.

---

## 5. It plays nice with the rest of your stack

### "Hey cache, prepare these files" — engine-agnostic prefetch

**Today.** Trino tells Shelf what it's about to need (we built that). Spark, DuckDB, and friends can't, because there's no shared protocol. So they always pay the cold-start cost.

**Coming.** A simple HTTP API that any engine can call: "I'm about to scan these manifests, please warm them up." Spark, DuckDB, Polars, Daft can all hit the same wire. The cache no longer has favorites.

> *The data platform team rolls out a Spark prefetch hook. Their Monday morning Spark batch — the one that historically takes 12 minutes — now starts already-warm and finishes in 4.*

---

### BI vs ETL, settled — without a meeting

**Today.** When the nightly backfill ETL job fires up at 8:55 AM, the morning BI dashboards stutter. Trino's resource groups handle CPU and memory but not "who gets the cache's bandwidth to S3." It's first-come-first-served, and ETL always comes first.

**Coming.** Per-tenant priority lanes. BI tenants get weight 10, ETL tenants get weight 1. ETL still gets through, but BI never starves.

> *Customer success no longer Slacks the data team at 9:02 AM saying "the morning report is broken." The morning report is fast, every morning, regardless of what the ETL pipeline is doing.*

---

### Many small files? Not a problem anymore

**Today.** A streaming pipeline can leave you with 100,000 tiny files in a single Iceberg table. Reading that table means 100,000 individual S3 requests. Trino's standard reader doesn't bundle them.

**Coming.** When Shelf sees a query asking for many adjacent ranges, it bundles them into one HTTP request. Same data, fewer round-trips, faster scan. Behind the scenes — you don't have to do anything.

> *The events team's "last 7 days of activity" dashboard, which scanned 73,000 files, used to take 41 seconds. Now it takes 18.*

---

## What we considered and dropped (and why)

We brainstormed 28 features. We're shipping 19. The other 9 we explicitly killed — being honest about it matters:

| Idea we considered | Why we dropped it (in one line) |
|---|---|
| Cryptographic "freshness proof" on every cached byte | Our existing keying already guarantees integrity for free; the extra crypto solves no real problem. |
| Pre-computed `$partitions` answers | Trino itself is fixing this in PR #26737 — would be wasted work. |
| Mirroring the entire data catalog inside Shelf | Apache Polaris already does catalog federation. Wrong project. |
| A custom credential-vending proxy for Glue/Snowflake | The Iceberg REST spec doesn't actually let us do what we'd need. We'd be lying about it working. |
| A parallel S3-delete companion for table maintenance | Iceberg core's `SupportsBulkOperations` already does this. Trino uses it. |
| Cross-AZ replication of the Shelf "pin list" | It's a 1 MB JSON file. S3 cross-region replication does it for free. |
| A dedicated cache pool for vector tables (Lance) | Lance has its own metadata cache. Our normal pool already caches Lance bytes. |
| Auto-pinning tables based on MLflow / W&B run logs | Those APIs don't actually expose what tables a training run read. The signal isn't there. |
| Confidence-gated *auto-applied* recommendations | Cache outages should never start with "the bot decided to..." Humans review the PR. |

## What we're explicitly *not* building

A longer list of "obvious-sounding" features that belong in other projects, would compromise correctness, or live at the wrong layer:

- **A query-result cache** ("remember the answer to this exact SQL"). Belongs in a separate gateway-style project, not in Shelf's data plane.
- **A vector database / full-text search index.** Lance, Pinecone, OpenSearch own these.
- **A new file format.** Parquet and Lance are doing fine.
- **A POSIX filesystem mount.** Mountpoint-S3 owns it.
- **An LLM agent KV-cache or differential-privacy passthrough.** Wrong layer.
- **Snowflake or Athena interception.** Closed clients we can't reach.

If you want any of those, we'll happily point you to the right project. Shelf does one thing — be the fastest, most honest cache for your Iceberg lakehouse — and tries to do nothing else.

---

## How to read this list

If you're a **data analyst**, the things you'll feel directly are §1 and §2 — faster queries, the dollar-saved tile, the advisor's recommendations.

If you're a **data engineer or platform owner**, §3 and §4 are how you'll evaluate, ship, and run it.

If you're an **ML or research engineer** using non-Trino tools, §5 — the multi-engine reach — is what makes Shelf useful in your stack.

If you're an **executive**, the one-line summary is: *we're shipping a cache that makes queries faster, tells you exactly how much money it saved, and gracefully gets out of the way when something breaks.*

We'll mark each section "shipping in v1.0" or "v1.x" once dates firm up. Today (v0.5), the foundation works in production: a cache that's honest about what it does, written in Rust, runs on commodity hardware. Everything above is the next 6–12 months of building on top.
