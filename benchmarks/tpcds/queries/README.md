# TPC-DS query bodies

The 99 canonical TPC-DS queries are **not** committed here; they
are pulled verbatim at harness-bootstrap time from
[`trinodb/trino`](https://github.com/trinodb/trino/tree/master/plugin/trino-tpcds/src/main/resources/tpcds)
so shelf's copy never drifts from upstream.

`bootstrap.sh` fetches the queries into this directory at runtime.
CI and local operators both run it before the first harness
invocation.

Each `qNN.sql` must expand exactly one `WITH`-less or single-`WITH`
Trino statement. Any query that needs Trino-side session setup
(e.g. `SET SESSION`) belongs in the runner, not the .sql file.

### Why not commit them

- The TPC-DS queries are already public; re-hosting them creates
  sync drift and a licence-attribution footgun.
- `bootstrap.sh` pins a specific upstream commit so the suite is
  reproducible at the byte level.
