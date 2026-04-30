# SHELF-26a — join / subquery / OR predicate extraction

**Status**: shipped (see `src/shelf_replay/predicates.py`, extended test
suite in `tests/test_trace.py` + `tests/test_pipeline.py::test_join_shape_prunes_like_single_table_fact_query`).

## Why

The replay harness extracts a WHERE-clause predicate from each row of
`your_query_log_table` so the E5 scanner can simulate
row-group-level pruning. Pre-SHELF-26a the extractor was conservative
to a fault: any `OR`, `JOIN`, or subquery returned `None`, which meant
the scanner fell through to file granularity and the reported E5 ratio
was pessimistic for a large class of real queries.

This ticket lifts the restriction for four common shapes while keeping
the extractor well inside "clearly safe" territory — we are **not**
building a SQL evaluator; every uncertain conjunct is dropped and every
uncertain predicate falls through to the file-granularity baseline.

## Supported shapes

### 1. Multi-table JOIN with alias-qualified WHERE terms

```sql
SELECT f.x
FROM fact f JOIN dim d ON f.dim_id = d.id
WHERE f.event_date = DATE '2026-04-17'
  AND d.name = 'X'
```

Each `PredicateTerm` now carries an optional `table_alias` (lower-cased,
matching the SQL prefix). The scanner filters terms per `TableRef` at
scan time using the alias-map recorded on `TraceEntry.table_aliases`,
so terms bound to `d` are simply dropped when `fact` is being scanned
and vice-versa.

Terms whose column cannot be bound to any alias in the `FROM` / `JOIN`
list poison the whole predicate (see §"unbound" below).

### 2. Scalar subqueries in the predicate RHS

```sql
WHERE event_date = (SELECT MAX(event_date) FROM audit)
```

The subquery cannot be evaluated from the trace alone. We drop **only
the affected term** and keep AND siblings, instead of the pre-SHELF-26a
behaviour of poisoning the whole predicate.

### 3. Semijoin / `IN (subquery)`

```sql
WHERE region IN (SELECT region FROM active_regions)
```

Same treatment as (2): the semijoin term is dropped, siblings survive.
We conservatively assume the semijoin does not prune on its own, which
is exactly how Trino behaves when the inner relation isn't a constant
set at planning time.

### 4. Top-level OR that collapses to a single-column IN

```sql
WHERE region = 'MP+CG' OR region = 'UP'   -- => region IN ('MP+CG','UP')
WHERE a = 1 OR b = 2                      -- => None (poisons; falls through)
```

We walk the OR tree and accept it only when every branch is a simple
`column = literal` with the **same** `(column, alias)` tuple. Anything
else (OR across columns, a non-equality branch, a branch that itself
contains a subquery) returns `None`.

### 5. CTE references

```sql
WITH x AS (SELECT * FROM t)
SELECT * FROM x WHERE event_date = DATE '2026-04-17'
```

The outer SELECT's WHERE is extracted as usual. We do **not** flatten
the CTE definition — its body is opaque, and the alias map on the
outer SELECT is sufficient for scanner filtering. If the CTE has a
JOIN internally, shape (1) applies only when we're looking at the
outer SELECT.

## Why drop only the affected term, not the lot

The scanner's job is "prune what we can at row-group granularity;
anything we can't reason about stays in". If a conjunct touches a
subquery, dropping just that conjunct leaves the other AND siblings
intact so they can still row-group-prune. Poisoning the whole
predicate (the pre-SHELF-26a behaviour) was a much blunter tool — a
single scalar subquery in a 5-term WHERE collapsed all five terms.

## Shape reference table

| SQL shape | `PredicateTerm` tuple (post-SHELF-26a) |
| --- | --- |
| `WHERE a = 1 AND b < 10` (single-table, bare cols) | `(('a','=',1,None), ('b','<',10,None))` |
| `FROM fact f JOIN dim d ... WHERE f.x = 1 AND d.y = 2` | `(('x','=',1,'f'), ('y','=',2,'d'))` |
| `FROM T AS F WHERE f.region = 'MP'` | `(('region','=','MP','f'),)` |
| `WHERE a = 1 AND b = (SELECT max(c) FROM t)` | `(('a','=',1,None),)` — subquery term dropped |
| `WHERE region IN (SELECT region FROM r) AND d = DATE '..'` | `(('d','=','..',None),)` — semijoin dropped |
| `WHERE region = 'X' OR region = 'Y'` | `(('region','in',('X','Y'),None),)` — OR→IN |
| `WHERE a = 1 OR b = 2` | `None` — OR across columns, poisons |
| `WHERE unknown.col = 1` (prefix not in FROM) | `None` — unbound |
| `WITH x AS (...) SELECT ... FROM x WHERE d = '..'` | `(('d','=','..',None),)` — outer WHERE extracted |
| no `WHERE` | `()` — empty tuple, full scan |

## Out of scope — documented, not implemented

These shapes intentionally fall through to `None` (file granularity).
Implementing them is either a separate ticket or fundamentally
impossible without trace enrichment:

- **Correlated subqueries.** The trace doesn't carry the correlation
  binding, so we cannot evaluate the subquery's LHS reference.
- **Window-function predicates** (`WHERE ROW_NUMBER() OVER (...) = 1`).
- **`UNION` branches.** We'd need to extract a per-branch predicate
  and combine — out of scope here.
- **`NOT IN`, `EXISTS`, `<>` over a constant set.** Easy to add but
  not load-bearing for the current E5 goal.
- **Predicate inference across equijoins**
  (`f.id = d.id AND d.region = 'X'` ⇒ `f.region ∈ {ids for 'X'}`).
  That's a separate, much bigger ticket — it requires joining against
  the dim table's metadata at replay time.

## Contract surface

- `PredicateTerm` (`src/shelf_replay/types.py`) grows a
  `table_alias: Optional[str] = None` field — default `None` keeps
  pre-SHELF-26a callers working; multi-table / alias-qualified queries
  populate it with the lower-cased SQL alias.
- `TraceEntry.table_aliases: tuple[tuple[str, str], ...]` records
  `(alias_lower, base_table_lower)` pairs harvested from the outermost
  SELECT's FROM + JOINs. The scanner uses this to resolve a term's
  `table_alias` to a base table name when filtering per `TableRef`.
- `_extract_predicate`'s return type is
  `tuple[PredicateTerm, ...] | None`; `None` still means "no
  row-group pruning"; empty tuple still means "no WHERE, full scan".
- Every fall-through path in `predicates.py` is an early `return None`
  with a code comment explaining what made the conjunct unsafe — we
  never raise on legal SQL.

## Tests

- `tests/test_trace.py::test_extracts_predicate_from_join_with_fact_table_alias`
- `tests/test_trace.py::test_scalar_subquery_falls_through_only_for_affected_term`
- `tests/test_trace.py::test_in_subquery_falls_through_only_for_affected_term`
- `tests/test_trace.py::test_or_over_same_column_collapses_to_in`
- `tests/test_trace.py::test_or_across_columns_returns_none`
- `tests/test_trace.py::test_cte_predicate_extracted_from_outer_select`
- `tests/test_trace.py::test_unbound_column_returns_none`
- `tests/test_trace.py::test_column_alias_resolution_is_case_insensitive`
- `tests/test_pipeline.py::test_join_shape_prunes_like_single_table_fact_query`
  (end-to-end against the synthetic fixture's new `q-06`)
