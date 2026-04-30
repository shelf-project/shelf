"""WHERE-clause → :class:`PredicateTerm` extraction for the replay harness.

Factored out of ``trace.py`` as part of SHELF-26a once we lifted the
"any OR / JOIN / subquery poisons the lot" restriction. The public
entry point is :func:`extract_predicate`; the module-level helpers are
kept private so callers don't depend on sqlglot internals.

See ``benchmarks/trino_logs/docs/SHELF-26a-predicate-extraction.md``
for the full shape-by-shape contract (multi-table joins, scalar /
IN subqueries, OR → IN collapse, CTEs) and the out-of-scope list.
"""

from __future__ import annotations

import sqlglot
from sqlglot import exp

from .types import PredicateTerm


def extract_predicate(
    sql: str,
) -> tuple[tuple[PredicateTerm, ...] | None, dict[str, str]]:
    """Extract WHERE conjuncts + alias map from ``sql``.

    Returns ``(terms, alias_map)`` where:

    - ``terms`` is ``None`` when nothing useful survives (callers treat
      ``None`` as "fall through to file granularity");
    - ``terms`` is an empty tuple when the query has no WHERE;
    - ``terms`` is a tuple of :class:`PredicateTerm` otherwise.
    - ``alias_map`` is ``{lowered_alias: lowered_base_table}`` harvested
      from the outermost SELECT's FROM + JOINs. May be empty when the
      SQL does not parse as a SELECT.
    """

    try:
        parsed = sqlglot.parse_one(sql, dialect="trino")
    except sqlglot.errors.ParseError:
        return None, {}
    if not isinstance(parsed, exp.Select):
        return None, {}

    alias_map = _build_alias_map(parsed)
    where = parsed.args.get("where")
    if where is None:
        return tuple(), alias_map

    conjuncts = _flatten_and(where.this)
    terms: list[PredicateTerm] = []
    for c in conjuncts:
        outcome = _term_from(c, alias_map)
        if outcome is _DROP:
            # Scalar / IN-subquery RHS — we cannot evaluate the RHS
            # offline, so we conservatively drop just this conjunct
            # and keep the other AND siblings so they can still
            # row-group-prune.
            continue
        if outcome is None:
            # Any unresolvable conjunct (unbound column prefix, OR
            # that does not collapse to an IN, unsupported op, etc.)
            # poisons the whole predicate — fall through to file
            # granularity at the scanner.
            return None, alias_map
        terms.append(outcome)
    return tuple(terms), alias_map


def _flatten_and(node: exp.Expression) -> list[exp.Expression]:
    if isinstance(node, exp.And):
        return _flatten_and(node.left) + _flatten_and(node.right)
    return [node]


def _flatten_or(node: exp.Expression) -> list[exp.Expression]:
    if isinstance(node, exp.Or):
        return _flatten_or(node.left) + _flatten_or(node.right)
    return [node]


def _build_alias_map(select: exp.Select) -> dict[str, str]:
    """Collect ``{alias_lower: base_table_lower}`` from FROM + JOINs.

    Unaliased tables map to themselves (``fact -> fact``) so that bare
    column references prefixed with the table name still resolve.
    Subquery / derived-table FROM items are skipped — flattening them
    is out of scope (see SHELF-26a design note).
    """

    alias_map: dict[str, str] = {}

    def _register(tbl: exp.Table) -> None:
        raw_name = (tbl.name or "").lower()
        explicit_alias = (tbl.alias or "").lower()
        if explicit_alias:
            alias_map[explicit_alias] = raw_name or explicit_alias
        if raw_name:
            alias_map.setdefault(raw_name, raw_name)

    # sqlglot exposes the FROM node under the ``from_`` key (Select
    # avoids collision with Python's ``from`` keyword).
    from_clause = select.args.get("from_") or select.args.get("from")
    if from_clause is not None:
        for tbl in from_clause.find_all(exp.Table):
            _register(tbl)
    for join in select.args.get("joins") or []:
        for tbl in join.find_all(exp.Table):
            _register(tbl)
    return alias_map


_OP_MAP = {
    exp.EQ: "=",
    exp.NEQ: "!=",
    exp.LT: "<",
    exp.LTE: "<=",
    exp.GT: ">",
    exp.GTE: ">=",
}


# Returned by ``_term_from`` when the conjunct touches a subquery we
# cannot evaluate offline — the caller drops the term but keeps AND
# siblings. ``None`` is reserved for the harder failure mode where the
# whole predicate must fall through.
_DROP = object()
_UNBOUND = object()


def _term_from(
    node: exp.Expression, alias_map: dict[str, str]
) -> "PredicateTerm | object | None":
    cls = type(node)
    op = _OP_MAP.get(cls)
    if op is not None:
        rhs = node.expression
        if isinstance(rhs, (exp.Subquery, exp.Select)):
            # ``col = (SELECT ...)`` — scalar subquery RHS. Drop the
            # term, keep AND siblings.
            return _DROP
        col_info = _col_info(node.this, alias_map)
        if col_info is None:
            return None
        col, alias = col_info
        val = _literal_value(rhs)
        if val is _UNBOUND:
            return None
        return PredicateTerm(column=col, op=op, value=val, table_alias=alias)

    if isinstance(node, exp.In):
        # ``col IN (SELECT ...)`` — semijoin. We conservatively assume
        # the semijoin does not prune and drop the term only; sibling
        # AND conjuncts still row-group-prune.
        if node.args.get("query") is not None:
            return _DROP
        for e in node.expressions:
            if isinstance(e, (exp.Subquery, exp.Select)):
                return _DROP
        col_info = _col_info(node.this, alias_map)
        if col_info is None:
            return None
        col, alias = col_info
        vals: list = []
        for expr in node.expressions:
            v = _literal_value(expr)
            if v is _UNBOUND:
                return None
            vals.append(v)
        if not vals:
            return None
        return PredicateTerm(
            column=col, op="in", value=tuple(vals), table_alias=alias
        )

    if isinstance(node, exp.Or):
        # Collapse ``col = v1 OR col = v2 OR ...`` into a single IN.
        # Anything more complex (OR across columns, OR with a
        # non-equality branch, OR containing a subquery) is not safe
        # to collapse and poisons the whole predicate via ``None``.
        return _collapse_or(node, alias_map)

    return None


def _collapse_or(
    or_node: exp.Or, alias_map: dict[str, str]
) -> "PredicateTerm | None":
    """Collapse a top-level OR to a single ``col IN (...)`` term.

    Conservative: every branch must be ``col = literal`` against the
    same (column, alias) pair. Any mismatch returns ``None`` so the
    caller falls through to file granularity.
    """

    branches = _flatten_or(or_node)
    target_col: str | None = None
    target_alias: str | None = None
    values: list = []
    for branch in branches:
        if not isinstance(branch, exp.EQ):
            return None
        if isinstance(branch.expression, (exp.Subquery, exp.Select)):
            return None
        col_info = _col_info(branch.this, alias_map)
        if col_info is None:
            return None
        col, alias = col_info
        val = _literal_value(branch.expression)
        if val is _UNBOUND:
            return None
        if target_col is None:
            target_col, target_alias = col, alias
        elif col != target_col or alias != target_alias:
            return None
        values.append(val)
    if target_col is None or not values:
        return None
    return PredicateTerm(
        column=target_col, op="in", value=tuple(values), table_alias=target_alias
    )


def _col_info(
    node: exp.Expression | None, alias_map: dict[str, str]
) -> tuple[str, str | None] | None:
    """Return ``(column_lower, alias_lower_or_None)`` for a Column node.

    - Prefixed column (``f.region``) → ``("region", "f")`` when ``f``
      is in the alias map, else ``None`` (unbound — caller poisons
      the predicate).
    - Bare column (``region``) → ``("region", None)``. Downstream
      scanner filtering treats ``alias is None`` as "applies to any
      scanned table", matching pre-SHELF-26a behaviour.
    """

    if not isinstance(node, exp.Column):
        return None
    col = (node.name or "").lower()
    if not col:
        return None
    table_prefix = (node.table or "").lower()
    if table_prefix:
        if table_prefix not in alias_map:
            return None
        return col, table_prefix
    return col, None


def _literal_value(node: exp.Expression | None):
    if node is None:
        return _UNBOUND
    if isinstance(node, exp.Literal):
        if node.is_int:
            return int(node.this)
        if node.is_number:
            return float(node.this)
        return str(node.this)
    if isinstance(node, exp.Boolean):
        return bool(node.this)
    if isinstance(node, exp.Null):
        return None
    # Handle CAST-wrapped literals (``CAST('2026-04-17' AS DATE)`` and
    # sqlglot's ``DATE 'YYYY-MM-DD'`` desugaring) by unwrapping one
    # level of Cast around a Literal.
    if isinstance(node, exp.Cast):
        return _literal_value(node.this)
    return _UNBOUND
