"""`_clear_date_range` must clear the whole FK closure, children first.

Why this file exists (t_ddd474f6): the forced-regeneration path
(`POST /{industry}/generator/start {"force": true}`) deletes a window's
generated rows, and it was written before the vendor layer existed. When
`inv.short_ship_events` arrived with a foreign key into `fulfillment.items`
(and `inv.supplier_credit_memos` into it, and `inv.stockout_events` into
`pos.transactions`/`online.orders`), nobody added the new children to the
clear. The result was not a quiet dangle: on any current data dir the delete
of the parent raises `ForeignKeyViolation`, the whole clear rolls back, and the
endpoint answers HTTP 500 — a forced backfill could not run at all.

This test reads the two artefacts and checks them against each other:
  * `grocery/generator/schema.sql` — the FK graph;
  * `base/api/main.py::_clear_date_range` — the order of its deletes.

It needs no database and no container: it is an audit of the statement list.
The live behaviour (which rows go, which survive across the window boundary) is
covered by the probe recorded on the card.

If you add a relation to the clear, add it AFTER its children. If you add an FK
into a relation the clear deletes, add that child to the clear too.
"""
import ast
import re
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[3]
SCHEMA = REPO / "grocery" / "generator" / "schema.sql"
MAIN = REPO / "base" / "api" / "main.py"

# Edges whose dependent rows can carry a timestamp OUTSIDE the window while
# naming a parent INSIDE it (a load created 23:00 delivers next morning; a
# return can refund a sale weeks later). For these, a date-only delete is not
# enough — the statement has to join the parent's own window. Each value is a
# fragment that must appear in that relation's DELETE.
CROSS_WINDOW_JOINS = {
    "inv.supplier_credit_memos": "short_ship_id IN (",
    "inv.short_ship_events": "fulfillment_item_id IN (",
    "inv.receipt_items": "load_id IN (",
    "inv.receipts": "load_id IN (",
    "pos.return_items": "transaction_id IN (",
    "pos.returns": "transaction_id IN (",
    "inv.stockout_events": "pos_transaction_id IN (",
}

TABLE_RE = re.compile(r"CREATE TABLE (\w+\.\w+)\s*\((.*?)\n\);", re.S)
REF_RE = re.compile(r"REFERENCES\s+(\w+\.\w+)\s*\(")


def _fk_graph() -> dict:
    """{parent relation: {child relations}} from schema.sql's REFERENCES."""
    sql = SCHEMA.read_text(encoding="utf-8")
    graph: dict = {}
    for table, body in TABLE_RE.findall(sql):
        for parent in REF_RE.findall(body):
            graph.setdefault(parent, set()).add(table)
    return graph


def _clear_statements() -> list:
    """[(relation, [relations the SQL also names], full SQL text)] in order."""
    tree = ast.parse(MAIN.read_text(encoding="utf-8"))
    func = next(node for node in tree.body
                if isinstance(node, ast.FunctionDef) and node.name == "_clear_date_range")
    assign = None
    for node in ast.walk(func):
        if (isinstance(node, ast.Assign)
                and any(getattr(t, "id", "") == "statements" for t in node.targets)):
            assign = node
            break
    assert assign is not None, "_clear_date_range no longer builds a `statements` list"

    def _consts(node):
        if not isinstance(node, (ast.Tuple, ast.List)):
            return None
        values = []
        for item in node.elts:
            if not isinstance(item, ast.Constant):
                return None
            values.append(item.value)
        return values

    def _text(node, texts):
        if isinstance(node, ast.Constant):
            return "" if node.value is None else str(node.value)
        if isinstance(node, ast.BinOp):
            return _text(node.left, texts) + _text(node.right, texts)
        if isinstance(node, ast.Name):
            return texts.get(node.id, "")
        return ""

    # `requires` is sometimes an inline tuple and sometimes a local name
    # (`receipt_requires`); the SQL is sometimes built from a shared window
    # fragment (`receipt_window`). Resolve both by replaying the local
    # assignments to a fixed point.
    names: dict = {}
    texts: dict = {}
    for _pass in range(3):
        for node in ast.walk(func):
            if not (isinstance(node, ast.Assign) and len(node.targets) == 1
                    and isinstance(node.targets[0], ast.Name)):
                continue
            target = node.targets[0].id
            values = _consts(node.value)
            if values is not None:
                names[target] = values
            text = _text(node.value, texts)
            if text:
                # A name assigned in both branches of the transport probe keeps
                # both variants: the audit asks whether the closure exists at all,
                # not which branch a given data dir takes.
                texts[target] = (texts[target] + "\n" + text) if target in texts else text

    def _requires(node):
        if isinstance(node, ast.Name):
            return names.get(node.id, [])
        return _consts(node) or []

    out = []
    for element in assign.value.elts:
        relation = element.elts[0].value
        sql = _text(element.elts[2], texts)
        assert sql, "the clear's SQL for %s could not be read as literal text" % relation
        out.append((relation, _requires(element.elts[1]), sql))
    return out


def test_statements_are_delete_for_their_own_relation():
    for relation, _requires, sql in _clear_statements():
        assert sql.startswith("DELETE FROM %s" % relation), (
            "%s's statement deletes a different relation: %r" % (relation, sql[:60]))


def test_every_fk_child_of_a_cleared_relation_is_cleared_first():
    graph = _fk_graph()
    order = [relation for relation, _requires, _sql in _clear_statements()]
    index = {rel: i for i, rel in enumerate(order)}

    problems = []
    for parent in order:
        for child in sorted(graph.get(parent, ())):
            if child not in index:
                problems.append(
                    "%s has an FK into %s, which the clear deletes, but %s is not "
                    "in the clear: the delete raises ForeignKeyViolation (or leaves a "
                    "dangling id)" % (child, parent, child))
            elif index[child] > index[parent]:
                problems.append(
                    "%s is deleted after its parent %s (positions %d > %d)"
                    % (child, parent, index[child], index[parent]))
    assert not problems, "\n".join(problems)


def test_cross_window_children_join_the_parents_window():
    by_relation = {rel: sql for rel, _requires, sql in _clear_statements()}
    missing = [rel for rel, fragment in CROSS_WINDOW_JOINS.items()
               if fragment not in by_relation.get(rel, "")]
    assert not missing, (
        "these relations are deleted by their own date only, so a dependent whose "
        "date is past the window would survive a cleared parent: %s" % ", ".join(missing))
