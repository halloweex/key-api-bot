"""The column names a ClickHouse `CREATE TABLE` declares.

For the tests that hold the DDL of the tables shipped to ClickHouse
(`core.ch_gold._TABLE_DDL`, `core.ch_silver._SILVER_DDL`) to the columns the
shippers insert and read back by name. A set, read off the statement
whatever its layout, its column order or its types: a comma inside a type's
parentheses (`Decimal(14, 2)`) or inside quotes separates nothing, `--`
comments are dropped, and an INDEX, PROJECTION or CONSTRAINT entry is not a
column.
"""
from __future__ import annotations

from typing import List, Set

_NOT_COLUMNS = {"INDEX", "PROJECTION", "CONSTRAINT", "PRIMARY"}


def _entries(ddl: str) -> List[str]:
    """The comma-separated entries of the column list, at depth zero."""
    i = ddl.index("(", ddl.upper().index("CREATE TABLE")) + 1
    entries: List[str] = []
    current: List[str] = []
    depth, quote = 0, None
    while i < len(ddl):
        ch = ddl[i]
        if quote:
            current.append(ch)
            if ch == "\\" and i + 1 < len(ddl):
                current.append(ddl[i + 1])
                i += 1
            elif ch == quote:
                quote = None
        elif ch in "'\"`":
            quote = ch
            current.append(ch)
        elif ddl.startswith("--", i):
            end = ddl.find("\n", i)
            i = len(ddl) if end < 0 else end
            continue
        elif ch == "(":
            depth += 1
            current.append(ch)
        elif ch == ")":
            if depth == 0:
                entries.append("".join(current))
                return entries
            depth -= 1
            current.append(ch)
        elif ch == "," and depth == 0:
            entries.append("".join(current))
            current = []
        else:
            current.append(ch)
        i += 1
    raise ValueError("the column list is never closed")


def declared_columns(ddl: str) -> Set[str]:
    """Every column name the statement declares."""
    names: Set[str] = set()
    for entry in _entries(ddl):
        entry = entry.strip()
        if not entry:
            continue
        if entry[0] in "`\"":
            names.add(entry[1:entry.index(entry[0], 1)])
            continue
        name = entry.split()[0]
        if name.upper() not in _NOT_COLUMNS:
            names.add(name)
    return names
