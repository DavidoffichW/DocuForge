from __future__ import annotations

from typing import List, Sequence


def _norm_text(v: object) -> str:
    if v is None:
        return ""
    if not isinstance(v, str):
        v = str(v)
    s = v.strip()
    if not s:
        return ""
    return " ".join(s.split())


def normalize_grid(grid: Sequence[Sequence[object]]) -> List[List[str]]:
    if grid is None:
        raise ValueError("grid must not be None")
    rows: List[List[str]] = []
    for r in grid:
        if r is None:
            rows.append([])
            continue
        rows.append([_norm_text(c) for c in list(r)])

    max_cols = 0
    for r in rows:
        if len(r) > max_cols:
            max_cols = len(r)

    out: List[List[str]] = []
    for r in rows:
        if len(r) < max_cols:
            r = list(r) + [""] * (max_cols - len(r))
        out.append(list(r))

    return out