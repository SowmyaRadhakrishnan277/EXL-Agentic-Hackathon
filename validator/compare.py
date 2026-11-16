"""
The comparison engine: two pandas DataFrames in, one result dictionary out.

Rules (all configurable, see README.md in this folder):
  * column names are matched case-insensitively; `column_map` renames legacy columns first
  * rows are matched on the key columns, never on row order. Keys are normalised so that
    101, 101.0 and '101' are the same key; NULL keys match NULL keys
  * with no keys the whole row is the key (a multiset comparison)
  * values: NULL = NULL (empty text, '.' and 'NULL' count as NULL); numbers are compared as
    numbers with a tolerance |a - b| <= abs + rel * max(|a|, |b|); dates and timestamps are
    compared as dates; everything else as text (trimmed, case-sensitive by default)
  * also reported: columns present on one side only, duplicate keys, column totals
"""
from __future__ import annotations

import math
import re
import time
from dataclasses import dataclass, field
from datetime import date, datetime
from decimal import ROUND_HALF_UP, Decimal, InvalidOperation
from typing import Any, Optional

import pandas as pd

NULL = "\x00NULL"                 # one shared object: `a is b` when both cells are NULL
SEP = "\x1f"
_NUM = re.compile(r"^[+-]?(?:\d+\.?\d*|\.\d+)(?:[eE][+-]?\d+)?$")
_LEADING_ZERO = re.compile(r"^[+-]?0\d")          # '00123' is an identifier, not the number 123
_ISO = re.compile(r"^(\d{4}-\d{2}-\d{2})(?:[ T](\d{2}:\d{2}:\d{2})(?:\.(\d+))?)?(?:Z|[+-]\d{2}:?\d{2})?$")


class ConfigError(ValueError):
    """The configuration (or the data it points at) cannot be used. Exit code 2."""


@dataclass
class ColumnRule:
    abs: float = 1e-9             # absolute tolerance for numbers
    rel: float = 0.0              # relative tolerance for numbers
    round: Optional[int] = None   # round both sides to this many decimals first (half away from zero)
    trim: bool = True
    case_sensitive: bool = True
    date_format: Optional[str] = None           # e.g. '%d/%m/%Y' for text dates that are not ISO
    null_values: tuple = ("", ".", "null")      # text treated as NULL (compared lower-case, trimmed)


@dataclass
class Rules:
    default: ColumnRule = field(default_factory=ColumnRule)
    columns: dict[str, ColumnRule] = field(default_factory=dict)

    def of(self, column: str) -> ColumnRule:
        return self.columns.get(column, self.default)


def _date_text(v) -> str:
    if getattr(v, "tzinfo", None) is not None:
        v = pd.Timestamp(v).tz_convert("UTC").tz_localize(None)
    if not (v.hour or v.minute or v.second or v.microsecond):
        return v.strftime("%Y-%m-%d")
    frac = f".{v.microsecond:06d}".rstrip("0") if v.microsecond else ""
    return v.strftime("%Y-%m-%d %H:%M:%S") + frac


def canon(v: Any, r: ColumnRule):
    """One cell -> NULL, a Decimal (any number) or a str (text, dates as ISO text)."""
    if v is None or v is pd.NA or v is pd.NaT:
        return NULL
    d: Optional[Decimal] = None
    if isinstance(v, bool) or type(v).__name__ == "bool_":
        return "true" if v else "false"
    if isinstance(v, Decimal):
        if v.is_nan():
            return NULL
        d = v
    elif isinstance(v, int):
        d = Decimal(v)
    elif isinstance(v, float):
        if math.isnan(v):
            return NULL
        d = Decimal(repr(v))
    elif hasattr(v, "item") and not isinstance(v, (str, bytes)):     # numpy scalar
        return canon(v.item(), r)
    elif isinstance(v, datetime):
        return _date_text(v)
    elif isinstance(v, date):
        return v.isoformat()
    else:
        s = str(v)
        t = s.strip()
        if t.lower() in r.null_values:
            return NULL
        if r.trim:
            s = t
        if r.date_format:
            try:
                return _date_text(datetime.strptime(t, r.date_format))
            except ValueError:
                pass
        m = _ISO.match(t)
        if m:
            day, clock, frac = m.groups()
            frac = (frac or "").rstrip("0")
            if not clock or (clock == "00:00:00" and not frac):
                return day
            return f"{day} {clock}" + (f".{frac}" if frac else "")
        if _NUM.match(t) and not _LEADING_ZERO.match(t):
            try:
                d = Decimal(t)
            except InvalidOperation:
                d = None
        if d is None:
            return s if r.case_sensitive else s.casefold()
    if r.round is not None and d.is_finite():
        d = d.quantize(Decimal(1).scaleb(-r.round), rounding=ROUND_HALF_UP)
    return d


def same(a, b, r: ColumnRule) -> bool:
    if a is b or a == b:
        return True
    if isinstance(a, Decimal) and isinstance(b, Decimal):
        return abs(a - b) <= Decimal(str(r.abs)) + Decimal(str(r.rel)) * max(abs(a), abs(b))
    return False


def _text(c) -> str:
    """A canonical cell as text: used in keys and in the report."""
    if c is NULL:
        return "NULL"
    if isinstance(c, Decimal):
        if c == c.to_integral_value():
            return str(int(c))
        return format(c.normalize(), "f")
    return c


def _canon_column(values, rule: ColumnRule) -> list:
    cache: dict = {}
    out = []
    for v in values:
        try:
            k = (type(v), v)
            c = cache.get(k, cache)
            if c is cache:
                c = cache[k] = canon(v, rule)
        except TypeError:                               # unhashable cell
            c = canon(v, rule)
        out.append(c)
    return out


def _prepare(df: pd.DataFrame, side: str, column_map: dict, ignore: set) -> pd.DataFrame:
    df = df.copy()
    df.columns = [str(c).strip().lower() for c in df.columns]
    if column_map:
        df = df.rename(columns=column_map)
    dup = sorted({c for c in df.columns if list(df.columns).count(c) > 1})
    if dup:
        raise ConfigError(f"{side}: column names repeat once case and spaces are ignored: {dup}")
    return df.drop(columns=[c for c in df.columns if c in ignore]).reset_index(drop=True)


def _match_keys(key_text: list[str], signature) -> tuple[list[str], int, int]:
    """Key per row, made unique by an occurrence number so duplicate keys still pair up.
    Returns (unique keys, number of duplicated key values, rows involved)."""
    s = pd.Series(key_text, dtype=object)
    counts = s.map(s.value_counts())
    dup_rows = int((counts > 1).sum())
    if not dup_rows:
        return [k + "#0" for k in key_text], 0, 0
    sig = pd.Series(signature(), dtype=object)
    order = pd.DataFrame({"k": s, "sig": sig}).sort_values(["k", "sig"], kind="stable")
    occ = order.groupby("k").cumcount().reindex(s.index)
    return [f"{k}#{n}" for k, n in zip(key_text, occ)], int(s[counts > 1].nunique()), dup_rows


def compare(name: str, legacy: pd.DataFrame, cloud: pd.DataFrame, keys: Optional[list[str]] = None,
            rules: Optional[Rules] = None, column_map: Optional[dict] = None,
            ignore: Optional[list[str]] = None, max_detail: int = 5000,
            allow_schema_diff: bool = False, allow_duplicate_keys: bool = False,
            max_cells_differing: int = 0, max_rows_missing: int = 0) -> dict:
    """Compare one pair of tables. Never raises for data differences, only for unusable input."""
    t0 = time.time()
    rules = rules or Rules()
    low = lambda d: {str(k).strip().lower(): v for k, v in (d or {}).items()}
    rules = Rules(rules.default, low(rules.columns))
    ignore_set = {str(c).strip().lower() for c in (ignore or [])}
    cmap = {k: str(v).strip().lower() for k, v in low(column_map).items()}
    L = _prepare(legacy, "legacy", cmap, ignore_set)
    C = _prepare(cloud, "cloud", {}, ignore_set)

    common = [c for c in L.columns if c in set(C.columns)]
    only_l = [c for c in L.columns if c not in set(C.columns)]
    only_c = [c for c in C.columns if c not in set(L.columns)]
    if not common:
        raise ConfigError(f"{name}: the two sides have no column in common "
                          f"(legacy: {list(L.columns)[:8]}, cloud: {list(C.columns)[:8]})")
    keys = [str(k).strip().lower() for k in (keys or [])]
    missing = [k for k in keys if k not in common]
    if missing:
        raise ConfigError(f"{name}: key column(s) {missing} are not on both sides (common columns: {common})")
    keyless = not keys
    key_cols = keys or common
    value_cols = [c for c in common if c not in key_cols]

    cl = {c: _canon_column(L[c].tolist(), rules.of(c)) for c in common}
    cc = {c: _canon_column(C[c].tolist(), rules.of(c)) for c in common}

    def key_texts(canon_cols, n):
        parts = [[_text(v) for v in canon_cols[k]] for k in key_cols]
        return [SEP.join(p[i] for p in parts) for i in range(n)]

    def signature(canon_cols, n):
        return lambda: [SEP.join(_text(canon_cols[c][i]) for c in value_cols) for i in range(n)]

    kl, dup_l, dup_rows_l = _match_keys(key_texts(cl, len(L)), signature(cl, len(L)))
    kc, dup_c, dup_rows_c = _match_keys(key_texts(cc, len(C)), signature(cc, len(C)))
    merged = pd.DataFrame({"k": kl, "li": range(len(L))}).merge(
        pd.DataFrame({"k": kc, "ci": range(len(C))}), on="k", how="outer", indicator=True)
    both = merged[merged["_merge"] == "both"]
    li = both["li"].astype(int).tolist()
    ci = both["ci"].astype(int).tolist()
    l_only = merged.loc[merged["_merge"] == "left_only", "li"].astype(int).tolist()
    c_only = merged.loc[merged["_merge"] == "right_only", "ci"].astype(int).tolist()

    def show_key(canon_cols, i):
        return ", ".join(f"{k}={_text(canon_cols[k][i])}" for k in (keys or common[:4]))

    def show_row(df, i):
        return " | ".join(f"{c}={'' if v is None else v}" for c, v in df.iloc[i].items())

    columns, details, bad_rows, cells = [], [], set(), 0
    for c in value_cols:
        r, a, b = rules.of(c), cl[c], cc[c]
        n = 0
        for x, y in zip(li, ci):
            if same(a[x], b[y], r):
                continue
            n += 1
            bad_rows.add(x)
            if len(details) < max_detail:
                va, vb = a[x], b[y]
                diff = _text(va - vb) if isinstance(va, Decimal) and isinstance(vb, Decimal) else ""
                details.append({"check": name, "key": show_key(cl, x), "column": c,
                                "legacy_value": _text(va), "cloud_value": _text(vb), "difference": diff})
        cells += n
        col = {"check": name, "column": c, "rows_compared": len(li), "cells_differing": n,
               "legacy_total": "", "cloud_total": "", "total_difference": ""}
        na = [v for v in a if isinstance(v, Decimal) and v.is_finite()]
        nb = [v for v in b if isinstance(v, Decimal) and v.is_finite()]
        text = any(isinstance(v, str) and v is not NULL for v in a) or any(isinstance(v, str) and v is not NULL for v in b)
        if (na or nb) and not text:
            sa, sb = sum(na, Decimal(0)), sum(nb, Decimal(0))
            col.update(legacy_total=_text(sa), cloud_total=_text(sb), total_difference=_text(sa - sb))
        columns.append(col)

    problems = []
    if (only_l or only_c) and not allow_schema_diff:
        problems.append(f"columns differ (legacy only: {only_l or '-'}, cloud only: {only_c or '-'})")
    if (dup_l or dup_c) and not keyless and not allow_duplicate_keys:
        problems.append(f"duplicate keys (legacy: {dup_l}, cloud: {dup_c})")
    if len(l_only) + len(c_only) > max_rows_missing:
        problems.append(f"{len(l_only)} rows only in legacy, {len(c_only)} rows only in cloud")
    if cells > max_cells_differing:
        problems.append(f"{cells} cells differ in {len(bad_rows)} rows")

    return {
        "check": name, "status": "PASS" if not problems else "FAIL", "problems": problems,
        "keys": keys, "keyless": keyless,
        "legacy_rows": len(L), "cloud_rows": len(C), "matched_rows": len(li),
        "legacy_only_rows": len(l_only), "cloud_only_rows": len(c_only),
        "rows_differing": len(bad_rows), "cells_differing": cells,
        "duplicate_keys_legacy": 0 if keyless else dup_l, "duplicate_keys_cloud": 0 if keyless else dup_c,
        "duplicate_rows_legacy": dup_rows_l, "duplicate_rows_cloud": dup_rows_c,
        "columns_compared": common, "columns_only_in_legacy": only_l, "columns_only_in_cloud": only_c,
        "columns": columns, "mismatches": details,
        "legacy_only": [{"check": name, "key": show_key(cl, i), "row": show_row(L, i)} for i in l_only[:max_detail]],
        "cloud_only": [{"check": name, "key": show_key(cc, i), "row": show_row(C, i)} for i in c_only[:max_detail]],
        "detail_limit": max_detail, "seconds": round(time.time() - t0, 2),
    }
