"""
Declarative mapping from nested Mongo documents to a flat, typed table.
Adding a column = adding one line here.
"""
from typing import Any, Callable, Dict, List, Optional, Tuple

import pyarrow as pa

TS = pa.timestamp("us", tz="UTC")   # Mongo stores UTC - keep the zone explicit instead of silently stripping it


def get_path(doc: Dict[str, Any], path: str) -> Any:
    """Resolve 'a.b.0.c' style paths; missing keys / short arrays return None."""
    cur: Any = doc
    for part in path.split("."):
        if cur is None:
            return None
        if isinstance(cur, list):
            if not part.isdigit() or int(part) >= len(cur):
                return None
            cur = cur[int(part)]
        elif isinstance(cur, dict):
            cur = cur.get(part)
        else:
            return None
    return cur


def _primary_category(doc: Dict[str, Any]) -> Optional[str]:
    for c in doc.get("categories") or []:
        if c.get("primary"):
            return c.get("name")
    return None


def _len(path: str) -> Callable[[Dict[str, Any]], int]:
    return lambda d: len(get_path(d, path) or [])


def _clean_phone(v: Any) -> Optional[str]:
    if v is None:
        return None
    digits = "".join(ch for ch in str(v) if ch.isdigit())
    return digits[-10:] if len(digits) >= 10 else None


# (column, source path OR function, arrow type, optional post-processing)
MAPPING: List[Tuple[str, Any, pa.DataType, Optional[Callable]]] = [
    ("listing_id",            "listing_id",                 pa.string(),  None),
    ("name",             "name",                  pa.string(),  str.strip),
    ("status",           "status",                pa.string(),  str.lower),
    ("city",             "address.city",          pa.string(),  lambda v: v.strip().lower()),
    ("area",             "address.area",          pa.string(),  None),
    ("pincode",          "address.pincode",       pa.string(),  str),
    ("primary_phone",    "contact.phones.0",      pa.string(),  _clean_phone),
    ("phone_count",      _len("contact.phones"),  pa.int32(),   None),
    ("email",            "contact.email",         pa.string(),  lambda v: v.strip().lower()),
    ("primary_category", _primary_category,       pa.string(),  None),
    ("category_count",   _len("categories"),      pa.int32(),   None),
    ("rating_avg",       "rating.avg",            pa.float64(), float),
    ("rating_count",     "rating.count",          pa.int32(),   int),
    ("created_on",       "created_on",            TS,           None),
    ("updated_on",       "updated_on",            TS,           None),
]

ARROW_SCHEMA = pa.schema([pa.field(col, typ) for col, _, typ, _ in MAPPING])
KEY = "listing_id"
PARTITION = "city"
ORDER_COL = "updated_on"


def flatten(doc: Dict[str, Any]) -> Dict[str, Any]:
    row = {}
    for col, src, _, post in MAPPING:
        val = src(doc) if callable(src) else get_path(doc, src)
        if val is not None and post is not None:
            try:
                val = post(val)
            except (TypeError, ValueError, AttributeError):
                val = None          # bad source value -> NULL, never crash the whole batch
        row[col] = val
    if row["city"] is None:
        row["city"] = "unknown"     # partition column must not be NULL
    return row
