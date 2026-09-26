"""
Parallel extraction from MongoDB.

The collection is split into N `_id` ranges (ObjectIds are time-ordered), and each range is read
by a separate process with its own connection. Every worker streams its range straight to a
Parquet file, so only file paths travel back to the parent process (no big pickled payloads).
"""
import logging
import os
from concurrent.futures import ProcessPoolExecutor, as_completed
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import pyarrow as pa
import pyarrow.parquet as pq
from bson import ObjectId
from pymongo import ASCENDING, DESCENDING, MongoClient

from .schema import ARROW_SCHEMA, flatten

log = logging.getLogger(__name__)
Range = Tuple[Optional[ObjectId], Optional[ObjectId]]


def id_ranges(coll, parts: int) -> List[Range]:
    """Split [min _id, max _id] into `parts` ranges by ObjectId generation time."""
    first = coll.find_one({}, {"_id": 1}, sort=[("_id", ASCENDING)])
    last = coll.find_one({}, {"_id": 1}, sort=[("_id", DESCENDING)])
    if not first:
        return []
    t0 = first["_id"].generation_time.timestamp()
    t1 = last["_id"].generation_time.timestamp() + 1
    step = (t1 - t0) / parts
    cuts = [ObjectId.from_datetime(datetime.fromtimestamp(t0 + i * step, tz=timezone.utc)) for i in range(1, parts)]
    bounds = [None] + cuts + [None]
    return [(bounds[i], bounds[i + 1]) for i in range(parts)]


def _range_filter(base: Dict[str, Any], rng: Range) -> Dict[str, Any]:
    lo, hi = rng
    id_cond = {}
    if lo is not None:
        id_cond["$gte"] = lo
    if hi is not None:
        id_cond["$lt"] = hi
    return {**base, "_id": id_cond} if id_cond else dict(base)


def extract_range(coll, query: Dict[str, Any], rng: Range, out_path: str, batch_size: int = 50_000) -> int:
    """Stream one _id range into a Parquet file. Returns rows written (0 = no file created)."""
    writer, rows, buf = None, 0, []
    cursor = coll.find(_range_filter(query, rng), no_cursor_timeout=False).sort("_id", ASCENDING).batch_size(5_000)
    try:
        for doc in cursor:
            buf.append(flatten(doc))
            if len(buf) >= batch_size:
                writer = _flush(buf, out_path, writer)
                rows += len(buf)
                buf = []
        if buf:
            writer = _flush(buf, out_path, writer)
            rows += len(buf)
    finally:
        cursor.close()
        if writer:
            writer.close()
    return rows


def _flush(buf, out_path, writer):
    table = pa.Table.from_pylist(buf, schema=ARROW_SCHEMA)
    if writer is None:
        writer = pq.ParquetWriter(out_path, ARROW_SCHEMA, compression="zstd")
    writer.write_table(table)
    return writer


def _worker(uri: str, db: str, coll_name: str, query: Dict[str, Any], rng: Range, out_path: str) -> Tuple[str, int]:
    client = MongoClient(uri, serverSelectionTimeoutMS=10_000, socketTimeoutMS=600_000)
    try:
        return out_path, extract_range(client[db][coll_name], query, rng, out_path)
    finally:
        client.close()


def extract_parallel(cfg: Dict[str, Any], query: Dict[str, Any], staging_dir: str, coll=None) -> Tuple[List[str], int]:
    """
    Returns (parquet files, total rows).
    Pass `coll` to run in-process (tests / small collections); otherwise uses `workers` processes.
    """
    os.makedirs(staging_dir, exist_ok=True)
    src = cfg["source"]
    parts = int(src.get("workers", 4))
    local = coll is not None
    if not local:
        coll_client = MongoClient(src["uri"])
        coll = coll_client[src["database"]][src["collection"]]
    ranges = id_ranges(coll, parts)
    jobs = [(rng, os.path.join(staging_dir, f"part-{i:03d}.parquet")) for i, rng in enumerate(ranges)]
    log.info("Extracting with %s ranges, filter=%s", len(jobs), query)

    results = []
    if local:
        results = [(path, extract_range(coll, query, rng, path)) for rng, path in jobs]
    else:
        coll_client.close()
        with ProcessPoolExecutor(max_workers=parts) as pool:
            futures = [pool.submit(_worker, src["uri"], src["database"], src["collection"], query, rng, path)
                       for rng, path in jobs]
            for f in as_completed(futures):
                results.append(f.result())      # any worker failure fails the run -> watermark not moved

    files = [p for p, n in results if n > 0]
    total = sum(n for _, n in results)
    log.info("Extracted %s rows into %s files", total, len(files))
    return files, total
