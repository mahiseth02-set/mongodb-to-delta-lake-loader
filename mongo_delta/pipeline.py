#!/usr/bin/env python3
"""
MongoDB -> Delta Lake loader.

  full         : whole collection
  incremental  : documents with updated_on > last watermark - overlap
  compact      : optimize + vacuum both Delta tables

Documents are routed to two Delta tables by status (active / inactive). When a document
changes status it is upserted into its new table and deleted from the old one.
"""
import argparse
import json
import logging
import os
import shutil
import sys
import time
from datetime import datetime, timedelta, timezone

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from mongo_delta.extract import extract_parallel  # noqa: E402
from mongo_delta.load import compact, delete_keys, latest_per_key, split_by_status, upsert  # noqa: E402
from mongo_delta.schema import ARROW_SCHEMA, KEY, ORDER_COL  # noqa: E402

log = logging.getLogger("mongo_delta")


class Watermark:
    def __init__(self, path):
        self.path = path

    def read(self):
        if not os.path.exists(self.path):
            return None
        with open(self.path) as f:
            return datetime.fromisoformat(json.load(f)["last_watermark"])

    def write(self, ts, rows):
        os.makedirs(os.path.dirname(os.path.abspath(self.path)), exist_ok=True)
        tmp = self.path + ".tmp"
        with open(tmp, "w") as f:
            json.dump({"last_watermark": ts.isoformat(), "rows_last_run": rows,
                       "run_at": datetime.now(timezone.utc).isoformat()}, f, indent=2)
        os.replace(tmp, self.path)


def run(cfg: dict, mode: str, coll=None) -> dict:
    tgt = cfg["target"]
    wm = Watermark(cfg["state_path"])
    t0 = time.time()

    if mode == "compact":
        compact(tgt["active_path"]); compact(tgt["inactive_path"])
        return {"mode": mode}

    query = {}
    if mode == "incremental":
        last = wm.read()
        if last is None:
            raise SystemExit("No watermark - run --mode full first")
        query = {ORDER_COL: {"$gt": last - timedelta(minutes=cfg.get("overlap_minutes", 5))}}

    staging = os.path.join(cfg["staging_dir"], datetime.now().strftime("run_%Y%m%d_%H%M%S_%f"))
    try:
        files, extracted = extract_parallel(cfg, query, staging, coll=coll)
        if extracted == 0:
            log.info("No changes")
            return {"mode": mode, "extracted": 0}

        batch = latest_per_key(pa.concat_tables([pq.read_table(f) for f in files]).cast(ARROW_SCHEMA))
        new_wm = pc.max(batch.column(ORDER_COL)).as_py()
        active, inactive = split_by_status(batch)

        if mode == "full":
            shutil.rmtree(tgt["active_path"], ignore_errors=True)
            shutil.rmtree(tgt["inactive_path"], ignore_errors=True)

        stats = {
            "mode": mode,
            "extracted": extracted,
            "active": upsert(tgt["active_path"], active),
            "inactive": upsert(tgt["inactive_path"], inactive),
            # status flips: remove the key from the table it no longer belongs to
            "moved_to_inactive": delete_keys(tgt["active_path"], inactive.column(KEY).to_pylist()),
            "moved_to_active": delete_keys(tgt["inactive_path"], active.column(KEY).to_pylist()),
        }
        wm.write(new_wm, extracted)           # only after both tables are written
        stats["seconds"] = round(time.time() - t0, 1)
        log.info("Done: %s", stats)
        return stats
    finally:
        shutil.rmtree(staging, ignore_errors=True)


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--config", default="config/pipeline.json")
    p.add_argument("--mode", choices=["full", "incremental", "compact"], default="incremental")
    args = p.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s - %(message)s")
    with open(args.config) as f:
        cfg = json.load(f)
    cfg["source"]["uri"] = os.environ.get("MONGO_URI", cfg["source"]["uri"])
    run(cfg, args.mode)


if __name__ == "__main__":
    main()
