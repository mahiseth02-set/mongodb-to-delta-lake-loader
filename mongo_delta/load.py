"""Delta Lake writes with delta-rs (no Spark / JVM needed)."""
import logging
from typing import List

import pyarrow as pa
import pyarrow.compute as pc
from deltalake import DeltaTable, write_deltalake
from deltalake.exceptions import TableNotFoundError

from .schema import KEY, ORDER_COL, PARTITION

log = logging.getLogger(__name__)


def latest_per_key(table: pa.Table) -> pa.Table:
    """Keep the newest version of each key (Delta MERGE fails if one target row matches many source rows)."""
    if table.num_rows == 0:
        return table
    table = table.sort_by([(KEY, "ascending"), (ORDER_COL, "descending")])
    keys = table.column(KEY).to_pylist()
    keep = [i for i in range(len(keys)) if i == 0 or keys[i] != keys[i - 1]]
    return table.take(pa.array(keep))


def _open(path: str):
    try:
        return DeltaTable(path)
    except TableNotFoundError:
        return None


def upsert(path: str, data: pa.Table) -> dict:
    """Insert new keys, update existing keys only if the incoming row is newer."""
    if data.num_rows == 0:
        return {"inserted": 0, "updated": 0}
    dt = _open(path)
    if dt is None:
        write_deltalake(path, data, partition_by=[PARTITION], mode="overwrite")
        return {"inserted": data.num_rows, "updated": 0}
    metrics = (dt.merge(source=data, predicate=f"t.{KEY} = s.{KEY}", source_alias="s", target_alias="t")
                 .when_matched_update_all(predicate=f"s.{ORDER_COL} >= t.{ORDER_COL}")
                 .when_not_matched_insert_all()
                 .execute())
    return {"inserted": metrics.get("num_target_rows_inserted", 0), "updated": metrics.get("num_target_rows_updated", 0)}


def delete_keys(path: str, keys: List[str]) -> int:
    """Remove keys that moved to the other table (e.g. active -> inactive)."""
    dt = _open(path)
    if dt is None or not keys:
        return 0
    src = pa.table({KEY: pa.array(keys, pa.string())})
    metrics = (dt.merge(source=src, predicate=f"t.{KEY} = s.{KEY}", source_alias="s", target_alias="t")
                 .when_matched_delete()
                 .execute())
    return metrics.get("num_target_rows_deleted", 0)


def split_by_status(table: pa.Table):
    is_active = pc.equal(table.column("status"), "active")
    return table.filter(is_active), table.filter(pc.invert(pc.fill_null(is_active, False)))


def compact(path: str, retention_hours: int = 168) -> None:
    """Merge small files and remove old, unreferenced ones."""
    dt = _open(path)
    if dt is None:
        return
    dt.optimize.compact()
    dt.vacuum(retention_hours=retention_hours, enforce_retention_duration=False, dry_run=False)
