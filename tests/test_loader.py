import os
import sys
from datetime import datetime, timedelta, timezone

import mongomock
import pytest
from deltalake import DeltaTable

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from mongo_delta.pipeline import run  # noqa: E402
from mongo_delta.schema import flatten, get_path  # noqa: E402
from scripts.generate_data import make_doc  # noqa: E402

T0 = datetime(2026, 9, 1, tzinfo=timezone.utc)


@pytest.fixture
def env(tmp_path):
    coll = mongomock.MongoClient().db.businesses
    cfg = {
        "source": {"workers": 3},
        "target": {"active_path": str(tmp_path / "active"), "inactive_path": str(tmp_path / "inactive")},
        "staging_dir": str(tmp_path / "staging"),
        "state_path": str(tmp_path / "state" / "wm.json"),
        "overlap_minutes": 5,
    }
    return coll, cfg


def _count(path, **filters):
    df = DeltaTable(path).to_pandas()
    for k, v in filters.items():
        df = df[df[k] == v]
    return len(df)


def test_get_path_handles_arrays_and_missing():
    doc = {"a": {"b": [{"c": 1}]}}
    assert get_path(doc, "a.b.0.c") == 1
    assert get_path(doc, "a.b.5.c") is None
    assert get_path(doc, "x.y") is None


def test_flatten_cleans_and_never_crashes():
    doc = make_doc(1, T0)
    doc["contact"]["phones"] = ["+91 98200-12345", "022 1234"]
    doc["rating"]["avg"] = "not-a-number"
    doc["address"]["city"] = "  Mumbai "
    row = flatten(doc)
    assert row["primary_phone"] == "9820012345"
    assert row["phone_count"] == 2
    assert row["rating_avg"] is None          # bad value -> NULL, not an exception
    assert row["city"] == "mumbai"


def test_full_then_incremental_with_status_flip(env):
    coll, cfg = env
    coll.insert_many([make_doc(i, T0 + timedelta(minutes=i)) for i in range(1, 301)])

    stats = run(cfg, "full", coll=coll)
    assert stats["extracted"] == 300
    total_active = _count(cfg["target"]["active_path"])
    total_inactive = _count(cfg["target"]["inactive_path"])
    assert total_active + total_inactive == 300

    # One active doc becomes inactive, one doc gets a new name, one new doc arrives
    later = T0 + timedelta(days=1)
    flip = coll.find_one({"status": "active"})
    coll.update_one({"_id": flip["_id"]}, {"$set": {"status": "inactive", "updated_on": later}})
    renamed = coll.find_one({"status": "active"})
    coll.update_one({"_id": renamed["_id"]}, {"$set": {"name": "Renamed Store", "updated_on": later}})
    coll.insert_one(make_doc(999, later))

    stats = run(cfg, "incremental", coll=coll)
    assert stats["moved_to_inactive"] == 1
    assert _count(cfg["target"]["active_path"], listing_id=flip["listing_id"]) == 0
    assert _count(cfg["target"]["inactive_path"], listing_id=flip["listing_id"]) == 1
    assert _count(cfg["target"]["active_path"], listing_id=renamed["listing_id"], name="Renamed Store") == 1
    assert (_count(cfg["target"]["active_path"]) + _count(cfg["target"]["inactive_path"])) == 301


def test_incremental_rerun_is_idempotent(env):
    coll, cfg = env
    coll.insert_many([make_doc(i, T0) for i in range(1, 51)])
    run(cfg, "full", coll=coll)
    before = _count(cfg["target"]["active_path"]) + _count(cfg["target"]["inactive_path"])
    run(cfg, "incremental", coll=coll)      # overlap window re-reads the same docs
    run(cfg, "incremental", coll=coll)
    after = _count(cfg["target"]["active_path"]) + _count(cfg["target"]["inactive_path"])
    assert before == after == 50
