#!/usr/bin/env python3
"""
Dummy business-listing documents for MongoDB.

  --mode seed   : insert N documents
  --mode churn  : update some, flip some active <-> inactive, insert new ones
"""
import argparse
import random
from datetime import datetime, timedelta, timezone

CITIES = ["Mumbai", "Pune", "Delhi", "Bangalore", "Hyderabad", "Chennai", "Kolkata", "Ahmedabad"]
AREAS = ["Andheri", "Kothrud", "Saket", "Indiranagar", "Gachibowli", "Adyar", "Salt Lake", "Navrangpura"]
CATS = ["Restaurants", "Doctors", "Plumbers", "Hotels", "Gyms", "Schools", "Electricians", "Salons"]


def make_doc(i: int, ts: datetime) -> dict:
    """Nested document with the kind of variety real collections have (optional fields, arrays)."""
    rnd = random.Random(i)
    city = rnd.choice(CITIES)
    cats = rnd.sample(CATS, rnd.randint(1, 3))
    doc = {
        "listing_id": f"B{i:08d}",
        "name": f"Business {i}",
        "status": "active" if rnd.random() < 0.8 else "inactive",
        "address": {"city": city, "area": rnd.choice(AREAS), "pincode": rnd.randint(110001, 700099)},
        "contact": {"phones": [f"+91 9{rnd.randint(100000000, 999999999)}" for _ in range(rnd.randint(0, 3))]},
        "categories": [{"name": c, "primary": n == 0} for n, c in enumerate(cats)],
        "rating": {"avg": round(rnd.uniform(1, 5), 1), "count": rnd.randint(0, 5000)},
        "created_on": ts - timedelta(days=rnd.randint(1, 900)),
        "updated_on": ts,
    }
    if rnd.random() < 0.6:
        doc["contact"]["email"] = f"contact{i}@example.com"
    return doc


def main():
    from pymongo import MongoClient

    p = argparse.ArgumentParser()
    p.add_argument("--uri", default="mongodb://localhost:27017")
    p.add_argument("--mode", choices=["seed", "churn"], required=True)
    p.add_argument("--rows", type=int, default=200_000)
    p.add_argument("--updates", type=int, default=5_000)
    p.add_argument("--flips", type=int, default=500)
    p.add_argument("--inserts", type=int, default=1_000)
    args = p.parse_args()

    coll = MongoClient(args.uri).demo.businesses
    now = datetime.now(timezone.utc)

    if args.mode == "seed":
        coll.create_index("listing_id", unique=True)
        coll.create_index("updated_on")                    # incremental pulls use this index
        for start in range(0, args.rows, 10_000):
            coll.insert_many([make_doc(i, now) for i in range(start + 1, min(start + 10_000, args.rows) + 1)])
        print(f"Seeded {args.rows} documents")
        return

    n = coll.estimated_document_count()
    for i in random.sample(range(1, n + 1), args.updates):
        coll.update_one({"listing_id": f"B{i:08d}"}, {"$set": {"rating.count": random.randint(0, 9000), "updated_on": now}})
    for i in random.sample(range(1, n + 1), args.flips):
        d = coll.find_one({"listing_id": f"B{i:08d}"}, {"status": 1})
        new = "inactive" if d and d["status"] == "active" else "active"
        coll.update_one({"listing_id": f"B{i:08d}"}, {"$set": {"status": new, "updated_on": now}})
    coll.insert_many([make_doc(i, now) for i in range(n + 1, n + args.inserts + 1)])
    print(f"Churn: {args.updates} updates, {args.flips} status flips, {args.inserts} inserts")


if __name__ == "__main__":
    main()
