#!/usr/bin/env python3
import os
import time
from pymongo import MongoClient
import psycopg2
from psycopg2.extras import execute_values

# --- Connection strings ---
MONGO_URI = os.environ.get("MONGO_URI", "mongodb://mongo:27017/?replicaSet=rs0")
PG_DSN = os.environ.get(
    "PG_DSN",
    "host=airflow_db port=5432 dbname=nomba user=airflow password=airflow"
)

# --- Mongo setup ---
mongo = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
db = mongo['warehouse']  # Mongo database
collection = db['stg_mongo_user']

# --- Postgres setup ---
pg = psycopg2.connect(PG_DSN)
pg.autocommit = True

# --- Ensure target table exists ---
CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS analytics.stg_mongo_user (
    source_id TEXT PRIMARY KEY,
    first_name TEXT,
    last_name TEXT,
    occupation TEXT,
    state TEXT,
    updated_at TIMESTAMP,
    deleted_at TIMESTAMP
);
"""
with pg.cursor() as cur:
    cur.execute(CREATE_TABLE_SQL)
print("✅ Ensured Postgres table analytics.stg_mongo_user exists.")

# --- Upsert query ---
UPSERT_SQL = """
INSERT INTO analytics.stg_mongo_user (source_id, first_name, last_name, occupation, state, updated_at)
VALUES %s
ON CONFLICT (source_id) DO UPDATE SET
  first_name = EXCLUDED.first_name,
  last_name = EXCLUDED.last_name,
  occupation = EXCLUDED.occupation,
  state = EXCLUDED.state,
  updated_at = EXCLUDED.updated_at
WHERE analytics.stg_mongo_user.updated_at <= EXCLUDED.updated_at
"""

# --- Change processor ---
def process_change(change):
    op = change.get('operationType')

    if op == 'delete':
        doc_id = str(change['documentKey']['_id'])
        with pg.cursor() as cur:
            cur.execute(
                "UPDATE analytics.stg_mongo_user SET deleted_at = now() WHERE source_id = %s",
                (doc_id,)
            )
        print(f"[DELETE] marked deleted: {doc_id}")
        return

    full = change.get('fullDocument')
    if not full:
        return

    source_id = str(full.get('_id'))
    vals = (source_id,
            full.get('firstName'),
            full.get('lastName'),
            full.get('occupation'),
            full.get('state'),
            full.get('updated_at') or time.strftime('%Y-%m-%d %H:%M:%S'))

    with pg.cursor() as cur:
        execute_values(cur, UPSERT_SQL, [vals])
    print(f"[UPSERT] {source_id}")

# --- Main CDC consumer ---
def main():
    print("🚀 Starting Mongo → Postgres CDC consumer...")
    with collection.watch(full_document='updateLookup') as stream:
        for change in stream:
            try:
                process_change(change)
            except Exception as e:
                print("❌ Error:", e)

if __name__ == "__main__":
    main()

