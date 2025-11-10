-- sql/ddl.sql
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- staging for Mongo user
CREATE TABLE IF NOT EXISTS stg_mongo_user (
  source_id text PRIMARY KEY,
  first_name text,
  last_name text,
  occupation text,
  state text,
  updated_at timestamptz,
  deleted_at timestamptz
);

-- staging for savings transactions (from Postgres source)
CREATE TABLE IF NOT EXISTS stg_savings_transaction (
  txn_id text PRIMARY KEY,
  plan_id text,
  amount numeric,
  currency text,
  side text,
  rate numeric,
  txn_timestamp timestamptz,
  updated_at timestamptz,
  deleted_at timestamptz
);
