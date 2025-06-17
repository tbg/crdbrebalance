CockroachDB Index Replica Rebalancer
====================================

Overview
--------

This specialized tool rebalances CockroachDB index replicas during or after index builds.
For a specific index and source store, it brings the replica count for index replicas on that
store close to the mean. It does not explicitly target disk usage.

Quick Start
-----------

1. Set up environment:
   python3 -m venv venv && source venv/bin/activate && pip install -r requirements.txt

2. Run the rebalancer:
   export DB_URL='postgres://root@<host>:<port>/defaultdb?sslmode=...'
   python main.py --table my_table --index my_index

3. Run tests: (needs a valid DB_URL; will create test schema; do not use in production)
   python test_rebalancer.py

Command Line Usage
------------------

python main.py \
  --db-url 'postgres://root@<host>:<port>/defaultdb?sslmode=disable' \
  --table <table_name> \
  --index <index_name> \
  [--num-replicas <count>] \
  [--from-store <store_id>] \
  [--disallowed-stores <store_id1> <store_id2>]

Required:
  --db-url: Database connection string
  --table: Target table name
  --index: Target index name

Optional:
  --num-replicas: Number of replicas to move (auto-detected to balance source with target mean)
  --from-store: Store ID to move ranges from (auto-detected: store with most replicas)
  --disallowed-stores: Store IDs to avoid as targets (auto-detected: top 50% by replica count)

You can override any auto-detected values by explicitly providing the corresponding command line arguments.