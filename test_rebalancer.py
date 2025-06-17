import os
import pytest
import psycopg2
from main import CockroachRebalancer
# DB_URL needs to be set to a pg connection string. Here's an example cluster:
# export c=$(whoami)-test
# roachprod create -n 7 --gce-use-spot --gce-enable-multiple-stores --local-ssd=true \
#   --gce-local-ssd-count=4 $c
# roachprod stage $c cockroach
# roachprod start --store-count 4 $c
# roachprod pgurl $c:1 --external --database defaultdb

def setup_test_environment(db_url: str):
    """
The test creates two tables (kvs with ~200 ranges, other_table with ~1000 ranges) to
validate that operations are properly scoped to the target index only. It moves 15
replicas and verifies correct behavior.

To set up a test cluster with roachprod:
export c=$(whoami)-test
roachprod create -n 7 --gce-enable-multiple-stores --gce-pd-volume-count 4 $c
roachprod stage $c cockroach && roachprod start --store-count 4 $c
    """
    with psycopg2.connect(db_url) as conn:
        with conn.cursor() as cur:
            # Drop table if exists
            cur.execute("DROP TABLE IF EXISTS kvs CASCADE;")
            conn.commit()
            # Create table
            cur.execute("""
                CREATE TABLE IF NOT EXISTS kvs (
                    key INT PRIMARY KEY,
                    v1  INT,
                    v2  INT
                );
            """)
            # Insert test data
            cur.execute("""
                INSERT INTO kvs
                SELECT
                    i,
                    (random()*2147483647)::INT,
                    (random()*2147483647)::INT
                FROM generate_series(1, 10000) AS g(i);
            """)
            # Create index
            cur.execute("CREATE INDEX kvs_v1_idx ON kvs (v1);")
            # Split ranges
            cur.execute("""
                ALTER INDEX kvs@kvs_v1_idx SPLIT AT
                SELECT g FROM generate_series(5000000, 995000000, 5000000) AS g;
            """)
            conn.commit()

            # --- Add other_table with 1000 ranges ---
            cur.execute("DROP TABLE IF EXISTS other_table CASCADE;")
            conn.commit()
            cur.execute("""
                CREATE TABLE IF NOT EXISTS other_table (
                    key INT PRIMARY KEY,
                    v1  INT,
                    v2  INT
                );
            """)
            cur.execute("""
                INSERT INTO other_table
                SELECT
                    i,
                    (random()*2147483647)::INT,
                    (random()*2147483647)::INT
                FROM generate_series(1, 10000) AS g(i);
            """)
            cur.execute("CREATE INDEX IF NOT EXISTS other_table_v1_idx ON other_table (v1);")
            # Split other_table into 1000 ranges
            cur.execute("""
                ALTER INDEX other_table@other_table_v1_idx SPLIT AT
                SELECT g FROM generate_series(10000, 10000000, 10000) AS g;
            """)
            conn.commit()

def test_rebalancing():
    """Test the rebalancing functionality with full auto-detection."""
    db_url = os.getenv('DB_URL')
    if not db_url:
        pytest.skip("DB_URL environment variable is not set")

    print("Setting up test tables...")
    setup_test_environment(db_url)

    rebalancer = CockroachRebalancer(db_url)
    rebalancer.run_rebalancer("kvs", "kvs_v1_idx")

if __name__ == "__main__":
    test_rebalancing()