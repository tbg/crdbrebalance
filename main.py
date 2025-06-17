import argparse
import psycopg2
import random
from typing import List, Set, Dict, Optional
from dataclasses import dataclass

@dataclass
class StoreInfo:
    """Information about a store including its node."""
    store_id: int
    node_id: int
    replica_count: int

@dataclass
class RebalanceConfig:
    """Configuration for the rebalancing operation."""
    from_store: int
    num_replicas: int
    table_name: str
    index_name: str
    disallowed_stores: Set[int]
    target_stores: List[int]
    store_info_map: Dict[int, StoreInfo]

class CockroachRebalancer:
    def __init__(self, db_url: str):
        """Initialize the rebalancer with database connection."""
        self.db_url = db_url

    def get_store_replica_counts(self, table_name: str, index_name: str) -> Dict[int, StoreInfo]:
        """Get the number of replicas per store for the specified index, including node information.

        Note: We query ALL stores from kv_store_status, not just those with current replicas,
        because during rebalancing we may encounter replicas on stores that don't currently
        have any replicas for this index.
        """
        with psycopg2.connect(self.db_url) as conn:
            with conn.cursor() as cur:
                # Get ALL stores and their node mappings from the cluster
                cur.execute("SELECT store_id, node_id FROM crdb_internal.kv_store_status")
                store_to_node = {row[0]: row[1] for row in cur.fetchall()}

                if not store_to_node:
                    raise ValueError("No stores found in cluster - cannot proceed")

                # Get replica counts per store for this specific index
                sql = f"""
                    WITH
                    table_info AS (
                      SELECT table_id
                      FROM crdb_internal.tables
                      WHERE name = '{table_name}'
                      AND state = 'PUBLIC'
                    ),
                    descriptor_json AS (
                      SELECT crdb_internal.pb_to_json('cockroach.sql.sqlbase.Descriptor', descriptor) AS descriptor_json
                      FROM system.descriptor
                      WHERE id = (SELECT table_id FROM table_info)
                    ),
                    live_indexes AS (
                      SELECT
                        (json_array_elements(descriptor_json -> 'table' -> 'indexes') ->> 'id')::INT AS index_id,
                        json_array_elements(descriptor_json -> 'table' -> 'indexes') ->> 'name' AS index_name
                      FROM descriptor_json
                    ),
                    mutating_indexes AS (
                      SELECT
                        (m -> 'index' ->> 'id')::INT AS index_id,
                        m -> 'index' ->> 'name' AS index_name
                      FROM (
                        SELECT json_array_elements(descriptor_json -> 'table' -> 'mutations') AS m
                        FROM descriptor_json
                      ) AS sub
                      WHERE m ? 'index'
                    ),
                    target_index AS (
                      SELECT index_id FROM (
                        SELECT * FROM live_indexes
                        UNION ALL
                        SELECT * FROM mutating_indexes
                      ) AS all_indexes
                      WHERE index_name = '{index_name}'
                    ),
                    target_ranges AS (
                      SELECT range_id, replicas
                      FROM crdb_internal.ranges
                      WHERE crdb_internal.pretty_key(start_key, 0) LIKE
                            '/' || (SELECT table_id FROM table_info)::STRING || '/' || (SELECT index_id FROM target_index)::STRING || '%'
                    )
                    SELECT
                      store_id,
                      count(*) AS num_replicas
                    FROM target_ranges,
                         unnest(replicas) AS store_id
                    GROUP BY store_id
                    ORDER BY num_replicas DESC;
                """
                cur.execute(sql)
                replica_rows = cur.fetchall()

                if not replica_rows:
                    raise ValueError(f"No replicas found for {table_name}@{index_name}")

                # Build replica count mapping
                replica_counts = {row[0]: row[1] for row in replica_rows}

                # Create result with ALL stores, assigning zero replicas to stores without current replicas
                result = {}
                for store_id, node_id in store_to_node.items():
                    replica_count = replica_counts.get(store_id, 0)
                    result[store_id] = StoreInfo(store_id=store_id, node_id=node_id, replica_count=replica_count)

                return result

    def auto_detect_stores_and_from_store(self, table_name: str, index_name: str) -> tuple[int, Set[int], List[int], Dict[int, StoreInfo]]:
        """Auto-detect from_store, disallowed stores and valid target stores.

        Returns:
            Tuple of (from_store, disallowed_stores, valid_target_stores, replica_counts)
            - from_store: Store with the most replicas
            - disallowed_stores: Top 50% of stores by replica count
            - valid_target_stores: Bottom 50% of stores by replica count
            - replica_counts: Dict mapping store_id to replica count
        """
        replica_counts = self.get_store_replica_counts(table_name, index_name)
        if not replica_counts:
            raise ValueError(f"No replicas found for {table_name}@{index_name}")

        # Sort stores by replica count (descending)
        sorted_stores = sorted(replica_counts.items(), key=lambda x: x[1].replica_count, reverse=True)

        # First store has the most replicas
        from_store = sorted_stores[0][0]

        # Split in half: top 50% are disallowed, bottom 50% are valid targets
        split_point = len(sorted_stores) // 2

        disallowed_stores = {store_id for store_id, _ in sorted_stores[:split_point]}
        valid_target_stores = [store_id for store_id, _ in sorted_stores[split_point:]]

        return from_store, disallowed_stores, valid_target_stores, replica_counts

    def auto_detect_num_replicas(self, from_store: int, valid_target_stores: List[int], replica_counts: Dict[int, StoreInfo]) -> int:
        """Auto-detect the number of replicas to move.

        Calculates how many replicas to move from from_store so that it ends up
        with approximately the same number of replicas as the mean across all stores.

        Args:
            from_store: Store to move replicas from
            valid_target_stores: List of allowed target stores
            replica_counts: Dict mapping store_id to replica count

        Returns:
            Number of replicas to move (minimum 0, maximum available on from_store)
        """
        if from_store not in replica_counts:
            raise ValueError(f"from_store {from_store} not found in replica counts")

        current_from_count = replica_counts[from_store].replica_count
        if current_from_count == 0:
            raise ValueError(f"from_store {from_store} has no replicas to move")

        if not replica_counts:
            raise ValueError("No replica data available - cannot calculate num_replicas")

        # Calculate mean replica count across all stores
        total_replicas = sum(store_info.replica_count for store_info in replica_counts.values())
        total_stores = len(replica_counts)
        mean_replica_count = total_replicas // total_stores

        # Calculate how many to move so from_store ends up at mean level
        # We want: current_from_count - num_to_move ≈ mean_replica_count
        num_to_move = current_from_count - mean_replica_count

        # Ensure we don't move more than available or less than 0
        num_to_move = max(0, min(num_to_move, current_from_count))

        return num_to_move

    def get_ranges_to_move(self, config: RebalanceConfig) -> Dict[int, List[int]]:
        """Get ranges that need to be moved from the source store.

        Returns:
            Dict mapping range_id to list of replica store IDs.
        """
        with psycopg2.connect(self.db_url) as conn:
            with conn.cursor() as cur:
                sql = f"""
                    WITH
                    table_info AS (
                      SELECT table_id FROM crdb_internal.tables WHERE name = '{config.table_name}' AND state = 'PUBLIC'
                    ),
                    descriptor_json AS (
                      SELECT crdb_internal.pb_to_json('cockroach.sql.sqlbase.Descriptor', descriptor) AS descriptor_json
                      FROM system.descriptor
                      WHERE id = (SELECT table_id FROM table_info)
                    ),
                    live_indexes AS (
                      SELECT
                        (json_array_elements(descriptor_json -> 'table' -> 'indexes') ->> 'id')::INT AS index_id,
                        json_array_elements(descriptor_json -> 'table' -> 'indexes') ->> 'name' AS index_name
                      FROM descriptor_json
                    ),
                    mutating_indexes AS (
                      SELECT
                        (m -> 'index' ->> 'id')::INT AS index_id,
                        m -> 'index' ->> 'name' AS index_name
                      FROM (
                        SELECT json_array_elements(descriptor_json -> 'table' -> 'mutations') AS m
                        FROM descriptor_json
                      ) AS sub
                      WHERE m ? 'index'
                    ),
                    target_index AS (
                      SELECT index_id FROM (
                        SELECT * FROM live_indexes
                        UNION ALL
                        SELECT * FROM mutating_indexes
                      ) AS all_indexes
                      WHERE index_name = '{config.index_name}'
                    ),
                    target_ranges AS (
                      SELECT range_id, replicas
                      FROM crdb_internal.ranges
                      WHERE crdb_internal.pretty_key(start_key, 0) LIKE
                            '/' || (SELECT table_id FROM table_info)::STRING || '/' || (SELECT index_id FROM target_index)::STRING || '%'
                        AND {config.from_store} = ANY (replicas)
                    )
                    SELECT range_id, replicas
                    FROM target_ranges
                    LIMIT {config.num_replicas}
                """
                cur.execute(sql)
                return {row[0]: row[1] for row in cur.fetchall()}

    def find_best_target_store(self, existing_replicas: List[int], target_stores: List[int], store_info_map: Dict[int, StoreInfo]) -> int:
        """Find the best target store for a replica, avoiding stores that already have replicas and preferring different nodes."""

        # Validate that we have store info for all existing replicas
        for store_id in existing_replicas:
            if store_id not in store_info_map:
                raise ValueError(f"Store {store_id} found in replica data but missing from store info map")

        # Validate that we have store info for all target stores
        for store_id in target_stores:
            if store_id not in store_info_map:
                raise ValueError(f"Target store {store_id} missing from store info map")

        # Get nodes that already have replicas for this range
        existing_nodes = {store_info_map[store_id].node_id for store_id in existing_replicas}
        existing_nodes_sorted = sorted(existing_nodes)

        # First preference: target stores that don't have this range AND are on different nodes
        node_safe_targets = [
            store for store in target_stores
            if store not in existing_replicas and store_info_map[store].node_id not in existing_nodes
        ]

        if node_safe_targets:
            return random.choice(node_safe_targets)

        # Second preference: target stores that don't have this range (but might be on same node)
        range_safe_targets = [store for store in target_stores if store not in existing_replicas]

        # TODO: This can just be a single error return.
        if range_safe_targets:
            # This is a configuration problem - we can avoid range conflicts but not node conflicts
            target_nodes = {store_info_map[store].node_id for store in range_safe_targets}
            overlapping_nodes = existing_nodes.intersection(target_nodes)
            raise ValueError(
                f"Cannot find target store on different node. "
                f"Existing replica nodes: {existing_nodes_sorted}, "
                f"Available target nodes: {sorted(target_nodes)}, "
                f"Overlapping nodes: {sorted(overlapping_nodes)}. "
                f"Cluster needs more nodes or better store distribution to avoid node conflicts."
            )

        # No stores available that don't already have this range
        raise ValueError(
            f"No target stores available that don't already have this range. "
            f"Existing replicas on stores: {sorted(existing_replicas)}, "
            f"Available target stores: {sorted(target_stores)}. "
            f"All target stores already have replicas of this range."
        )

    def process_ranges_incrementally(self, config: RebalanceConfig, target_stores: List[int], store_info_map: Dict[int, StoreInfo]) -> tuple[int, int]:
        """Process ranges one at a time with detailed observability.

        Returns:
            Tuple of (successful_moves, errors)
        """
        ranges = self.get_ranges_to_move(config)
        if not ranges:
            print(f"No ranges found on store {config.from_store} for {config.table_name}@{config.index_name}")
            return (0, 0)

        print(f"Processing {len(ranges)} ranges for relocation:")
        print()

        successful_moves = 0
        errors = 0

        with psycopg2.connect(self.db_url) as conn:
            with conn.cursor() as cur:
                for i, (range_id, existing_replicas) in enumerate(ranges.items(), 1):
                    # Show existing replicas with node information
                    replica_info = []
                    for store_id in existing_replicas:
                        if store_id not in store_info_map:
                            raise ValueError(f"Store {store_id} found in range data but missing from store info map")
                        store_info = store_info_map[store_id]
                        marker = "*" if store_id == config.from_store else ""
                        replica_info.append(f"n{store_info.node_id}s{store_id}{marker}")

                    # Find best target store
                    target_store = self.find_best_target_store(existing_replicas, target_stores, store_info_map)

                    if target_store not in store_info_map:
                        raise ValueError(f"Selected target store {target_store} missing from store info map")
                    target_store_info = store_info_map[target_store]

                    # Generate and execute command
                    cmd = f"ALTER RANGE {range_id} RELOCATE FROM {config.from_store} TO {target_store};"

                    # Single line output
                    replicas_str = " ".join(replica_info)
                    print(f"Range {i:2}/{len(ranges)} [{range_id}]: {replicas_str} -> n{target_store_info.node_id}s{target_store}", end=" ")

                    try:
                        cur.execute(cmd)
                        # Check results
                        success = True
                        if cur.description:
                            results = cur.fetchall()
                            for row in results:
                                if len(row) < 3 or str(row[2]).lower() != 'ok':
                                    success = False
                                    break

                        if success:
                            print("✓")
                            successful_moves += 1
                        else:
                            print("✗")
                            errors += 1
                            for row in results:
                                if len(row) < 3 or str(row[2]).lower() != 'ok':
                                    print(f"  Error result: {' | '.join(str(col) for col in row)}")

                        conn.commit()
                    except Exception as e:
                        print("✗")
                        errors += 1
                        print(f"  Error: {e}")
                        conn.rollback()

        return (successful_moves, errors)

    def format_store_distribution(self, store_info_map: Dict[int, StoreInfo], from_store: int, disallowed_stores: Optional[Set[int]] = None) -> str:
        """Format store distribution as a single line string.

        Args:
            store_info_map: Store information mapping
            from_store: Source store ID
            disallowed_stores: Set of disallowed store IDs (optional)

        Returns:
            Formatted string of store distribution
        """
        sorted_stores = sorted(store_info_map.items(), key=lambda x: x[1].replica_count, reverse=True)

        store_parts = []
        for store_id, store_info in sorted_stores:
            marker = ""
            if store_id == from_store:
                marker = "*"
            elif disallowed_stores is not None and store_id in disallowed_stores and store_id != from_store:
                marker = "!"
            store_parts.append(f"n{store_info.node_id}s{store_id}[{store_info.replica_count}]{marker}")

        return " ".join(store_parts)

    def rebalance(self, config: RebalanceConfig) -> None:
        """Main rebalancing operation."""

        # Use auto-detected target stores, then filter out any manually specified disallowed stores
        # TODO: Improve this. We don't need to recompute this stuff in rebalance.
        if config.disallowed_stores:
            # User provided manual disallowed stores - exclude them from auto-detected targets
            all_disallowed = config.disallowed_stores | {config.from_store}
            target_stores = [store for store in config.target_stores
                           if store not in all_disallowed and store != config.from_store]
        else:
            # Use auto-detected stores, exclude the from_store
            target_stores = [store for store in config.target_stores if store != config.from_store]

        if not target_stores:
            raise ValueError("No valid target stores available. Check disallowed_stores and from_store.")

        # Process ranges incrementally
        successful_moves, errors = self.process_ranges_incrementally(config, target_stores, config.store_info_map)

        # Print summary
        print()
        print(f"Summary:")
        print(f"  Successful moves: {successful_moves}")
        print(f"  Errors: {errors}")
        print()

        # Get final store distribution
        final_store_info = self.get_store_replica_counts(config.table_name, config.index_name)
        final_distribution = self.format_store_distribution(final_store_info, config.from_store)

        # Print before/after comparison
        initial_distribution = self.format_store_distribution(config.store_info_map, config.from_store, config.disallowed_stores)
        print("Before/After comparison:")
        print(f"  Before: {initial_distribution}")
        print(f"  After:  {final_distribution}")

    def run_rebalancer(self, table_name: str, index_name: str, from_store: Optional[int] = None,
                      num_replicas: Optional[int] = None, disallowed_stores: Optional[List[int]] = None) -> None:
        """Run the complete rebalancing operation with auto-detection and user overrides.

        Args:
            table_name: Target table name
            index_name: Target index name
            from_store: Store ID to move ranges from (auto-detected if None)
            num_replicas: Number of replicas to move (auto-detected if None)
            disallowed_stores: Store IDs not allowed as targets (auto-detected if None)
        """
        # Always auto-detect, then apply user overrides
        auto_from_store, auto_disallowed_stores, auto_target_stores, replica_counts = self.auto_detect_stores_and_from_store(table_name, index_name)

        # Use user-provided values or fall back to auto-detected ones
        final_from_store = from_store if from_store is not None else auto_from_store
        final_disallowed_stores = set(disallowed_stores) if disallowed_stores else auto_disallowed_stores

        # Auto-detect num_replicas if not provided
        if num_replicas is None:
            # Calculate valid target stores after applying user overrides
            if disallowed_stores:
                # User provided manual disallowed stores - exclude them from auto-detected targets
                all_disallowed = set(disallowed_stores) | {final_from_store}
                valid_targets_for_calc = [store for store in auto_target_stores
                                        if store not in all_disallowed and store != final_from_store]
            else:
                # Use auto-detected stores, exclude the from_store
                valid_targets_for_calc = [store for store in auto_target_stores if store != final_from_store]

            final_num_replicas = self.auto_detect_num_replicas(final_from_store, valid_targets_for_calc, replica_counts)
        else:
            final_num_replicas = num_replicas

        if from_store is not None:
            print(f"Using from_store: {final_from_store} ({replica_counts[final_from_store].replica_count} replicas)")

        if disallowed_stores:
            print(f"Using disallowed_stores: {sorted(final_disallowed_stores)}")

        if num_replicas is None:
            total_replicas = sum(store_info.replica_count for store_info in replica_counts.values())
            total_stores = len(replica_counts)
            mean_replica_count = total_replicas // total_stores
            print(f"Auto-detected num_replicas: {final_num_replicas} (target mean: {mean_replica_count})")
        print()

        print(f"Rebalancing {table_name}@{index_name}:")
        print()

        # Show complete store distribution for context
        print("Current replica distribution (sorted by replica count, *source, !disallowed):")
        distribution_str = self.format_store_distribution(replica_counts, final_from_store, final_disallowed_stores)
        print(f"  {distribution_str}")
        print()

        config = RebalanceConfig(
            from_store=final_from_store,
            num_replicas=final_num_replicas,
            table_name=table_name,
            index_name=index_name,
            disallowed_stores=final_disallowed_stores,
            target_stores=auto_target_stores,
            store_info_map=replica_counts
        )

        self.rebalance(config)

def main():
    parser = argparse.ArgumentParser(description='Rebalance CockroachDB index replicas across stores')
    parser.add_argument('--db-url', required=True,
                      help='Database URL (e.g., postgresql://username:password@host:port/database?sslmode=require)')
    parser.add_argument('--table', required=True,
                      help='Target table name')
    parser.add_argument('--index', required=True,
                      help='Target index name')
    parser.add_argument('--from-store', type=int,
                      help='Store ID to move ranges from (auto-detected if not specified)')
    parser.add_argument('--num-replicas', type=int,
                      help='Number of replicas to move (auto-detected if not specified)')
    parser.add_argument('--disallowed-stores', type=int, nargs='+',
                      help='Store IDs not allowed as relocation targets (auto-detected if not specified)')

    args = parser.parse_args()

    rebalancer = CockroachRebalancer(args.db_url)

    rebalancer.run_rebalancer(args.table, args.index, args.from_store, args.num_replicas, args.disallowed_stores)

if __name__ == "__main__":
    main()