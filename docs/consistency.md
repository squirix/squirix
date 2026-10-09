# Consistency

squirix is a single-owner distributed cache with static routing. Each key maps to one owner node for the lifetime of a
routing configuration.

## Guarantees (v0.1 preview)

- Single-key reads and writes execute on the owning node.
- With RF=1, durability is per node, with no replication or automatic failover. RF>1 replicates each write to its
  replica group; RF>=3 can opt into [automatic failover and quorum reads](configuration.md#automatic-failover-and-quorum-reads).
- With persistence on, a read never returns a value whose journal frame is not yet durable, and a failed fsync never
  leaves the value in memory.
- Multi-key operations are not transactions across owners.
- Memory pressure may reject growing writes before they are persisted.
- Journal disk quota may reject durable appends with `JOURNAL_DISK_QUOTA` before they are persisted.

## Non-goals (v0.1 preview)

- Cluster-wide linearizability proofs.
- Automatic rebalancing or cluster-wide membership-driven routing in squirix v0.1.
- Cross-owner atomic updates.

For routing and bootstrap endpoints, see [clustering.md](clustering.md). For operational guidance, see
[operational-runbook.md](operational-runbook.md).
