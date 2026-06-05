# Consistency

squirix is a single-owner distributed cache with static routing. Each key maps to one owner node for the lifetime of a
routing configuration.

## Guarantees (v0.1 preview)

- Single-key reads and writes execute on the owning node.
- Durability is per node. There is no replication or automatic failover.
- Multi-key operations are not transactions across owners.
- Memory pressure may reject growing writes before they are persisted.

## Non-goals (v0.1 preview)

- Cluster-wide linearizability proofs.
- Automatic rebalancing or cluster-wide membership-driven routing in squirix v0.1.
- Cross-owner atomic updates.

For operational guidance, see [operational-runbook.md](operational-runbook.md) and [server-mode.md](server-mode.md).
