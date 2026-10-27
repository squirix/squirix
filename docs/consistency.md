# Consistency

squirix is a single-owner distributed cache with static routing. Each key maps to one owner node for the lifetime of a
routing configuration.

## Guarantees (v0.1 preview)

- Single-key reads and writes execute on the owning node; with automatic failover on, on the current leader of the key's replica group.
- With RF=1, durability is per node, with no replication or automatic failover. RF>1 replicates each write to its
  replica group; RF>=3 can opt into [automatic failover and quorum reads](configuration.md#automatic-failover-and-quorum-reads).
- With persistence on, a read never returns a value whose journal frame is not yet durable, and a failed fsync never
  leaves the value in memory.
- Multi-key operations are not transactions across owners.
- Memory pressure may reject growing writes before they are persisted.
- Journal disk quota may reject durable appends with `JOURNAL_DISK_QUOTA` before they are persisted.
- The client does not coalesce concurrent reads of one key: each `GetValueAsync`, `GetEntryAsync` and `GetExpirationAsync`
  call sends its own request. A shared in-flight read could hand a later caller a reply that predates a write committed
  before that caller started, including the caller's own write.

## CAP and PACELC

squirix chooses consistency over availability and over latency: in PACELC terms it is PC/EC, in CAP terms CP.

- Partition (PC): a replica group that cannot reach a majority refuses writes instead of accepting them on a minority,
  and with quorum reads on a read that cannot be confirmed fails with `Unavailable` instead of returning a possibly
  stale value. RF=2 stops writes when either member is lost; RF=1 keys are unavailable while their owner is down.
- Else (EC): with quorum reads on, the leader confirms with a majority that it still leads before it answers a read,
  so every read pays a round trip to the group in exchange for seeing every write committed before it.
- Automatic failover (RF>=3) restores availability after the leader is lost by electing a new one; it does not trade
  away consistency. See [clustering.md](clustering.md#automatic-failover).

## Non-goals (v0.1 preview)

- Cluster-wide linearizability proofs.
- Automatic rebalancing or cluster-wide membership-driven routing in squirix v0.1.
- Cross-owner atomic updates.

For routing and bootstrap endpoints, see [clustering.md](clustering.md). For operational guidance, see
[operational-runbook.md](operational-runbook.md).
