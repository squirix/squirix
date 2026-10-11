# Clustering and routing

squirix v0.1 uses **static cluster topology** with **consistent-hash single-owner routing**. Each key maps to one
owner node for the lifetime of a routing configuration.

## Static topology

Peers are configured explicitly in `Squirix.settings.json` or `SquirixServerOptions`. There is no dynamic membership
or automatic rebalancing in v0.1.

Example settings discovery and validation: [configuration.md](configuration.md).

## Client bootstrap endpoints

Applications connect with one or more bootstrap URLs in `SquirixClientOptions.Endpoints`. These URLs are an **HA front door**
— interchangeable views of the same cluster — **not independent shards**.

```csharp
using System;
using Squirix.Client;

await using var client = await SquirixClient.ConnectAsync(
    options =>
    {
        options.Endpoints.Add(new Uri("https://squirix-a:5001"));
        options.Endpoints.Add(new Uri("https://squirix-b:5002"));
    },
    cancellationToken);
```

Connect succeeds when any endpoint is reachable. Per-operation transport failover retries on the next bootstrap URL.
Details: [bootstrap-client-failover.md](bootstrap-client-failover.md).

## Consistency guarantees (v0.1 preview)

- Single-key reads and writes execute on the owning node (the group leader when automatic failover is on)
- RF=1 keeps single-owner behavior: durability is per node with no replication or automatic failover
- RF=2 is a synchronous mirror only: the majority is two, so losing either member stops RF=2 writes and no
  replacement is elected; RF=2 never promotes after peer loss
- RF>=3 with persistence and mTLS commits on a majority, so a group keeps accepting writes while one follower is down;
  losing the owner of a group needs [automatic failover](#automatic-failover) (RF>=3 only)
- Automatic failover is off by default: the owner of each key leads its replica group, and while that owner is down
  its keys are unavailable ([architecture/replication-consensus.md](architecture/replication-consensus.md))
- Multi-key operations are not transactions across owners
- Memory pressure may reject growing writes before they are persisted
- Journal disk quota may reject durable appends with `JOURNAL_DISK_QUOTA` before they are persisted

Non-goals: cluster-wide linearizability proofs, cross-owner atomic updates, dynamic membership-driven routing.

Full semantics: [consistency.md](consistency.md).

## Automatic failover

Opt-in, RF>=3 only. With it off, RF>1 behaves as described above: the owner of each key leads its group.

Turn it on by setting both `Squirix:Cluster:AutomaticFailoverEnabled` and `Squirix:Cluster:QuorumReadsEnabled` to
`true` in the settings of every node, on empty data directories. There is no CLI flag or environment variable.
Validation refuses one flag without the other, either flag with `ReplicaCount` below 3, and a changed value on an
existing data directory. Both flags are inputs of the topology fingerprint, so a node configured differently refuses
replication with its peers. RF>1 itself still requires persistence, internode mTLS, and `ReplicationEnabled`. Details:
[configuration.md](configuration.md#automatic-failover-and-quorum-reads).

When a leader goes silent:

- The other members of its group notice after the election timeout plus a random jitter, run a pre-vote and a vote,
  and a member that gets a majority of votes becomes leader in a new term; a member votes only for a candidate whose
  log is at least as up to date as its own. A new leader accepts writes only after a majority committed an entry of
  its term. Election timeout (1 s), jitter (up to 1 s), heartbeat (100 ms), and vote timeout (250 ms) are fixed and
  not configurable, so expect a few seconds without a leader.
- Without a reachable majority no leader is elected and the group refuses reads and writes (fails closed).
- A deposed or cut-off former leader steps down and refuses requests; it rejoins as a follower and catches up from the
  new leader's log.

What clients see:

- Any node accepts a single-key call and routes it to the leader of the key's group, rerouting at most once if the
  target answers that it is no longer the leader. A node without a known leader waits for one (election timeout plus
  jitter, capped by the call deadline).
- While no leader is available the call fails with gRPC `Unavailable`: `Replica group has no leader with authority on
  this node; nothing was read or written.` or the detail `replica_leader_changed`. Nothing was written, so a retry
  with the same `operation_id` is safe.
- The `squirix` client retries transport failures and moves to the next bootstrap endpoint within its single 15 s
  operation deadline ([bootstrap-client-failover.md](bootstrap-client-failover.md)). A failover that takes longer
  surfaces as `Unavailable` or `DeadlineExceeded`. A mutation that fails with `COMMIT_OUTCOME_UNKNOWN` or
  `DeadlineExceeded` may have been committed. A retry with the same `operation_id` replays the original outcome for at
  least one minute after the write, and for up to 15 minutes while the group's outcome table has room.
- Reads are quorum reads: the leader confirms with a majority that it still leads before it answers, so a read sees
  every write committed before it. A read that cannot be confirmed fails with `Unavailable` and the detail
  `read_quorum_unconfirmed` or `read_index_unapplied`; it never returns a possibly stale value.
- Expiry on RF>1 is decided by the leader and takes effect through committed records, so followers and a new leader
  agree on which keys are gone.

Known limits:

- RF=2 never promotes after losing a member; RF=1 never elects.
- A follower that fell behind the compacted part of the leader's log is not repaired from a snapshot. It stays out of
  the write quorum; the group continues on the remaining majority with less fault margin. The leader logs a warning
  (`needs entries the leader already compacted`) and counts `squirix_replication_catch_up_sessions_total` with
  `outcome="compacted"`. There is no operator repair for it in this release.
- All nodes must run builds that agree on the replication settings and the minimum cluster package version
  (`0.1.0-preview.9`); mixed versions fail readiness through the topology fingerprint, and a data directory activated by
  an older build refuses to start.
- Membership is static; there is no rebalancing or dynamic membership.

Observe a failover with `/health/ready` (the `replica_readiness` check) and the metrics `squirix_replication_role` and
`squirix_replication_term` per group ([diagnostics.md](diagnostics.md)). `squirix-server doctor` is an offline check
of the data directory and does not show the live leader.

## Multi-node deployment

The Docker Compose HA example runs three nodes with RF=3, persistence, and mTLS: [containerization.md](containerization.md).

RF=2 is documented as a synchronous mirror only; the HA demo uses RF=3. RF>1 topologies require a homogeneous
cluster package version; mixed versions fail readiness through the topology fingerprint.

Remote peers require internode mTLS at startup (cluster CA, per-node certificate with `CN` equal to `NodeId`, internal
listener port). External
application clients still authenticate with JWT on the primary listener. Full guidance:
[security/internode-mtls.md](security/internode-mtls.md).

From the **host**, bootstrap clients at the published HTTPS ports (`https://localhost:5001`,
`https://localhost:5002`, `https://localhost:5003`) with the compose JWT settings. Inside the Docker network, nodes use
service DNS names and container port **5000** (`https://squirix-node-a:5000` in mounted settings).

Before changing topology in containers, validate settings:

```bash
squirix-server validate-config --settings ./Squirix.settings.json --strict
```

`Cluster.Uri` must match the local peer entry in each node's settings file.

## Ring agreement

Every node builds its consistent-hash ring from its own configuration. Two nodes agree on key ownership only when these inputs match:

- `ClusterId`
- `VirtualNodes`
- the set of peer `NodeId` values (peer order, duplicates, URIs, configuration generation, and replica count do not matter)

A node that finds a peer with a different ring stops serving cache operations instead of executing on inconsistent ownership:

- Each internal owner call carries a fingerprint of the sender ring. The receiving node compares it with its own before any handler or idempotency store runs.
- A mismatch refuses the call, so a forwarded operation is never executed by a node that disagrees with the sender about the owner.
- Both nodes are fenced: the node that refused the call and the node whose forward was refused.
- A fenced node refuses every cache operation until it is restarted.

Symptoms:

- Clients get `Unavailable` with the detail `Cluster ring mismatch: the forwarding node and the key owner disagree on the peer list, ring settings or server version; nothing was executed.`
  for the first forwarded call, and `Cache operations are refused: the key owner or this node
  detected a cluster ring mismatch with a peer; make the peer lists agree and restart the affected nodes.` afterwards. Retrying against the fenced nodes does not help;
  the client fails over to other endpoints.
- A missing peer fingerprint means the peer runs a version without ring agreement.
- `/health/ready` answers `503` with the body `Unhealthy` on a fenced node; `/health/live` is not affected. The `ring_agreement` description that names the peer and the
  side that detected the mismatch appears in the health-check log entry, which repeats at `Error` on each probe, not in the HTTP body.
- The node logs one `Error` entry per peer with the local and peer ring fingerprints.

Fix:

1. Make `ClusterId`, `VirtualNodes`, and the peer `NodeId` list identical on every node.
2. Restart the misconfigured nodes and every node that reports the mismatch.
3. Change peer lists with a full stop and start of the cluster, not a rolling restart: a node on the old list and a node on the new list disagree until all nodes are restarted.
4. Moving to a version that has ring agreement, and any later change of the fingerprint format, also needs a full stop and start, RF=1 included: a node on the old version sends no
   fingerprint, so the upgraded nodes record a mismatch and are fenced during a rolling upgrade.

Limitations:

- Detection happens on the first forwarded call between the two nodes; nodes that never forward to each other are not compared.
- A peer that is not in the receiver peer list is rejected at the mTLS handshake and appears as an unreachable owner, not as a ring mismatch.
- Recovery needs a restart; a fenced node does not rejoin by itself when the configurations are corrected.
- The check covers ownership inputs only. Replication topology agreement for RF>1 is verified separately by the topology fingerprint.
