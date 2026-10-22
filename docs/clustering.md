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

## Starting a replica group

With RF>1 a replica group is the owner of a set of keys and the nodes that hold copies of its log; a node that holds a
copy on a new, empty data directory holds an unseeded copy. An unseeded copy takes no part in the group: it grants no
vote, takes no entries, and does not lead or campaign.

- A replica group of a new cluster starts once every member of the group is up. The owner of the group asks each other
  member for the state of its copy; when all of them hold unseeded copies of the same topology, it offers each member a
  place, and completes its own copy once all of them accepted. The group then runs as usual: the owner leads it, or
  with automatic failover the group elects its leader. Until then the keys of the group are unavailable (`Unavailable`) and the nodes report not ready.
  Starting only part of a new cluster starts no group whose members are not all up. A node of a new cluster therefore
  reports not ready until every member of its groups is up, so start-up orchestration must not wait for one node to be
  ready before it starts the next.
- All members of a group losing their disks at once cannot be told from a new cluster: the group starts empty.
- A member that starts on an empty data directory while its group already runs (for example after a disk loss) holds an
  unseeded copy until the leader of the group repairs it, with no operator action: meanwhile it grants no vote, takes no
  entries, and reports not ready, and the group keeps working on its other members. Before the repair the leader asks
  the other members for their current term. It needs answers from members that kept their copies and that, with the
  leader and without the wiped member, form a majority: the other member of three, two of the three others of five.
  When none of them holds a term above the leader's, the leader sends the data of the group by a state transfer; a
  higher term makes the leader step down instead. The copy stays unseeded until the transfer ends; its last step makes
  it complete and records the leader's term and a vote for the leader, so the member cannot vote a second time in a
  term it may have voted in before the loss. It then takes entries and votes as before. A leader notices the member
  at its next heartbeat; with automatic failover off, at the next write of the group.
- A wiped member is not repaired when too few members kept their copies for that check (for example two wiped members
  of three: no leader is elected, and the cluster is recreated), nor, with automatic failover off, when it is the owner
  of the group, since nobody else leads it.
- Do not restore the data directory of a replicated node from an older backup: the restored copy opens complete with an
  old term, vote, and log, and votes at once, which can help elect a leader that lacks entries the node acknowledged.
- With automatic failover off, an owner in that state refuses the keys of its group with `Unavailable`; the groups of
  the other nodes are not affected.

A node with an unseeded copy fails `/health/ready` with `503`. The `replica_readiness` description reads `replica group
'{id}' is not seeded on this node: its copy votes for nobody and takes no entries until the group is started or the copy
is repaired.` and appears in the health-check log entry, not in the HTTP body. The node logs the state of such a copy
when it opens the log of the group. `squirix-server doctor` shows it as `member unseeded offered` or `member unseeded not-offered` on the group line
([operational-runbook.md](operational-runbook.md)).

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
- A leader that stops gracefully stops heartbeating and gives up its authority when the stop begins. Its group elects a
  new leader while the node is still draining its in-flight requests, so the drain time is not added to the failover.
  A write that reaches the stopping node after that is refused with nothing written. A write it was already committing
  runs to its end as before.
- A deposed or cut-off former leader steps down and refuses requests; when it kept its data directory it rejoins as a
  follower and catches up from the new leader's log.
- A follower that needs log entries the leader already compacted is repaired by a state transfer: the leader sends it
  the data of the group at its applied index, and the follower then catches up from the log. No operator action is
  needed. While the transfer runs the copy of the follower is marked incomplete and the follower stays out of the write
  quorum; it counts again once it caught up. See
  [state transfer](architecture/replication-consensus.md#state-transfer) and the
  [runbook](operational-runbook.md#state-transfer).
- A new leader that cannot get a majority to commit an entry of its term within 30 seconds steps down and does not
  campaign for a few seconds, so another member can take the group; while the same node keeps being elected without
  getting authority, the 30 seconds double up to 4 minutes. The group accepts no writes or reads from that leader in
  the meantime.
- A leader with authority that cannot apply entries a majority already holds, such as with a full node journal or a
  failing disk, and whose applied index does not move for 30 seconds revokes its authority and stops heartbeating, so a
  member that can apply takes the group. The node does not campaign until it has applied its committed entries, which
  can take arbitrarily long with a full journal, and then waits a few seconds more; its memory of the group does not
  follow the new leader meanwhile. Any progress of the applied index restarts the 30 seconds, and the window does not
  double. Entries no majority holds do not count; a leader that lost its majority steps
  down by the quorum check instead. A node that leads its group without an election (fewer than three replicas, or
  failover off) has no such deadline, and no other member can take its group.
- A leader with authority whose appends to its own group log keep failing, with a write or flush error, a log that
  refuses as not ready, or an append that never returns, gives the group up the same way after 30 seconds, so a member
  that can append takes the group. A failed append or a refusing log starts the 30 seconds at the first failure after
  the last success, and the leader gives up while a failure is still visible. A log that latched its failure counts as refusing
  from then on, also when the next writes are refused before they reach it. An append that is merely in flight does
  not count from that moment: it must be the same append at two checks in a row and stand still for the whole 30
  seconds itself. The leader gives up only while the other members, without the leader, form a majority that answered
  from a ready log within the election timeout. A member whose log latched a failure answers but cannot vote, so when
  every replica runs out of disk together nobody could take the group, and the leader keeps it and its reads keep being
  served. A successful append clears the 30 seconds, and the window does not double. A failure, a quiet period, and
  another failure end the leadership at the second failure once 30 seconds passed without a success. Each fault type is
  logged once until an append succeeds. A node that leads its group without an election has no such deadline.
- A group log append that fails in the frame write, the flush, or the commit-index write behind the frames latches
  the log failed until the node restarts, because the frame may be on disk and a restart recovers it as an uncommitted
  tail that a majority can commit. The client gets `COMMIT_OUTCOME_UNKNOWN`, also on a retry with the same
  operation identifier, which is not executed again. A follower with a latched log does not vote and does not accept
  appends, so the group loses that member until the node restarts. An electable leader gives the group up through the
  deadline above; when the failed append is the leader-term entry, the promotion is not authorized and the group moves
  to another node through the election timeout. A leader without an election stays without writes until the node
  restarts. The latch is logged once at error level (event 4077), and the log reports readiness `failed` in the status.
- A follower that holds committed entries of a group it cannot apply, such as with a full node journal or a failing disk,
  does not campaign for that group, because a win would fail the start of its leadership and cost the group a term and an
  election. It still votes and accepts appends. The block ends at the first pass that applies everything committed, within
  about a second once the fault clears, and the node campaigns at its next election timeout. A group stopped by a committed
  record it cannot apply stays blocked until a state transfer replaces its copy. A block is logged once. A node
  that becomes blocked while it campaigns drops the candidacy before it promotes itself. When every member is blocked
  nobody campaigns, as nobody could lead the group anyway. After a restart a node also waits, without campaigning or
  taking its own group at the provisional term, until its apply loop has looked at the group once, which is after local
  recovery, and for a copy that is not complete or not ready, until it is; a node that cannot read the log status of a
  group keeps waiting.

What clients see:

- Any node accepts a single-key call and routes it to the leader of the key's group, rerouting at most once if the
  target answers that it is no longer the leader. A node without a known leader waits for one (election timeout plus
  jitter, capped by the call deadline less 1 s). All the leader waits of one call, on the entry node and on the nodes
  it is forwarded to, share one attempt budget (one attempt, 3 s, or the leader wait override plus 1 s) measured from
  the arrival of the call at the entry node. A wait for a leader ends at least 1 s before the attempt or the call
  deadline ends, and no reroute starts after that point; the forward connect timeout is at most half of that margin. A forward started after a
  leader wait, a stale reroute or a fallback keeps at least the margin, so a host that is down or drops connection attempts is refused as owner
  unreachable before that forward's deadline; a first forward with less deadline left than the connect timeout, or a host that accepts TCP but
  hangs in the TLS handshake, can still end as a deadline.
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
- A member that came back on an empty data directory holds an unseeded copy until a leader repairs it
  ([Starting a replica group](#starting-a-replica-group)). Until then it costs the group one vote: with two such members
  of three, or three of five, no leader is elected, nothing repairs them, and the group refuses reads and writes.
- All nodes must run builds that agree on the replication settings and the minimum cluster package version
  (`0.1.0-preview.9`); mixed versions fail readiness through the topology fingerprint, and a data directory activated by
  an older build refuses to start. So does a group metadata file written by an earlier build: the node prints `Replica
  group '{id}' metadata was written by an earlier build and is not supported; the file was left unchanged.` and does
  not start.
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
container names and container port **5000** (`https://squirix-node-a:5000` in mounted settings).

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
