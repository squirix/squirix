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

- Single-key reads and writes execute on the owning node
- RF=1 keeps single-owner behavior: durability is per node with no replication or automatic failover
- RF=2 is a synchronous mirror only: the majority is two, so losing either member stops RF=2 writes and no
  replacement is elected; RF=2 never promotes after peer loss
- RF>=3 with persistence and mTLS survives single-node loss on the remaining majority; automatic failover and
  quorum reads apply only to RF>=3 after the proof matrix (see [architecture/replication-consensus.md](architecture/replication-consensus.md))
- Multi-key operations are not transactions across owners
- Memory pressure may reject growing writes before they are persisted
- Journal disk quota may reject durable appends with `JOURNAL_DISK_QUOTA` before they are persisted

Non-goals: cluster-wide linearizability proofs, cross-owner atomic updates, dynamic membership-driven routing.

Full semantics: [consistency.md](consistency.md).

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

- Clients get `Unavailable` with the detail `Cluster ring mismatch: the forwarding node and the key owner were started with different peer lists; nothing was executed.`
  for the first forwarded call, and `Cache operations are refused: this node detected a cluster ring mismatch with a peer; make the peer lists agree and restart the
  affected nodes.` afterwards. Retrying does not help.
- `/health/ready` answers `503` on a fenced node; the `ring_agreement` check names the peer and the side that detected the mismatch. `/health/live` is not affected.
- The node logs one `Error` entry per peer with the local and peer ring fingerprints.

Fix:

1. Make `ClusterId`, `VirtualNodes`, and the peer `NodeId` list identical on every node.
2. Restart the misconfigured nodes and every node that reports the mismatch.
3. Change peer lists with a full stop and start of the cluster, not a rolling restart: a node on the old list and a node on the new list disagree until all nodes are restarted.

Limitations:

- Detection happens on the first forwarded call between the two nodes; nodes that never forward to each other are not compared.
- Recovery needs a restart; a fenced node does not rejoin by itself when the configurations are corrected.
- The check covers ownership inputs only. Replication topology agreement for RF>1 is verified separately by the topology fingerprint.
