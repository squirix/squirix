# Clustering and routing

squirix v0.1 uses **static cluster topology** with **consistent-hash single-owner routing**. Each key maps to one
owner node for the lifetime of a routing configuration.

## Static topology

Peers are configured explicitly in `Squirix.settings.json` or `SquirixServerOptions`. There is no dynamic membership
or automatic rebalancing in v0.1.

Example settings discovery and validation: [configuration.md](configuration.md).

## Client bootstrap endpoints

Applications connect with one or more bootstrap URLs in `SquirixOptions.Endpoints`. These URLs are an **HA front door**
— interchangeable views of the same cluster — **not independent shards**.

```csharp
await using var client = await SquirixClient.ConnectAsync(
    options =>
    {
        options.Endpoints.Add("https://squirix-a:5001");
        options.Endpoints.Add("https://squirix-b:5002");
    },
    cancellationToken);
```

Connect succeeds when any endpoint is reachable. Per-operation transport failover retries on the next bootstrap URL.
Details: [bootstrap-client-failover.md](bootstrap-client-failover.md).

## Consistency guarantees (v0.1 preview)

- Single-key reads and writes execute on the owning node
- Durability is per node; no replication or automatic failover
- Multi-key operations are not transactions across owners
- Memory pressure may reject growing writes before they are persisted

Non-goals: cluster-wide linearizability proofs, cross-owner atomic updates, dynamic membership-driven routing.

Full semantics: [consistency.md](consistency.md).

## Multi-node deployment

Docker Compose examples with two nodes: [containerization.md](containerization.md).

From the **host**, bootstrap clients at the published HTTPS ports (`https://localhost:5001`,
`https://localhost:5002`) with the compose JWT settings. Inside the Docker network, nodes use service DNS names and container
port **5000** (`https://squirix-node-a:5000` in mounted settings).

Before changing topology in containers, validate settings:

```powershell
squirix-server validate-config --settings ./Squirix.settings.json --strict
```

`Cluster.Url` must match the local peer entry in each node's settings file.
