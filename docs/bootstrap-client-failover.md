# Bootstrap client failover

Remote applications connect with `SquirixClient.ConnectAsync` and one or more bootstrap URLs in
`SquirixOptions.Endpoints`.

## What bootstrap endpoints are

- **HA / standby / front door:** interchangeable views of the same Squirix cluster. Each URL should reach a node (or
  proxy) that can serve the cache API and route to key owners.
- **Not shards:** do not list independent partitions as bootstrap endpoints. Key placement is owned by the server
  cluster, not the client URL list.

## Connect semantics

- Warm-up succeeds when **any** configured endpoint is reachable (configuration order).
- Unreachable endpoints are skipped without failing connect.
- The first reachable peer uses the configured bootstrap connect budget (default 5s per attempt, 30s overall).
- After a primary peer is selected, remaining peers are probed with a short fail-fast budget (500ms per attempt, 2s
  overall) so dead standby URLs do not delay connect.
- When at least one peer connects but others fail warm-up, the client emits
  `squirix_client_pool_bootstrap_warmup_skipped_total` (tags: `node_id`, `reason`) and an OpenTelemetry activity
  `client.bootstrap.warmup.peer_skipped` per skipped peer. Connect semantics are unchanged.

## Per-operation failover (v0.1 exported client)

The v0.1 `Squirix` client exposes basic key/value and expiration operations on `ICache<T>` only. Each exported method
maps to one or more gRPC calls on the server.

| Operation kind                                                                                                                                                  | Failover behavior                                                              |
| --------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------ |
| exported `ICache<T>` methods (`GetValueAsync`, `SetAsync`, `AddAsync`, `RemoveAsync`, `GetOrAddAsync`, `TouchAsync`, `RemoveExpirationAsync`, `UpdateAsync`, …) | On transport-level failure, retry the same RPC on the next bootstrap endpoint. |

Transport failover covers gRPC transport errors (for example `Unavailable`, `DeadlineExceeded`). Application-level
outcomes (`NotFound`, validation errors) are not failover signals.

## Testing

- E2E (`ClientBootstrapConnectTests`) exercises the public `SquirixClient.ConnectAsync` path with a live endpoint plus
  an unreachable peer using production connect defaults.
- Client integration tests (`ClientPoolWarmUpTests`) cover unreachable-only warm-up with explicit fail-fast options.

## What this is not

- Not full cluster partition routing or dynamic membership on the client.
- Not replication or automatic data failover between nodes (see [consistency](consistency.md) for server semantics).
