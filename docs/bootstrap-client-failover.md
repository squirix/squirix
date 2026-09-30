# Bootstrap client failover

Remote applications connect with `SquirixClient.ConnectAsync` and one or more bootstrap URLs in
`SquirixClientOptions.Endpoints`.

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
maps to **one** unary gRPC RPC on the server (transport failover may **retry** that same RPC on another bootstrap
endpoint).

| Operation kind                                                                                                                                                  | Failover behavior                                                              |
| --------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------ |
| exported `ICache<T>` methods (`GetValueAsync`, `SetAsync`, `AddAsync`, `RemoveAsync`, `GetOrAddAsync`, `TouchAsync`, `RemoveExpirationAsync`, `UpdateAsync`, …) | On transport-level failure, retry the same RPC on the next bootstrap endpoint. |

Transport failover covers gRPC transport errors (for example `Unavailable`, `DeadlineExceeded`). Application-level
outcomes (`NotFound`, validation errors) are not failover signals.

The client SDK keeps the same `operation_id` when it retries a mutating RPC on the next bootstrap endpoint. Server
nodes propagate that id through owner-routing hops, so a retry that reaches a different entry node still deduplicates on
the key owner when the payload fingerprint matches. See [api.md](api.md) for `operation_id` format and replay rules.

## Operation deadline

Every exported `ICache<T>` operation runs under one absolute deadline, 15s after the operation starts. The deadline is
shared by everything the operation does: retries on one endpoint (3 attempts of up to 3s each), failover to the next
bootstrap endpoint, and the server-side routing to the key owner. It does not grow with the number of endpoints.

- The client sends the deadline to the server as the gRPC call deadline. The entry node and any owner-routing hop work
  within the same remaining budget.
- When the deadline passes, the operation fails with `RpcException` and status `DeadlineExceeded`; no further attempt or
  endpoint is tried. When the last endpoint failed with a transport error just before the deadline, that error is
  surfaced instead.
- A caller `CancellationToken` still applies: whichever of the token and the deadline fires first ends the operation.
  Caller cancellation surfaces as `OperationCanceledException`.
- A mutation that fails with `DeadlineExceeded` may still have been applied on the server. Treat its outcome as unknown:
  a new call gets a new `operation_id`, so read the key back before repeating a change that is not idempotent.
- Deadline expiry is counted in `squirix_rpc_timeouts_total` with tags `scope=overall` and `kind=deadline_budget`.

## Testing

- E2E (`ClientBootstrapConnectTests`) exercises the public `SquirixClient.ConnectAsync` path with a live endpoint plus
  an unreachable peer using production connect defaults.
- Client integration tests (`ClientPoolWarmUpTests`) cover unreachable-only warm-up with explicit fail-fast options.
- Client unit tests (`OperationDeadlineTests`) drive a real `RemoteCache` against hung endpoints and check the shared
  deadline, its gRPC call deadline, caller cancellation and the `operation_id` across retries.
- E2E (`OperationDeadlineE2ETests`) checks that the server receives the client deadline and that an owner-routing hop
  gets a budget no larger than the entry node's.

## What this is not

- Not full cluster partition routing or dynamic membership on the client.
- Not replication or automatic data failover between nodes (see [consistency](consistency.md) for server semantics).
