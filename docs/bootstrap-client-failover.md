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
- Before a primary peer is selected, each endpoint has its own connect budget: 5s per attempt and 30s for that endpoint.
  Unreachable endpoints earlier in the list can each consume that budget before a later endpoint is tried.
- After a primary peer is selected, each remaining peer is probed with a shorter budget: 500ms per attempt and 2s for
  that peer. These probes still complete before `ConnectAsync` returns.
- The 30s budget is not a cap on the complete `ConnectAsync` call. Pass a caller `CancellationToken` with a timeout to
  bound the whole connection sequence.
- When at least one peer connects but others fail warm-up, the client emits
  `squirix_client_pool_bootstrap_warmup_skipped_total` (tags: `node_id`, `reason`) and an OpenTelemetry activity
  `client.bootstrap.warmup.peer_skipped` per skipped peer. Connect semantics are unchanged.

Warm-up connects the gRPC channel; it does not execute a cache RPC or validate a bearer token. `ConnectAsync` can
succeed against a JWT-protected server without a token, while the first cache operation fails with `Unauthenticated`.
It also does not establish that recovery has finished or that a replica group has a leader.

## Per-operation failover (v0.1 exported client)

Each exported `ICache<T>` operation maps to one unary gRPC method. The SDK can retry that request on the same
bootstrap endpoint or send it to another endpoint.

The retry policy uses gRPC status codes, not a distinction between transport and application errors. It retries
`Unavailable`, `Internal`, and `ResourceExhausted`, as well as attempt timeouts, while attempts and the operation budget
remain. After the endpoint policy fails, `Unavailable`, `DeadlineExceeded`, `Internal`, and `ResourceExhausted` can move
the call to another bootstrap endpoint. This includes capacity refusals such as memory pressure, journal quota, and
payload limits: a deterministic rejection can therefore be retried without becoming recoverable.

`NotFound`, `InvalidArgument`, and authentication failures are not failover signals. An answer from a deposed replica
group leader (`FailedPrecondition` with detail `stale-term`) reroutes the call at most once to another endpoint.
`COMMIT_OUTCOME_UNKNOWN` is neither retried nor failed over. Avoid adding an unbounded application retry loop around
these SDK retries; inspect capacity and validation failures instead.

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
  endpoint is tried. When the deadline passes after an endpoint failed with a transport error, that endpoint's error is
  surfaced and the remaining endpoints are not tried.
- For `GetOrAddAsync` the deadline starts after the value factory returns; the factory itself is bounded only by
  cancellation. Concurrent callers using the same client-side cache instance and key share one in-flight factory run, and its token is canceled once every caller waiting
  on it has canceled; a canceled caller stops waiting without canceling the others.
- A caller `CancellationToken` still applies: whichever of the token and the deadline fires first ends the operation.
  Caller cancellation surfaces as `OperationCanceledException`.
- A mutation that times out or whose caller stops waiting may still have been applied on the server. A new SDK call
  gets a new `operation_id`. Reconcile the application state before repeating a non-idempotent change; reading the key
  can help, but does not by itself prove which writer produced its current value.
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
- Not replication or leader election: the client only retries and reroutes. Electing a new leader for a replica group
  is an opt-in server feature for RF>=3, see [clustering.md](clustering.md#automatic-failover).
