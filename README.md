# squirix

[![CI](https://github.com/squirix/squirix/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/squirix/squirix/actions/workflows/ci.yml)
[![License: Apache 2.0](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![NuGet](https://img.shields.io/badge/NuGet-0.1.0--preview.1-004880?logo=nuget&logoColor=white)](https://www.nuget.org/packages/Squirix/)
[![.NET](https://img.shields.io/badge/.NET-10.0-512BD4?logo=dotnet&logoColor=white)](https://dotnet.microsoft.com/download/dotnet/10.0)

squirix is the v0.1 client/server cache engine for .NET. Applications use the `Squirix` client SDK to talk to a
`Squirix.Server` node.

- Status: **v0.1 preview** (`0.1.0-preview.1`)
- Target framework: .NET 10 only
- License: Apache-2.0

Why .NET 10 only:

- squirix prioritizes deterministic correctness and durability behavior over broad runtime compatibility.
- Maintaining older TFM branches increases recovery/journal validation surface and slows hardening work.
- The project intentionally adopts modern runtime APIs and token primitives available in .NET 10.

## Documentation map

- [Architecture](docs/architecture.md)
- [No local mode](docs/local-mode.md)
- [Server mode](docs/server-mode.md)
- [Naming: product vs .NET identifiers](docs/naming-conventions.md)
- [Configuration](docs/configuration.md)
- [Cache name validation](docs/cache-name-validation.md)
- [Consistency](docs/consistency.md)
- [Containerization](docs/containerization.md)
- [Memory pressure](docs/configuration.md#memory-pressure-squirixsettingsjson)
- [Bootstrap client failover](docs/bootstrap-client-failover.md)
- [Operational runbook](docs/operational-runbook.md)
- [Diagnostics](docs/diagnostics.md)
- [Storage maintenance](docs/storage-maintenance.md)
- [Journal group commit](docs/journal-group-commit.md)
- [Serializer customization](docs/serialization.md)

## What squirix currently is

- Typed named caches through `ICache<T>`
- Basic single-owner routing with consistent hashing
- Server-owned node persistence with journal, snapshots, and recovery
- gRPC transport plus health/readiness and basic owner-inspection REST endpoints
- Optional **memory pressure**: decorator-owned approximate accounting (`MemoryAccountingLocalCacheDecorator`,
  `MemoryAccountingCacheDecorator`) and admission ( `MemoryAdmissionCacheDecorator`) — see
  [Memory pressure](docs/configuration.md#memory-pressure-squirixsettingsjson)
- Backpressure and health endpoints

Preview limitations:

- Early preview release (**0.1.0-preview.1**): API, wire format, and on-disk layouts may change during **0.x**
- Performance characteristics are not final and should not be compared to mature cache products yet
- Static peer configuration is the current cluster bootstrap model; dynamic membership and automatic topology changes
  are future work
- No replication or automatic failover; durability and recovery semantics are still hardening across preview releases
- No cluster-wide transactions or snapshots
- Runtime topology changes are not supported by static routing in squirix v0.1
- **0.x** preview releases (from **0.1.0** until **1.0.0**) do not promise upgrade or persistence compatibility
- Opaque tokens/cursors use versioned `Base64Url` formats; pre-v1 legacy token formats are unsupported unless
  explicitly documented otherwise

## Package layout

Client applications reference `Squirix`. Production cache engine deployments run `Squirix.Server.Host`, which
references the `Squirix.Server` runtime library.

| Package          | Purpose                                                                                           |
|------------------|---------------------------------------------------------------------------------------------------|
| `Squirix`        | v0.1 preview client SDK: typed `ICache<T>`, expiration, custom serialization, server connectivity |
| `Squirix.Server` | Basic cache server engine, runtime, hosting, durability, static ownership, REST/gRPC host         |

The standalone `Squirix.Server.Host` executable is published both as a release archive and as the `Squirix.Server.Tool`
.NET global tool package.

Primary production topology:

```text
application -> Squirix client SDK -> Squirix.Server node
```

The v0.1 client SDK uses the gRPC contract at
[`src/shared/transport/grpc/Protos/SquirixCache.proto`](src/shared/transport/grpc/Protos/SquirixCache.proto).
Generated CLR transport types remain internal implementation details.

The package boundary keeps the client surface in `Squirix` and the server runtime, durability, static ownership,
REST/gRPC hosting, and health/readiness endpoints in `Squirix.Server`. Product code must not use
`InternalsVisibleTo("Squirix.Server")`.

## Quick start

### Run a standalone development node

Install the .NET global tool:

```powershell
dotnet tool install --global Squirix.Server.Tool
squirix-server run --dev --data-dir ./data
```

The host prints the gRPC endpoint and a ready-to-use `SquirixClient.ConnectAsync(...)` snippet. The primary listener is
HTTP/2. Set `SQUIRIX_HTTP1_PORT` when a browser-friendly HTTP/1 sidecar is needed for health or admin requests.

Useful commands:

```powershell
squirix-server init
squirix-server validate-config --settings ./Squirix.settings.json
squirix-server validate-config --settings ./Squirix.settings.json --strict
squirix-server doctor --settings ./Squirix.settings.json
squirix-server version
```

### Run a local Docker node

Prerequisite: Docker Desktop installed and running.

```powershell
docker build -t squirix-server .
docker run --rm -p 5001:5001 -e SQUIRIX_ALLOW_UNAUTHENTICATED_EXTERNAL=true squirix-server run --urls http://0.0.0.0:5001
```

Notes:

- The node listens on `0.0.0.0:5001` in this example.
- Health and admin routes are reachable on that listener for local verification (see commands below).
- For a dedicated plaintext HTTP/1.1 sidecar on a **separate** port (as in `docker/docker-compose.yml`), set
  `SQUIRIX_HTTP1_PORT` to a port different from the primary `Cluster.Url` listen port.
- Non-loopback plaintext sidecar exposure is blocked by default. To explicitly allow it, set
  `SQUIRIX_HTTP1_ALLOW_INSECURE_EXTERNAL=true`.
- The HTTP/1.1 sidecar is a local/dev convenience surface. It is not a production-safe default.

Verify:

```powershell
curl.exe -i http://localhost:5001/health
curl.exe -s http://localhost:5001/health/ready/details
curl.exe -s http://localhost:5001/admin/whoami
```

Optional API key auth:

- Set `SQUIRIX_API_KEYS=KEY1,KEY2` on each service.
- Send `X-Api-Key: KEY1` (or JWT bearer token) to REST, `/admin`, and gRPC endpoints.
- Auth policy is enforced server-side with the same `ApiOrJwt` requirements across REST/admin/gRPC.

## Build and test locally

Prerequisite: .NET SDK as pinned in [`global.json`](global.json) (minimum **10.0.203**; `rollForward` allows newer
**10.0.x** feature bands).

The canonical solution file is **`squirix.slnx`** at the repository root.

```powershell
dotnet --info
dotnet restore squirix.slnx
dotnet build squirix.slnx --configuration Release
$env:DOTNET_SYSTEM_NET_HTTP_SOCKETSHTTPHANDLER_HTTP2UNENCRYPTEDSUPPORT = "1"
dotnet test squirix.slnx --configuration Release --no-build
```

The HTTP/2 env var is required for local h2c scenarios. If a system HTTP proxy is enabled, cleartext gRPC to loopback
can fail with “unable to establish HTTP/2 connection”; the SDK disables the proxy for `http://` cluster URLs, and local
tests use `UseProxy = false` on their handlers.

## Repository layout

- Source projects live under `src/` (`squirix`, `squirix.server`, shared transport under `src/shared/...`).
- Client test projects (PascalCase display names; lowercase dotted folders on disk):
  - **Squirix.UnitTests** — `tests/squirix/squirix.unit-tests/`
  - **Squirix.IntegrationTests** — `tests/squirix/squirix.integration-tests/`
  - **Squirix.E2ETests** — `tests/squirix.e2e.tests/`
- Server test projects live under `tests/squirix.server/` (for example **Squirix.Server.UnitTests**,
  **Squirix.Server.SmokeTests**, **Squirix.Server.IntegrationTests**).
- Benchmark projects:
  - **Squirix.Benchmarks** — `benchmarks/squirix.benchmarks/` (internal layer breakdown)
  - **Squirix.E2EBenchmarks** — `benchmarks/squirix.e2e.benchmarks/` (public-client regression guards)
  - See [docs/benchmarks/read-path-optimization-notes.md](docs/benchmarks/read-path-optimization-notes.md) for
    maintainer methodology notes.
- Project directory names are lowercase dotted; `.csproj` filenames remain PascalCase.

## .NET API (v0.1)

The v0.1 application surface is narrow:

- **`SquirixClient.ConnectAsync(...)`** — connect to server bootstrap endpoints
- **`ISquirixClient.GetCacheAsync<T>(...)`** — resolve a typed cache handle
- **`ICache<T>`** — async key/value and expiration operations:
  - `AddAsync`, `TryAddAsync`, `SetAsync`, `UpdateAsync`
  - `GetValueAsync`, `GetEntryAsync`, `GetExpirationAsync`, `GetOrAddAsync`
  - `RemoveAsync`, `TouchAsync` (`TimeSpan` or `DateTimeOffset`), `RemoveExpirationAsync`
- **`CacheEntryOptions`** — optional expiration on writes (`AddAsync`, `TryAddAsync`, `SetAsync`, `GetOrAddAsync`)
- **`CacheEntry<T>`**, **`CacheValueResult<T>`**, **`CacheEntryResult<T>`**, **`CacheExpirationResult`** — read models
  and lookup results
- **`SquirixOptions`** — endpoints, API key, bearer token provider, serializer
- **`ISquirixSerializer`** — optional custom serialization

```csharp
using System;
using System.Threading;
using Squirix;

internal sealed record User(string Name);

var cancellationToken = CancellationToken.None;

await using var client = await SquirixClient.ConnectAsync(
    "https://localhost:5001",
    cancellationToken);
ICache<User> cache = await client.GetCacheAsync<User>("users", cancellationToken);

await cache.SetAsync(
    "user:42",
    new User("Alice"),
    cancellationToken: cancellationToken);

var lookup = await cache.GetValueAsync("user:42", cancellationToken);
Console.WriteLine(lookup.Found ? lookup.Value?.Name ?? "<null>" : "<missing>");

await cache.SetAsync(
    "session:99",
    new User("Bob"),
    new CacheEntryOptions { Expiration = TimeSpan.FromMinutes(30) },
    cancellationToken);

_ = await cache.RemoveExpirationAsync("session:99", cancellationToken);
```

- **Writes** take a typed value plus optional `CacheEntryOptions` (`Expiration` or `ExpiresAt`, not both). There is no
  `CacheEntry<T>` parameter overload on `AddAsync`, `TryAddAsync`, or `SetAsync`.
- **`CacheEntry<T>`** is returned by read APIs such as `GetEntryAsync`; it is not a write parameter on the exported
  surface.
- Prefer **`GetValueAsync`** for reads. It returns explicit presence through `CacheValueResult<T>`. There is no
  `GetValueOrDefaultAsync` on `ICache<T>`; do not treat a default/null return as proof of absence when null is a valid
  cached value.
- **`ContainsAsync`** is intentionally not part of the v0.1 public API. In a distributed cache, existence can become
  stale immediately; use `GetValueAsync` when you need a fresh answer.
- **`RemoveExpirationAsync`** removes expiration from a live entry only (maps to the gRPC `Persist` RPC on the server).
  It does not mean durable persistence to disk. Returns `false` when the key is missing, expired, or already
  non-expiring.
- Do not use `await using` on a cache variable: `ICache<T>` is not async-disposable. See
  [`src/squirix/ICache.cs`](src/squirix/ICache.cs).
- The `Squirix` package is async-only (`ICache<T>`).
- Client applications reference the `Squirix` package. Server process deployments use the `Squirix.Server.Host`
  executable backed by the `Squirix.Server` runtime library.
- There is no local, embedded, or in-process client mode.
- Persistence, journal, snapshots, recovery, static ownership, health/readiness endpoints, and server hosting are
  server configuration concerns.
- Internal runtime/factory services are not compatibility surfaces.

## HTTP routes

Health and admin endpoints are commonly reached through the HTTP/1.1 sidecar port (`SQUIRIX_HTTP1_PORT`).

Health:

- `GET /health`
- `GET /health/live`
- `GET /health/ready`
- `GET /health/ready/details`

Admin:

- `GET /admin/whoami`
- `GET /admin/owner/{key}`
- `GET /admin/ring`

Current REST cache routes:

- `PUT /api/v1/cache/{key}`
- `GET /api/v1/cache/{key}`
- `HEAD /api/v1/cache/{key}`
- `DELETE /api/v1/cache/{key}`

Metrics (Prometheus text scrape, enabled by default):

- `GET /metrics`

Disable or reconfigure through the `PrometheusMetrics` section in `Squirix.settings.json`. See
[configuration](docs/configuration.md#prometheus-metrics-squirixsettingsjson).

## REST and gRPC notes

- REST exposes a subset of cache operations (see HTTP routes above).
- Application gRPC uses [`SquirixCache.proto`](src/shared/transport/grpc/Protos/SquirixCache.proto).
- The v0.1 `Squirix` client SDK maps basic `ICache<T>` operations to the shared `SquirixCache.proto` contract.

## Consistency

squirix is a single-owner system with best-effort routing and preview-state durability coverage. Do not treat the
current preview as a formally linearizable or strongly consistent distributed system.

- Single-key operations execute on one owner node.
- Multi-owner operations are not distributed transactions.

See [docs/consistency.md](docs/consistency.md) for semantics and caveats.

## Durability and recovery

squirix server nodes persist state with journal segments, snapshots, and a manifest.

- Durability is per node. There is no replication.
- journal, snapshots, compaction, and recovery semantics are still evolving across preview releases.
- Exported v0.1 `ICache<T>` mutations are journal/recovery-backed on the server when persistence is enabled.
- Treat serializer choice as part of the persisted state contract.

Before production-like use, read:

- [Operational runbook](docs/operational-runbook.md)
- [Storage maintenance](docs/storage-maintenance.md)
- [Diagnostics](docs/diagnostics.md)

## Limits and defaults

Current defaults in the built-in node host:

- REST payload size limit: about `1 MiB` per request body
- Journal max segment: `64 MiB`
- Snapshot trigger interval: `5` minutes
- Compaction thresholds: min tail `2` segments or `64 MiB`, min gap `2` minutes

## Configuration

See [docs/configuration.md](docs/configuration.md) for the current option shapes and environment variables.

Example static ownership section:

```json
{
    "Squirix": {
        "Cluster": {
            "ClusterId": "dev-core",
            "NodeId": "node-a",
            "Url": "http://localhost:5001",
            "VirtualNodes": 128,
            "Peers": [
                {
                    "NodeId": "node-a",
                    "Url": "http://localhost:5001"
                },
                {
                    "NodeId": "node-b",
                    "Url": "http://localhost:5002"
                }
            ]
        }
    }
}
```

## Serializer customization

- Server nodes use the default JSON encoder for journal, snapshots, and wire adapters.
- Remote clients set `SquirixOptions.Serializer` when calling `SquirixClient.ConnectAsync(...)`; each session keeps its
  own serializer.
- Serializer choice affects persisted payload compatibility.

See [docs/serialization.md](docs/serialization.md).

## Troubleshooting

- Browser gets `ERR_EMPTY_RESPONSE`: Use the HTTP/1.1 sidecar port, not the internal h2c port.
- Browser says an HTTP/1.x request was sent to an HTTP/2-only endpoint: You hit port `5000`. Use the sidecar port.
- `401` or `403` on `/admin`: API keys or JWT are enabled and your request is missing credentials.
- gRPC returns `Unauthenticated` while REST/admin return `401` for missing or invalid credentials under the same auth policy.
- Readiness fails while liveness is OK: Recovery may still be running. Check logs and `/health/ready/details`.

## Contact

- Email: [admin@squirix.io](mailto:admin@squirix.io)
- Slack: [Squirix workspace](https://squirix.slack.com)

## More docs

- [contributing.md](contributing.md)
