# Getting started

This guide walks through running a squirix server and connecting a .NET client.

Already using ASP.NET Core HybridCache? See [switching from Redis or Garnet to Squirix](distributed-cache.md#switch-from-redis-or-garnet-to-squirix)
for the preview.11 adapter.

Prerequisites:

- [.NET 10 SDK](https://dotnet.microsoft.com/download/dotnet/10.0) for applications consuming the NuGet packages.
- Building repository sources uses the SDK selected by [`global.json`](../global.json).
- Docker Desktop with Linux containers for the Docker examples; the client example requires Compose 2.17 or newer.
- For local HTTPS (tests, benchmarks, examples; not Docker): `dotnet dev-certs https --trust`

## 1. Run a development server

Choose one of the server options below. Leave that server running while using the client; stop it before trying another
option on the same port. The local console example uses the global tool. Docker has a separate client example that
handles its development JWT and certificate setup.

### NuGet global tool

```bash
dotnet tool install --global squirix.server.tool --version 0.1.0-preview.10
squirix-server run
```

The commands in this guide use the published `0.1.0-preview.10` packages. See the [root README](../README.md) for the current package version.

The host listens on `https://localhost:5001` by default (loopback bind), runs as an in-memory cache, and prints
ready-to-use client and operational endpoint URLs. **No JWT is required** on this default URL — any local process can
access the cache API. This is a development convenience, not production hardening. See
[server-mode.md — Loopback development default](server-mode.md#loopback-development-default-not-production-posture).

To use journal/snapshot persistence instead, stop the in-memory server with Ctrl+C and restart it with:

```bash
squirix-server run --persist --data-dir data
```

Health probes use the same HTTPS listener (local tool default port **5001**):

```bash
curl https://localhost:5001/health
curl https://localhost:5001/metrics
```

`/metrics` accepts loopback connections without a token even when JWT authentication is enabled.

If the server cannot bind to port 5001, or the client reports a TLS chain error such as `PartialChain`,
check that the CLI started successfully and that the endpoint belongs to that process. A container or another
server already using the port may answer the health probe with a different certificate. If port 5001 is in use,
start the CLI with `squirix-server run --urls https://localhost:5443` (if 5443 is free), then use that URL in the
health probes and the client example. Keep TLS verification enabled.

### Docker

> **Development only.** Examples below use the public test JWT key `ZGV2LXNxdWlyaXgtZG9ja2VyLWp3dC1rZXkhISEhISE=` and bundled dev
> HTTPS/mTLS certificates. Do not reuse them outside a local machine.
> See [containerization.md](containerization.md#security).

Run Docker commands from the root of a repository checkout. The multiline commands below use Bash syntax.
The source image builds that checkout; the release image installs the published tool version selected by its build argument.

Single-container examples start in the default **ephemeral** mode (in-memory cache). The three-node `docker compose`
example enables persistence with `--persist --data-dir /data` and named volumes.

```bash
docker build -f docker/Dockerfile -t squirix-server .
docker run --rm \
  -p 5000:5000 \
  -e SQUIRIX_JWT_SIGNING_KEY=ZGV2LXNxdWlyaXgtZG9ja2VyLWp3dC1rZXkhISEhISE= \
  -e SQUIRIX_JWT_ISSUER=https://squirix.docker.dev \
  -e SQUIRIX_JWT_AUDIENCE=squirix \
  squirix-server run --urls https://0.0.0.0:5000
```

Port **5000** is the primary HTTPS listener (gRPC, `/health`, `/metrics`). Images ship a bundled development
HTTPS certificate; use `curl -k` from the host. When JWT is configured, pass a bearer token for `/metrics` scrapes from
outside the container.

Release image (NuGet tool; Dockerfile default `SQUIRIX_VERSION` matches the published package):

```bash
docker build -f docker/Dockerfile.release -t squirix-server:release .
docker run --rm \
  -p 5000:5000 \
  -e SQUIRIX_JWT_SIGNING_KEY=ZGV2LXNxdWlyaXgtZG9ja2VyLWp3dC1rZXkhISEhISE= \
  -e SQUIRIX_JWT_ISSUER=https://squirix.docker.dev \
  -e SQUIRIX_JWT_AUDIENCE=squirix \
  squirix-server:release run --urls https://0.0.0.0:5000
```

Three-node RF=3 cluster (`docker compose up -d --build` in `docker/`): nodes A, B and C on
`https://localhost:5001`, `https://localhost:5002` and `https://localhost:5003` (host ports map to container **5000**).
See [containerization.md](containerization.md).

### From this repository

Run this alternative from the repository root with its selected SDK. It starts the server built from the current
checkout, which may contain unreleased changes:

```bash
dotnet run --project src/squirix.server.host/Squirix.Server.Host.csproj -- run
```

## 2. Add the client SDK

Keep the server running. In a second terminal, create a console app and install the client SDK:

```bash
dotnet new console --framework net10.0 -o SquirixDemo
dotnet add SquirixDemo/SquirixDemo.csproj package squirix --version 0.1.0-preview.10
```

## 3. Connect and use a typed cache

Use the HTTPS gRPC endpoint from the host output.

**Local tool or `dotnet run`** (default `https://localhost:5001`, no JWT unless you configure auth):

Replace `SquirixDemo/Program.cs` with:

```csharp
using System;
using System.Threading;
using Squirix.Client;

using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(15));
var cancellationToken = timeout.Token;

await using var client = await SquirixClient.ConnectAsync(new Uri("https://localhost:5001"), cancellationToken);

var cache = await client.GetCacheAsync<string>("demo", cancellationToken);
await cache.SetAsync("greeting", "hello", null, cancellationToken);

var lookup = await cache.GetValueAsync("greeting", cancellationToken);
Console.WriteLine(lookup.Found ? lookup.Value : "<missing>");
```

Run the app:

```bash
dotnet run --project SquirixDemo
```

Expected output: `hello`.

This example uses a string. For application-specific value types, register a source-generated JSON context as shown in
[Serialization](serialization.md#application-value-types).

**Docker Compose:** use the [complete client example](../samples/docker-client/README.md).
It generates a short-lived development JWT and trusts the cluster's HTTPS certificate only inside its own container.
Follow that example's startup, readiness checks, and client command in order. A container being listed as running
does not mean its replica groups are ready.

Expected output: `Found=True; Value=hello from Docker`.
For a reproducible leader-crash scenario, see the [failover demo](../samples/docker-client/FAILOVER.md).
The client connects to Compose DNS names on port 5000. Host applications instead use the published ports 5001–5003,
`options.BearerTokenProvider`, and a properly trusted server certificate. The public client has no certificate-validation
override option; do not disable TLS verification. See [HTTPS in containers](containerization.md#https-in-containers).

Multiple bootstrap endpoints (HA front door, not shards):

```csharp
using System;
using System.Threading;
using Squirix.Client;

using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(15));
var cancellationToken = timeout.Token;

await using var client = await SquirixClient.ConnectAsync(
    static options =>
    {
        options.Endpoints.Add(new Uri("https://cache-a.example.internal:5001"));
        options.Endpoints.Add(new Uri("https://cache-b.example.internal:5002"));
    },
    cancellationToken);
```

See [bootstrap client failover](bootstrap-client-failover.md) and [configuration](configuration.md).

## CLI reference

```bash
squirix-server init [--settings Squirix.settings.json]
squirix-server validate-config --settings Squirix.settings.json [--strict]
squirix-server doctor [--settings Squirix.settings.json]
squirix-server version
```

Square brackets in the command synopsis mark optional arguments; omit the brackets when running a command.
`init` creates a settings file once and refuses to overwrite an existing file. `--strict` performs optional-section
validation on `validate-config`; it does not add that validation to `doctor` or `run`.

`run` supports `--urls`, `--persist`, `--data-dir` (with `--persist`), `--settings`, and `--enable-replication`.
Run `validate-config --settings Squirix.settings.json --strict` before starting the server; passing `--strict` to
`run` does not perform that validation. Without `--settings`, the host discovers `Squirix.settings.json` or
`squirix.settings.json` in the working directory and application directory.

Use paths such as `Squirix.settings.json` or `config/node.json`; operator-supplied paths containing `.` or `..`
segments are rejected. Relative paths are resolved against the process working directory.

For settings outside the standard discovery locations, `--settings` selects cluster settings only. Optional runtime
sections still use standard file discovery; see [file discovery](configuration.md#file-discovery).

## Next steps

- Embed the server in ASP.NET Core: [server mode](server-mode.md)
- Tune cluster / memory-pressure / metrics settings: [configuration](configuration.md)
- Understand routing and consistency: [clustering](clustering.md)
