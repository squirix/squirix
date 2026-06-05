# Squirix read path optimization notes

<!-- markdownlint-disable MD013 -->

Maintainer notes for benchmark methodology and read-path investigation. Not part of the v0.1 product documentation set.

Last updated: 2026-06-06 (Windows workstation, Release builds).

## Benchmark methodology

`PublicSdkOperationsBenchmarks` is the basic end-to-end benchmark set for the public Squirix SDK against a real single-node Squirix server.

Memory/Garnet rows are intentionally not part of this basic e2e set. They measure different product layers and should live only in a deliberately named comparison benchmark if that
comparison is needed later.

### Project split

| Project                 | Scope                                            | `InternalsVisibleTo`       |
|-------------------------|--------------------------------------------------|----------------------------|
| `Squirix.E2EBenchmarks` | Public SDK + real node                           | No                         |
| `Squirix.Benchmarks`    | Server pipeline + raw gRPC stubs (TestKit hooks) | Yes (`Squirix.Benchmarks`) |

E2E benchmark infrastructure lives only as physical files under `benchmarks/squirix.e2e.benchmarks/` (no linked compile from other projects).

---

## Benchmark commands

### Layer breakdown (internal — `Squirix.Benchmarks`)

```text
dotnet run --project benchmarks\squirix.benchmarks\Squirix.Benchmarks.csproj -c Release -- --filter *ReadPathBreakdownBenchmarks* --join --warmupCount 1 --iterationCount 3
```

| Benchmark method                     | Layer isolated                                                          |
|--------------------------------------|-------------------------------------------------------------------------|
| `SquirixServerPipelineReadBatched`   | Server adapter + decorators + store, **no network, no SDK**             |
| `SquirixGrpcTransportReadBatched`    | Raw gRPC `GetValue` stub + server pipeline, **no public SDK**           |
| `SquirixClientPoolPolicyReadBatched` | `ClientPool` + `CallPolicy` + generated gRPC stub, **no public facade** |

### End-to-end public SDK baselines (`Squirix.E2EBenchmarks`)

Public client only — no `InternalsVisibleTo`, no in-process server DI:

```text
dotnet run --project benchmarks\squirix.e2e.benchmarks\Squirix.E2EBenchmarks.csproj -c Release -- --filter *PublicSdkOperationsBenchmarks* --join --warmupCount 1 --iterationCount 3
```

| Benchmark method                | Layer isolated                                            |
|---------------------------------|-----------------------------------------------------------|
| `ReadExistingValueBatched`      | Existing-key `GetValueAsync`                              |
| `ReadMissingValueBatched`       | Missing-key `GetValueAsync`                               |
| `WriteNewValueBatched`          | Unique-key `SetAsync`                                     |
| `OverwriteExistingValueBatched` | Existing-key overwrite `SetAsync`                         |
| `GetOrAddExistingValueBatched`  | Hit `GetOrAddAsync`, factory stays cold                   |
| `GetOrAddMissingValueBatched`   | Miss `GetOrAddAsync`, factory + insert path               |
| `ReadLiveExpiringValueBatched`  | Existing-key read with live expiration metadata           |
| `MixedReadWriteBatched`         | Deterministic 90 percent read / 10 percent write workload |

Compare breakdown `SquirixGrpcTransportReadBatched` with e2e `ReadExistingValueBatched` to estimate public SDK tax on top of transport.

---

## Measured numbers

Settings: `ReadBatch = 1_024` ops per invoke, `--warmupCount 1 --iterationCount 3`, Release, loopback.

### Internal breakdown (`ReadPathBreakdownBenchmarks`) — 2026-06-06

| Layer                              | Mean per read | Notes                                     |
|------------------------------------|--------------:|-------------------------------------------|
| `SquirixServerPipelineReadBatched` |   **2.49 µs** | Includes decorator stack + physical store |
| `SquirixGrpcTransportReadBatched`  |  **159.9 µs** | Uses `GetValue` RPC (not entry `Get`)     |

Reading the split:

- Server decorators + store ≈ **2.5 µs** — small vs transport.
- Adding gRPC/HTTP/2 ≈ **+157 µs** (`GrpcTransport` − `ServerPipeline`).

### E2E public SDK baseline (`PublicSdkOperationsBenchmarks`)

| Layer                      |             Latest directional result |            Allocated |
|----------------------------|--------------------------------------:|---------------------:|
| `ReadExistingValueBatched` | **118-140 µs** depending on run shape | **~12.3-12.8 KB/op** |

The public value read now uses the value-only `GetValue` RPC. Same-process breakdown shows public SDK latency is currently at raw gRPC level within short-run noise; the remaining
public delta is mainly allocation, roughly **+1.25 KB/op** over raw gRPC.

### Regression tracking

After each optimization step, add a row here. Primary e2e guardrail: **`ReadExistingValueBatched`**.

| Step                  | `ReadExistingValueBatched` | `SquirixGrpcTransportReadBatched` | `SquirixServerPipelineReadBatched` |
|-----------------------|---------------------------:|----------------------------------:|-----------------------------------:|
| Baseline (2026-06-06) |                    ~194 µs |                           ~160 µs |                            ~2.5 µs |

---

## Implementation status

### Step 1 — Value-only RPC + client wiring

| Item                                                                             | Status |
|----------------------------------------------------------------------------------|--------|
| `GetValueRequest` / `GetValueResponse` + `rpc GetValue` in `SquirixCache.proto`  | Done   |
| `ICacheApi.TryGetValueAsync`, `RoutedCacheApi`, `SquirixServiceAdapter.GetValue` | Done   |
| `BenchmarkRawGrpcCache` uses `GetValueAsync`                                     | Done   |
| `RemoteCache<T>.GetValueAsync` → `GetValue` RPC (not `Get` / entry)              | Done   |
| `ClusteredCache.TryGetValueAsync` routes via `OwnerFor`                          | Done   |
| `ClusteredCache.GetValueAsync` value-only path (avoid entry fetch)               | Done   |
| `ClientCache.GetValueAsync` → `TryGetValueAsync` / `_read.TryGetValueAsync`      | Done   |

Short e2e result after this step: `ReadExistingValueBatched` ≈ **126.7 µs**, **15.53 KB** allocated per operation.

### Step 2 — Compact `CacheValue` wire format

Done for the value-only `GetValue` RPC. Public `SetAsync` / `TryAddAsync` now also have compact `InsertValue` / `TryInsertValue` RPCs. `GetEntry` and remove previous-value payloads
still use the entry/struct path.

Short e2e result after this step:

| Benchmark                      |                     Before |                      After |
|--------------------------------|---------------------------:|---------------------------:|
| `ReadExistingValueBatched`     | **126.7 µs**, **15.53 KB** | **118.7 µs**, **12.46 KB** |
| `ReadMissingValueBatched`      | **118.1 µs**, **12.69 KB** | **114.5 µs**, **12.69 KB** |
| `ReadLiveExpiringValueBatched` | not measured in step 1 run | **114.2 µs**, **12.55 KB** |

### Step 3 — Server decorator / pipeline micro-opts (partial)

| Item                                                                                                                         | Status                              |
|------------------------------------------------------------------------------------------------------------------------------|-------------------------------------|
| `BackpressureCacheDecorator`: sync fast-path for `TryGetValueAsync` (`IsCompletedSuccessfully`, lease dispose without await) | Done                                |
| `CallPolicy.ConfigurePerAttemptTimeout`: skip redundant per-attempt timer when ambient RPC budget is the binding constraint  | Done                                |
| `TracingCacheDecorator` / `MetricsCacheDecorator` / `DomainErrorMappingCacheDecorator`: sync fast-path on completed reads    | **Not done** — still always `await` |

Use `SquirixServerPipelineReadBatched` before/after further decorator work.

### Step 4+ — Client `CallPolicy` / gRPC fixed costs

Per read on the public path: semaphore wait, timeout tokens, retry machinery, unary gRPC framing, and `GetValueAsync`
async layers. Quantify with e2e vs breakdown delta after step 1. (Internal breakdown benchmarks may use
`BenchmarkNodeReadSurface.GetValueOrDefaultAsync`; that helper is not part of the exported `ICache<T>` surface.)

Current breakdown signal:

| Benchmark                          |         Result |
|------------------------------------|---------------:|
| `SquirixServerPipelineReadBatched` |   **2.534 µs** |
| `SquirixGrpcTransportReadBatched`  | **142.482 µs** |

The server pipeline is not the read bottleneck in the current single-node path. Transport/request overhead dominates.

Additional decode isolation check:

| Benchmark                              |         Result |
|----------------------------------------|---------------:|
| `SquirixGrpcTransportReadBatched`      | **132.946 µs** |
| `SquirixGrpcTransportFoundOnlyBatched` | **129.470 µs** |
| `SquirixServerPipelineReadBatched`     |   **2.781 µs** |

`FoundOnly` avoids client-side value decoding but allocates and runs almost the same as the normal raw gRPC read. This rules out `CacheValue` decode as the next meaningful target.
The remaining read cost is dominated by unary gRPC request/response overhead and fixed transport machinery.

Additional request allocation isolation check:

| Benchmark                                           |                                                    Result |
|-----------------------------------------------------|----------------------------------------------------------:|
| `SquirixGrpcTransportFoundOnlyBatched`              | **134.823 µs**, about **11.58 KB/op** from GC diagnostics |
| `SquirixGrpcTransportFoundOnlyReusedRequestBatched` | **133.077 µs**, about **11.54 KB/op** from GC diagnostics |

Reusing the protobuf request instance saves only about **40 B/op** in this sequential benchmark. Product request pooling is not worth the complexity or concurrency risk at this
point.

Same-process public SDK delta check:

| Benchmark                         |                                                    Result |
|-----------------------------------|----------------------------------------------------------:|
| `SquirixGrpcTransportReadBatched` | **123.082 µs**, about **11.57 KB/op** from GC diagnostics |
| `SquirixPublicSdkReadBatched`     | **143.645 µs**, about **12.99 KB/op** from GC diagnostics |

Public SDK overhead against the same node is roughly **+20 µs** and **+1.4 KB/op** in this short run. This is now measurable enough to inspect client failover/policy wrappers, but
still much smaller than the fixed raw unary gRPC cost.

Client policy isolation check:

| Benchmark                                         |                                                 Result |
|---------------------------------------------------|-------------------------------------------------------:|
| `BootstrapFailoverCompletedValueTaskBatched`      |                              **15.223 ns**, **0 B/op** |
| `CallPolicyCompletedValueTaskBatched`             | **260.545 ns**, about **144 B/op** from GC diagnostics |
| `BootstrapAndCallPolicyCompletedValueTaskBatched` | **278.299 ns**, about **144 B/op** from GC diagnostics |

`BootstrapEndpointFailover` is effectively free on the one-bootstrap-node happy path. `CallPolicy` has a small fixed allocation cost, likely from the per-call attempt cancellation
source/timer path, but this is far below the raw unary gRPC allocation and does not explain the full public SDK delta by itself.

State-overload client wrapper check:

| Benchmark                                         |                                Before |                                 After |
|---------------------------------------------------|--------------------------------------:|--------------------------------------:|
| `BootstrapAndCallPolicyCompletedValueTaskBatched` |                        **280.628 ns** |                        **278.928 ns** |
| `SquirixPublicSdkReadBatched`                     | **143.645 µs**, about **12.99 KB/op** | **137.346 µs**, about **12.83 KB/op** |
| `SquirixGrpcTransportReadBatched`                 | **123.082 µs**, about **11.57 KB/op** | **135.070 µs**, about **11.57 KB/op** |

The state-overload path reduces closure pressure only marginally. The latest same-process read run shows public SDK latency near raw gRPC within noise, while allocation remains
roughly **+1.25 KB/op** above raw gRPC.

Metric overhead isolation check:

| Benchmark                             |                                                 Result |
|---------------------------------------|-------------------------------------------------------:|
| `CallPolicyCompletedValueTaskBatched` | **259.112 ns**, about **144 B/op** from GC diagnostics |
| `QueueWaitMetricObserveBatched`       |                              **96.676 ns**, **0 B/op** |

Queue-wait metric recording is allocation-free in this benchmark. The remaining `CallPolicy` allocation is therefore not from metric tags; it is most likely the per-attempt
`CancellationTokenSource` required for timeout enforcement.

gRPC handler configuration check:

| Change                                                                                                                                                                                                                       | Result |
|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|--------|
| Removed benchmark-only `new SocketsHttpHandler()` override so public benchmark uses product default `GrpcCleartextHttp2.CreateChannelHandler` (`UseProxy=false`, `EnableMultipleHttp2Connections=true` for cleartext HTTP/2) | Done   |

Short same-process read result after this correction:

| Benchmark                         |                                Result |
|-----------------------------------|--------------------------------------:|
| `SquirixGrpcTransportReadBatched` | **141.206 µs**, about **11.57 KB/op** |
| `SquirixPublicSdkReadBatched`     | **140.026 µs**, about **12.83 KB/op** |

Latency is now indistinguishable within short-run noise. The remaining public SDK delta is allocation-only, roughly **+1.25 KB/op**, and is not caused by the loopback handler/proxy
setting.

Client wrapper allocation decomposition:

| Benchmark                            |                                Result |
|--------------------------------------|--------------------------------------:|
| `SquirixGrpcTransportReadBatched`    | **158.912 µs**, about **11.57 KB/op** |
| `SquirixClientPoolPolicyReadBatched` | **154.408 µs**, about **12.46 KB/op** |
| `SquirixPublicSdkReadBatched`        | **143.751 µs**, about **12.82 KB/op** |

Latency order is noise in this short run, but allocation is stable enough to locate the remaining delta:

- raw gRPC -> `ClientPool + CallPolicy`: about **+0.89 KB/op**
- `ClientPool + CallPolicy` -> public facade: about **+0.36 KB/op**
- raw gRPC -> public facade: about **+1.25 KB/op**

This narrows the next useful investigation to `ClientPool` / `CallPolicy` execution and gRPC call wrapper allocation. Broad public facade rewrites are not justified by the measured
**~0.36 KB/op** facade-only delta.

Latest narrow client wrapper check:

| Change                                                                                                                                                   | Result |
|----------------------------------------------------------------------------------------------------------------------------------------------------------|--------|
| `ClientScopedCache<T>.GetValueAsync` bypasses the generic `Forward<TResult>(Func<Task<TResult>>)` wrapper and directly checks disposal before delegating | Done   |

Short e2e result after this change:

| Benchmark                      |                     Before |                       After |
|--------------------------------|---------------------------:|----------------------------:|
| `ReadExistingValueBatched`     | **118.7 µs**, **12.46 KB** | **118.92 µs**, **12.35 KB** |
| `ReadMissingValueBatched`      | **114.5 µs**, **12.69 KB** | **112.37 µs**, **12.59 KB** |
| `ReadLiveExpiringValueBatched` | **114.2 µs**, **12.55 KB** |  **97.20 µs**, **12.49 KB** |

Treat the latency movement as noisy. The useful signal is that this removes only about **0.1 KB/op**, so wrapper closure allocation is not the main remaining cost.

Latest `RemoteCache.GetValueAsync` state-overload check:

| Change                                                                                                                                         | Result |
|------------------------------------------------------------------------------------------------------------------------------------------------|--------|
| Routed public `GetValueAsync` through a static state delegate and removed the per-call capturing async wrapper around the gRPC `GetValue` call | Done   |

Short same-process read result after this change:

| Benchmark                            |                                Before |                                 After |
|--------------------------------------|--------------------------------------:|--------------------------------------:|
| `SquirixGrpcTransportReadBatched`    | **158.912 µs**, about **11.57 KB/op** | **131.304 µs**, about **11.55 KB/op** |
| `SquirixClientPoolPolicyReadBatched` | **154.408 µs**, about **12.46 KB/op** | **139.033 µs**, about **12.45 KB/op** |
| `SquirixPublicSdkReadBatched`        | **143.751 µs**, about **12.82 KB/op** | **140.689 µs**, about **12.63 KB/op** |

Treat latency as noise because raw gRPC moved in the same run. The allocation signal is useful: this removes about **0.19 KB/op** from the public facade layer. The remaining raw
gRPC -> public SDK allocation delta is now about **+1.08 KB/op**, still mostly below the public facade.

Latest value-only write check:

| Change                                                                                                                                              | Result |
|-----------------------------------------------------------------------------------------------------------------------------------------------------|--------|
| Added compact `InsertValue` / `TryInsertValue` RPCs and routed public `SetAsync` / `TryAddAsync` through `CacheValue` instead of `Entry` + `Struct` | Done   |

Short e2e result after this change:

| Benchmark                     |                     Before |                      After |
|-------------------------------|---------------------------:|---------------------------:|
| `WriteNewValueBatched`        | **184.6 µs**, **19.47 KB** | **181.3 µs**, **19.01 KB** |
| `GetOrAddMissingValueBatched` | **296.3 µs**, **32.96 KB** | **311.9 µs**, **32.54 KB** |

Treat latency as noisy. The allocation drop is only about **0.4-0.5 KB/op**, so write payload shape is not the main remaining cost either.

---

## Main suspects (remaining work)

### 1. Per-attempt timeout allocation in `CallPolicy`

`CallPolicyCompletedValueTaskBatched` isolates about **144 B/op**. Queue-wait metrics are **0 B/op**, and state overloads barely moved the number. The remaining allocation is most
likely the per-attempt `CancellationTokenSource`.

Do not change this without focused cancellation/deadline coverage. This path owns retry timeout semantics.

### 2. Remaining public SDK allocation delta

Same-process reads show public SDK latency is near raw gRPC, but public SDK still allocates roughly **+1.25 KB/op**. This is not explained by request allocation, value decode,
metric tags, or benchmark handler configuration.

Next useful move here is allocation profiling, not another blind micro-change.

### 3. `GetOrAdd` miss path has two unary calls

Historical baseline was **445.8 µs / 59.36 KB**. After read-path fixes and value-only write RPCs, the miss path is closer to **296-312 µs / ~32.5 KB**. That shape is consistent
with one miss read RPC plus one write RPC. Without changing the current API/protocol shape, there is little left to remove.

### 4. Unary gRPC transport dominates

`SquirixGrpcTransportReadBatched` remains around **120-160 µs / ~11.5 KB/op** across short runs. This is the main read-path cost. Since batch/streaming/custom transport is out of
scope, treat this as a current floor.

### 5. Decorator / pipeline overhead

Read-path decorators under `src/squirix.server/Node/App/Decorators/`:

- `ValidationCacheDecorator`
- `TracingCacheDecorator`
- `MetricsCacheDecorator`
- `BackpressureCacheDecorator` (sync fast-path on `TryGetValue` only)
- `DeadlineCacheDecorator`
- `DomainErrorMappingCacheDecorator`

Profile here only if `SquirixServerPipelineReadBatched` regresses or if a low-latency in-process/local-owner mode becomes a product goal.

---

## Optimization plan

1. **Remove miss exceptions from `GetValue`**
    - Root cause found: public `RemoteCache<T>.GetValueAsync` still called `GetEntryOrDefaultAsync`, which used the old `Get` RPC. Missing keys produced server `NotFound`, then a
      client-side caught `RpcException` per miss.
    - Fix: route `RemoteCache<T>.GetValueAsync` through `GetValueRequest` / `GetValueResponse` so misses return `found=false`.
    - Verification: `ReadMissingValueBatched` no longer reports `Exceptions:` in BenchmarkDotNet diagnostics.
    - Result: **165.8 µs / 37.59 KB** -> **118.1 µs / 12.69 KB** in the short e2e run.

2. **Introduce compact `CacheValue` wire format**
    - Replaced `GetValueResponse.value` `google.protobuf.Struct` with a compact value message on the value-only RPC.
    - Added `oneof` for common scalars: `string`, `int64`, `double`, `bool`, `null`, plus fallback structured payload.
    - Keep `GetEntry` metadata path unchanged.
    - Result: `ReadExistingValueBatched` **126.7 µs / 15.53 KB** -> **118.7 µs / 12.46 KB** in the short e2e run.
    - Remaining allocation is still high, so the next pass should look below protobuf value shape: gRPC/client policy/request allocation and async wrappers.

3. **Split read/write payload paths cleanly**
    - Ensure `GetValue` never constructs `CacheEntry<T>` on client or server.
    - Keep expiration/version metadata only on `GetEntry`.
    - Re-run `ReadExistingValueBatched` and `ReadLiveExpiringValueBatched` after the split.

4. **Optimize `GetOrAdd` miss path**
    - Historical baseline: **445.8 µs**, **59.36 KB**.
    - Target flow on single node: `GetValue miss -> factory -> TryAdd/Insert -> return`.
    - Look for extra miss exceptions, duplicate reads, and unnecessary entry/struct serialization.
    - Re-measured after read-path fixes: **296.3 µs**, **32.96 KB**. That is close to one miss read plus one write RPC.
    - Added compact value-only write RPCs. Result: **311.9 µs**, **32.54 KB** in a short run; allocation barely moved and latency was noisy.
    - Conclusion: a meaningful `GetOrAdd` miss optimization needs to reduce round trips or introduce a different protocol shape. More protobuf payload trimming is unlikely to pay
      off.

5. **Quantify public SDK vs raw gRPC**
    - Re-run e2e + breakdown after each step.
    - Compare `ReadExistingValueBatched` with `SquirixGrpcTransportReadBatched` to estimate SDK overhead.
    - Applied a narrow client `CallPolicy` fast path: when the operation token is not cancellable and there is no ambient deadline, per-attempt timeout CTS is no longer linked to
      an uncancellable token.
    - Result: `ReadExistingValueBatched` **118.7 µs / 12.46 KB** -> **109.1 µs / 12.46 KB** in the short e2e run. Allocation did not move, so the remaining allocation is likely
      request/gRPC/async object churn rather than linked token registrations alone.
    - Applied a narrow `ClientScopedCache<T>.GetValueAsync` fast path that avoids the generic `Forward<TResult>` delegate wrapper for public reads.
    - Result: allocation moved only from roughly **12.46 KB** to **12.35 KB** on existing reads in a short run. This is too small to justify broad wrapper rewrites as the next main
      optimization.
    - Added `SquirixGrpcTransportFoundOnlyBatched` to separate raw unary transport from client-side value decode.
    - Result: normal raw gRPC read **132.946 µs**, found-only raw gRPC read **129.470 µs**. Decode is not the bottleneck.
    - Added `SquirixGrpcTransportFoundOnlyReusedRequestBatched` to isolate per-call `GetValueRequest` allocation.
    - Result: request reuse saved only about **40 B/op**. Do not add product request pooling based on this signal.
    - Added `SquirixPublicSdkReadBatched` to compare public SDK and raw gRPC inside the same node benchmark.
    - Result: public SDK read **143.645 µs / ~12.99 KB**, raw gRPC read **123.082 µs / ~11.57 KB**. The client SDK tax is measurable but not the dominant cost.
    - Added `ClientPolicyOverheadBenchmarks` to isolate `BootstrapEndpointFailover` and `CallPolicy` without gRPC.
    - Result: bootstrap failover is about **15 ns / 0 B**; call policy is about **260 ns / 144 B**. Optimizing this can trim allocations, but will not materially change read
      latency.
    - Added state overloads for `BootstrapEndpointFailover` and `CallPolicy`, then routed `RemoteCache.ExecuteAsync` through them.
    - Result: wrapper-only benchmark moved **280.628 ns -> 278.928 ns**. Same-process public read moved **143.645 µs / ~12.99 KB -> 137.346 µs / ~12.83 KB**, but raw gRPC also
      moved in the same run, so treat latency as noise. Allocation improved only slightly.
    - Added `QueueWaitMetricObserveBatched` to isolate metric overhead.
    - Result: queue-wait metric observe is **96.676 ns / 0 B**. Do not target metric tags for allocation reduction.
    - Corrected benchmark runtime so public benchmarks use product default gRPC handler settings instead of a plain `SocketsHttpHandler`.
    - Result: public SDK latency is now at raw gRPC level in the same-process read benchmark. Handler configuration was distorting benchmark latency, but not the remaining public
      allocation delta.
    - Added `SquirixClientPoolPolicyReadBatched` to place a diagnostic row between raw generated gRPC and the public SDK facade.
    - Result: raw gRPC **~11.57 KB/op**, `ClientPool + CallPolicy` **~12.46 KB/op**, public SDK **~12.82 KB/op**. Most of the remaining allocation delta sits below the public
      facade, around client pool / policy / unary call wrapper execution.
    - Routed `RemoteCache.GetValueAsync` through the state-overload execution path and removed its capturing async wrapper.
    - Result: public SDK allocation moved **~12.82 KB/op -> ~12.63 KB/op** in the same-process breakdown. This confirms facade wrapper allocation exists but is small; the remaining
      allocation delta is still mainly `ClientPool` / `CallPolicy` / unary gRPC wrapper cost.

6. **Only then tune decorators**
    - Server pipeline was previously around **2.5 µs**.
    - Optimize `TracingCacheDecorator`, `MetricsCacheDecorator`, `DomainErrorMappingCacheDecorator`, and `DeadlineCacheDecorator` only if breakdown shows they matter after
      transport/payload work.

7. **Optional remote-owner benchmark**
    - Add a two-node e2e row only when cross-node tax needs continuous tracking.
    - Keep it separate from the basic single-node public SDK baselines.

---

## Benchmark guardrail files

- **E2E:** `benchmarks/squirix.e2e.benchmarks/PublicSdkOperationsBenchmarks.cs`
- **Breakdown:** `benchmarks/squirix.benchmarks/ReadPathBreakdownBenchmarks.cs`

Quick run (default iteration count):

```text
dotnet run --project benchmarks\squirix.e2e.benchmarks\Squirix.E2EBenchmarks.csproj -c Release -- --filter *PublicSdkOperationsBenchmarks*
dotnet run --project benchmarks\squirix.benchmarks\Squirix.Benchmarks.csproj -c Release -- --filter *ReadPathBreakdownBenchmarks*
```

On Windows, if BenchmarkDotNet fails writing under `Application Data`:

```text
$env:BMDN_ArtifactsPath = "c:\Users\HOME\Source\Repos\squirix\squirix\BenchmarkDotNet.Artifacts"
```
