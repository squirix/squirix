# Squirix E2E Benchmarks (`Squirix.E2EBenchmarks`)

The E2E benchmark suite measures the public `ICache<T>` client API against real Squirix server nodes. It is intended
for diagnostics and regression investigation, not marketing numbers.

## Project

The benchmarks live in project **Squirix.E2EBenchmarks** on disk at:

```text
benchmarks/squirix.e2e.benchmarks/Squirix.E2EBenchmarks.csproj
```

The suite uses BenchmarkDotNet and starts real in-process Squirix nodes through the existing server testkit.

## Run

Run all E2E benchmarks:

```bash
dotnet run -c Release --project benchmarks/squirix.e2e.benchmarks
```

Run one small smoke benchmark:

```bash
SQUIRIX_E2E_BENCHMARK_SMOKE=1 dotnet run -c Release --project benchmarks/squirix.e2e.benchmarks -- --filter '*GetValueShouldReturnHitAsync*' --warmupCount 1 --iterationCount 1
```

Run a longer local job:

```bash
SQUIRIX_E2E_BENCHMARK_LONG=1 dotnet run -c Release --project benchmarks/squirix.e2e.benchmarks
```

## Filters

BenchmarkDotNet filters can target method names:

```bash
dotnet run -c Release --project benchmarks/squirix.e2e.benchmarks -- --filter '*GetValue*'
dotnet run -c Release --project benchmarks/squirix.e2e.benchmarks -- --filter '*RemoteOwnerReadMostly*'
dotnet run -c Release --project benchmarks/squirix.e2e.benchmarks -- --filter '*Touch*'
dotnet run -c Release --project benchmarks/squirix.e2e.benchmarks -- --filter '*GetOrAdd*'
```

Topology and value-shape differences appear in the `Scenario` parameter column. Compare rows with the same benchmark
method and different scenario values.

## Scenario Matrix

Topologies:

- `SingleNode`
- `TwoNodeLocalOwner`
- `TwoNodeRemoteOwner`
- `TwoNodeUniformKeys`
- `TwoNodeHotKeys`

Value shapes:

- `PrimitiveLong`
- `SmallString`
- `SmallCustomRecord`
- `NestedCustomClass`

Durability:

- `Ephemeral` — in-memory server (default)
- `Persistence` — journal/snapshot stack enabled

The full scenario matrix uses `Ephemeral` only. Compare both modes with:

```bash
SQUIRIX_E2E_BENCHMARK_SMOKE=1 dotnet run -c Release --project benchmarks/squirix.e2e.benchmarks -- --filter '*DurabilityComparison*'
```

Or include both modes in the full matrix:

```bash
SQUIRIX_E2E_BENCHMARK_DURABILITY=1 dotnet run -c Release --project benchmarks/squirix.e2e.benchmarks
```

Client SDK benchmarks (`Squirix.Benchmarks`) expose the same modes through `ComparisonBenchmarks`.

## Wire allocation matrix

Single-node allocation baselines for every public `ICache<T>` operation on the gRPC wire path.
Use this matrix to compare `develop` against wire-encoding changes (for example `refactor/address-wire-alloc`).

Benchmark classes:

- `WireScalarAllocBenchmarks` — `string` values (scalar wire path)
- `WireStructuredAllocBenchmarks` — `BenchmarkUserProfile` values (structured payload path)

Each class runs 13 benchmark methods (`Batch = 512`, `[MemoryDiagnoser]`) covering all happy-path `ICache<T>` APIs.
BenchmarkDotNet parametrizes `DurabilityMode` (`Ephemeral` vs `Persistence` / `UsePersistence()`), producing **52 rows**
per full run (13 methods × 2 value shapes × 2 durability modes).

Results table: [wire-alloc-baseline.md](wire-alloc-baseline.md) (four sections: scalar/structured × ephemeral/persistence).

Smoke run (fast, one read path, persistence only):

```powershell
dotnet run -c Release --project benchmarks/squirix.e2e.benchmarks -- `
  --filter '*Wire*AllocBenchmarks.GetValueAsync*' `
  --filter '*Persistence*' `
  --warmupCount 1 `
  --iterationCount 3
```

Full matrix (ephemeral + persistence; expect several minutes — persistence writes are slower and `Remove*` benchmarks
re-seed 512 keys in `IterationSetup`):

```powershell
dotnet run -c Release --project benchmarks/squirix.e2e.benchmarks -- `
  --filter '*Wire*AllocBenchmarks*' `
  --warmupCount 1 `
  --iterationCount 3 `
  --exporters json
```

Persistence-only subset:

```powershell
dotnet run -c Release --project benchmarks/squirix.e2e.benchmarks -- `
  --filter '*Wire*AllocBenchmarks*' `
  --filter '*Persistence*' `
  --warmupCount 1 `
  --iterationCount 3 `
  --exporters json
```

Use `--iterationCount 3` or higher. `--iterationCount 1` with `[MinIterationTime(150)]` often produces empty BDN rows.

Update the committed baseline tables from BenchmarkDotNet JSON (one report file per benchmark class):

```powershell
./tools/benchmarks/update-wire-alloc-table.ps1 `
  -ArtifactsDir BenchmarkDotNet.Artifacts/results `
  -GitSha (git rev-parse --short HEAD) `
  -Branch (git branch --show-current)
```

## Benchmark Groups

Basic operations:

- `SetShouldStoreValueAsync`
- `GetValueShouldReturnHitAsync`
- `GetValueShouldReturnMissAsync`
- `GetEntryShouldReturnHitAsync`
- `TryAddShouldAddMissingValueAsync`
- `TryAddReturnsFalseForExistingValueAsync`
- `AddShouldStoreMissingValueAsync`
- `AddShouldThrowForExistingValueAsync`
- `UpdateShouldModifyExistingValueAsync`
- `UpdateReturnsFalseForMissingValueAsync`
- `RemoveShouldDeleteExistingValueAsync`
- `RemoveReturnsFalseForMissingValueAsync`

Expiration:

- `TouchShouldUpdateRelativeExpirationAsync`
- `TouchShouldUpdateAbsoluteExpirationAsync`
- `GetExpiryReturnsExpiringEntryAsync`
- `GetExpiryReturnsNonExpiringEntryAsync`
- `RemoveExpiryClearsExpirationAsync`

Get-or-add:

- `GetOrAddShouldReturnExistingValueAsync`
- `GetOrAddShouldCreateMissingValueAsync`

Mixed workloads:

- `ReadHeavy95To5ShouldExecuteAsync`
- `ReadMostly80To15To5ShouldExecuteAsync`
- `HotKeyReadMostlyShouldExecuteAsync`
- `TwoNodeReadMostlyUniformExecutesAsync`
- `RemoteOwnerReadMostlyShouldExecuteAsync`

## Interpreting Output

Compare only rows for the same benchmark method when diagnosing topology, value shape, or durability impact.

- Single-node vs two-node shows client/server and routing overhead.
- Local-owner vs remote-owner shows the routing and inter-node forwarding cost.
- Uniform keys show normal distributed ownership behavior.
- Hot keys highlight lock, contention, and routing pressure around a small keyset.
- Primitive vs custom type rows show serializer and payload-shape cost.
- Exception-path rows are diagnostic only because exception allocation is expected to dominate.

BenchmarkDotNet writes artifacts under:

```text
BenchmarkDotNet.Artifacts
```

The config exports GitHub Markdown, JSON, and CSV outputs.

## External Baselines

This suite does not add Redis or MemoryCache baselines. If external baseline projects are added later, compare them as
separate benchmark groups and avoid mixing external service setup cost into Squirix E2E rows.

## Known Limitations For v0.1 Benchmarks

- Cluster membership is static peer configuration.
- Wire alloc and durability comparison benchmarks support `E2EBenchmarkDurabilityMode.Persistence`
  (`UsePersistence()`). The default scenario matrix stays ephemeral-only unless
  `SQUIRIX_E2E_BENCHMARK_DURABILITY=1`.
- The benchmark project is diagnostic and early-preview oriented; absolute numbers depend heavily on the local machine,
  OS, thermal state, and background load.
- Remove-hit benchmarks include inline reset work to keep destructive operations valid across repeated BenchmarkDotNet
  invocations.
- `RemoveExpirationShouldClearExpiration` uses `IterationSetup` to re-seed expiring entries outside the measured method
  body.
