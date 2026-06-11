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
SQUIRIX_E2E_BENCHMARK_SMOKE=1 dotnet run -c Release --project benchmarks/squirix.e2e.benchmarks -- --filter '*GetValueShouldReturnHit*' --warmupCount 1 --iterationCount 1
```

PowerShell:

```powershell
$env:SQUIRIX_E2E_BENCHMARK_SMOKE='1'
dotnet run -c Release --project benchmarks\squirix.e2e.benchmarks -- --filter '*GetValueShouldReturnHit*' --warmupCount 1 --iterationCount 1
Remove-Item Env:\SQUIRIX_E2E_BENCHMARK_SMOKE
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
- `Persistence` — WAL/snapshot stack enabled

The full scenario matrix uses `Ephemeral` only. Compare both modes with:

```bash
SQUIRIX_E2E_BENCHMARK_SMOKE=1 dotnet run -c Release --project benchmarks/squirix.e2e.benchmarks -- --filter '*DurabilityComparison*'
```

Or include both modes in the full matrix:

```bash
SQUIRIX_E2E_BENCHMARK_DURABILITY=1 dotnet run -c Release --project benchmarks/squirix.e2e.benchmarks
```

Client SDK benchmarks (`Squirix.Benchmarks`) expose the same modes through `DurabilityComparisonBenchmarks`.

## Benchmark Groups

Basic operations:

- `SetShouldStoreValue`
- `GetValueShouldReturnHit`
- `GetValueShouldReturnMiss`
- `GetEntryShouldReturnHit`
- `TryAddShouldAddMissingValue`
- `TryAddShouldReturnFalseForExistingValue`
- `AddShouldStoreMissingValue`
- `AddShouldThrowForExistingValue`
- `UpdateShouldModifyExistingValue`
- `UpdateShouldReturnFalseForMissingValue`
- `RemoveShouldDeleteExistingValue`
- `RemoveShouldReturnFalseForMissingValue`

Expiration:

- `TouchShouldUpdateRelativeExpiration`
- `TouchShouldUpdateAbsoluteExpiration`
- `GetExpirationShouldReturnExpiringEntry`
- `GetExpirationShouldReturnNonExpiringEntry`
- `RemoveExpirationShouldClearExpiration`

Get-or-add:

- `GetOrAddShouldReturnExistingValue`
- `GetOrAddShouldCreateMissingValue`

Mixed workloads:

- `ReadHeavy95To5ShouldExecute`
- `ReadMostly80To15To5ShouldExecute`
- `HotKeyReadMostlyShouldExecute`
- `UniformTwoNodeReadMostlyShouldExecute`
- `RemoteOwnerReadMostlyShouldExecute`

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
- Durability mode is currently `Default` only through the benchmark harness.
- The benchmark project is diagnostic and early-preview oriented; absolute numbers depend heavily on the local machine,
  OS, thermal state, and background load.
- Remove-hit benchmarks include inline reset work to keep destructive operations valid across repeated BenchmarkDotNet
  invocations.
- `RemoveExpirationShouldClearExpiration` uses `IterationSetup` to re-seed expiring entries outside the measured method
  body.
