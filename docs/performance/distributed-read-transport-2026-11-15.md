# Distributed read owner access, HTTPS RPC and official adapter, 2026-11-15

The shared outcome-store lock is measurable, but the real HTTPS RPC path dominates this fixture. A separate RPC profile does not reproduce the lock dominance of the earlier
local-only stress test. The next investigation should focus on HTTP/2/hosting and deadline/cancellation allocation, preserving the existing consistency and timeout contracts.
No production optimization is proposed or applied here.

This does **not** identify the cause of the FullStackHero 20–23% deficit against Redis/Valkey. That application's configuration and original traces remain unavailable. This is
a layer breakdown on one local machine, not a Redis/Valkey comparison.

## Sources and fixture

Production sources are develop `622b6843c0421426dd4a509f130f7c8b0b2bd54a`, including PR #1764. All new execution code is in benchmarks or TestKit. The previous PR's local
probe runner is unchanged.

- One in-process ephemeral node, RF=1, replication disabled, no persistence, no JWT and no sliding expiration.
- Real Kestrel HTTP/2 over TLS on loopback. Certificate chain and hostname validation remain enabled; the local ASP.NET development certificate is supplied as a trusted root
  to the measurement process.
- SDK 11.0.100, runtime and ASP.NET Core 10.0.15, Debian 13, AMD EPYC 7763 host, 9 processors reported by .NET. The shared host's priority and CPU scheduling are not
  controlled. BDN could not elevate priority or fully inspect sysfs CPU topology.
- Concurrent Workstation GC; client and server share a process, ThreadPool and GC. A separate-process ASP.NET deployment may behave differently.
- 1 KiB deterministic payload, a different key per logical reader, absolute expiration one hour ahead. Every returned array is compared byte-for-byte with the seed, including
  the direct path.
- A batch always contains 4,096 reads in total, divided across 1, 8 or 32 async workers. A shared start signal schedules all workers on the ThreadPool. These are logical
  readers, not dedicated OS threads.
- `Outcomes` is an **extra** outcome count. Key seeding also retains one Set outcome per key. The 60,000 extra outcomes use the real `RecordSuccess` path, including monotonic
  creation stamps; they are not restored snapshots as in the earlier probe-only test.
- New operation IDs on every read. Direct/raw use Guid strings; the official client uses its existing batched CSPRNG identity generator. This small difference is not
  separately priced.

## Layer boundaries

| Layer | Included | Excluded or different |
| --- | --- | --- |
| DirectAsync | Real NodeDistributedCacheAccess admission, both outcome probes, local entry read, policy parsing, ByteString response copy and final owned-array copy | Router, gRPC interceptors, HTTP/2, TLS, protobuf wire encoding/decoding and SDK policy |
| RawGrpcAsync | Generated GetDistributedAsync client, real TLS/HTTP/2 and complete server RPC pipeline, final owned-array copy | No caller cancellation token, no deadline, no SDK failover/retry/concurrency policy |
| RawGrpcWithBoundsAsync | Raw RPC plus the client defaults' 3-second attempt timer and 15-second outgoing operation deadline, linked to a benchmark lifetime token | Models bounds on a successful call; does not implement SDK queuing, retries, rerouting or error mapping |
| AdapterAsync | Official SquirixDistributedCache provider, session, EndpointFailover, CallPolicy, outgoing deadline, cancellation, RPC and final array | Application/HybridCache L1, application payload serialization and application HTTP endpoint |

Raw-minus-direct includes hosting, routing and interceptor work as well as transport. Adapter-minus-unbounded-raw is **not** the price of SDK wrappers alone: the
deadline/cancellation contracts differ. There is no server-side JSON value serialization on these paths.

## Broad pass: three layers, 18 cases

Two warmup iterations and five requested measurement iterations per case, one in-process launch, tiering disabled. BDN used adaptive invocation counts with a 250 ms target;
the minimum of four invocations made some network iterations substantially longer. BDN's default outlier handling retains four or five observations. The [full
JSON](distributed-read-transport-2026-11-15/full-run.json) retains measurement records, allocation counts and confidence intervals.

The bounded-raw control was added after this broad pass; the two focused launches below use the final four-layer source. The existing three paths' successful read behavior is
unchanged.

| Extra outcomes | Workers | Direct, us/op | Raw HTTPS, us/op | Adapter, us/op |
| ---: | ---: | ---: | ---: | ---: |
| 0 | 1 | 1.825 | 616.641 | 637.199 |
| 0 | 8 | 1.326 | 86.109 | 103.191 |
| 0 | 32 | 1.438 | 43.656 | 57.165 |
| 60000 | 1 | 2.567 | 649.858 | 644.732 |
| 60000 | 8 | 1.625 | 91.511 | 106.822 |
| 60000 | 32 | 1.922 | 46.711 | 54.757 |

These are aggregate batch elapsed times divided by total reads. For multiple workers they are **not individual request latencies**. At 32 workers and 60,000 extra outcomes the
broad pass corresponds to approximately 520k direct reads/s, 21.4k raw RPC reads/s and 18.3k adapter reads/s. This large separation does not separately quantify the transport,
scheduler, hosting or serialization components.

The broad allocation measurements were about 2.49 KiB/read for direct access, 12.6–12.8 KiB/read for raw RPC and 13.9–14.1 KiB/read for the adapter. BDN's allocations include
**both server and client in this process**, not just SDK allocations. Removing a 72-byte NodeCacheEntry would address less than 1% of the measured full RPC allocation and
would not by itself explain the throughput gap.

## Focused control: 32 readers, 60,000 extra outcomes

Two separate process launches, two warmups and five requested measured iterations per method, eight invocations per iteration (32,768 reads), unroll factor one. [Run
1](distributed-read-transport-2026-11-15/control-1.json) and [run 2](distributed-read-transport-2026-11-15/control-2.json) preserve all measurements and BDN
outlier/confidence-interval data. Very short focused direct iterations are corroborative only; the adaptive broad pass is the primary direct-read estimate.

| Method | Run 1, us/op | Run 2, us/op | Run 1, B/op | Run 2, B/op |
| --- | ---: | ---: | ---: | ---: |
| DirectAsync | 2.005 | 1.873 | 2546 | 2546 |
| RawGrpcAsync | 46.866 | 47.618 | 13127 | 13118 |
| RawGrpcWithBoundsAsync | 49.664 | 62.354 | 14235 | 14206 |
| AdapterAsync | 51.913 | 60.165 | 14411 | 14373 |

The bounded raw control adds 1,108 B/read in run 1 and 1,088 B/read in run 2. That reproduces most of the adapter's extra 1,284/1,255 B over unbounded raw. The remaining
adapter-versus-bounded-raw allocation is 176/167 B/read. This localizes the **allocation difference** to the combination of a propagated deadline and a linked attempt timer
(client and server together), rather than to value JSON serialization or construction of a new service adapter.

Throughput varies materially between process launches, especially with bounds. BDN's 99.9% intervals overlap for several comparisons. The bounded control is numerically near
the adapter in both launches, but the data do not support a precise percentage penalty, a claimed general RPS improvement or a conclusion that a timer alone is the cause. No
deadline or cancellation guarantee was disabled in production.

## Separate sampled-thread-time profile

The final adapter code was profiled separately at 32 readers and 60,000 extra outcomes, with one warmup, three requested measured iterations and eight invocations. Its timings
are excluded from the tables. Startup EventPipe enabled sampled thread stacks plus GC/contention events. The [top-30
report](distributed-read-transport-2026-11-15/profile-top-30.txt) and [stack summary](distributed-read-transport-2026-11-15/profile-stack-summary.json) are included.

Across the complete profile, including idle workers, startup, setup, GC and teardown:

- HasRetainedOperation accounts for about 0.236% of inclusive sampled thread time.
- Lock.TryEnterSlow specifically beneath HasRetainedOperation accounts for about 0.197%; contention is present but no longer dominates the trace as it did in the synthetic
  probe-only stress.
- Http2Connection.SendAsync, HTTP connection-pool/balancer continuations, pipeline reads/writes and GC appear in the profile. Monitor waits include several different
  transport/pipeline and initialization/finalizer sites; they cannot all be assigned to the outcome store or one HTTP/2 lock.

These are interpolated **sampled thread times**, not CPU utilization or a speedup bound. Idle ThreadPool waits dominate the denominator. Frame-interval counts are not the
number of actual contention events. The profile supports the changed priority together with the timing separation; it does not prove that eliminating the store lock would
improve the application by a particular amount.

## Reproduction

Build with the repository analyzers, use a valid trusted HTTPS certificate and an available loopback port. SQUIRIX_BENCHMARK_URI is optional; without it the test port pool
chooses a URI. SSL_CERT_FILE was used only to trust the local development certificate, without a validation bypass. Do not use the development certificate as a production
trust setup.

```sh
dotnet build benchmarks/squirix.benchmarks/Squirix.Benchmarks.csproj -c Release

DOTNET_TieredCompilation=0 \
SQUIRIX_BENCHMARK_URI=https://127.0.0.1:39523 \
dotnet benchmarks/squirix.benchmarks/bin/Release/net10.0/Squirix.Benchmarks.dll \
  --filter '*DistributedReadTransportBenchmarks*' --inProcess \
  --warmupCount 2 --iterationCount 5 --launchCount 1 --iterationTime 250 \
  --unrollFactor 1 --exporters json --artifacts /tmp/distributed-read-full
```

The full command now also includes the new bounded-raw method. For each focused run, use a separate process and artifact folder:

```sh
DOTNET_TieredCompilation=0 \
SQUIRIX_BENCHMARK_URI=https://127.0.0.1:39523 \
dotnet benchmarks/squirix.benchmarks/bin/Release/net10.0/Squirix.Benchmarks.dll \
  --filter '*DistributedReadTransportBenchmarks*60000*32*' --inProcess \
  --warmupCount 2 --iterationCount 5 --launchCount 1 --invocationCount 8 \
  --unrollFactor 1 --exporters json --artifacts /tmp/distributed-read-focused
```

For the separate profile, prepend these process-local variables and select AdapterAsync only. Use one warmup and three measured iterations; keep eight invocations.
Profile/report conversions must run sequentially because dotnet-trace shares an intermediate ETLX path.

```sh
DOTNET_EnableEventPipe=1
DOTNET_EventPipeConfig='Microsoft-Windows-DotNETRuntime:0x4001:5,Microsoft-DotNETCore-SampleProfiler:0x0:5'
DOTNET_EventPipeOutputPath=/tmp/distributed-read-rpc.nettrace

dotnet-trace report /tmp/distributed-read-rpc.nettrace topN -n 30 --inclusive -v
```

The variable assignments above must be exported or supplied as command prefixes, as in the timing commands. A short dry smoke across six raw RPC cases and two short focused
validation launches also passed; their one-invocation timings are not the reported throughput evidence.

## Decision and remaining limits

Do not optimize away either replay probe on this evidence. The most useful next target is the **full HTTP/2/hosting read path and deadline/cancellation allocation**, with a
successful-call allocation reduction as an experiment only if profiling identifies an avoidable allocation with unchanged ownership and lifetime behavior. The bounds control
narrows an allocation hypothesis; it is not an optimization candidate that can replace CallPolicy or omit retries.

Before changing production behavior, reproduce on the actual FullStackHero topology, runtime/GC, TLS/authentication, payload size, expiration options, L2 hit rate and
forwarding/renewal rates. This fixture uses neither replication/quorum reads nor sliding renewal/persistence and does not measure tail latency. It also has no concurrent
mutation workload, caller-triggered cancellation or failover tests. No general claim about RF=3 reads, Redis/Valkey or production request latency follows from these numbers.

## Validation

- Release benchmark/client/adapter/server/TestKit build with repository analyzers: zero warnings/errors.
- All 18 broad cases, both four-case controlled launches and the separate profile completed with exact payload assertions intact.
- The three retained timing JSON files contain 6737920 validated reads in Workload/Actual measurements alone; setup, warmup, pilot and profiling reads are additional. This
  counts logical reads, not an independently instrumented wire-attempt/error counter.
- Production sources are unchanged. Benchmark assertions cover response bytes and fixture startup; no shipping cache algorithm was modified.
