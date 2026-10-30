# Remote invocation classification allocations

Every inbound server RPC enters RemoteInvocationContext. The original scope is a reference record,
and `AsyncLocal<bool>` writes box the Boolean and replace execution-context state even when classification stays the same.
The changed scope is a readonly record struct. A private shared marker represents true and null represents false,
so changing classification does not box Boolean values; same-value enter and restore perform no async-local write.

Public calls nested inside owner calls still clear the owner flag and restore it afterwards.
The trusted-peer and ring checks of the inbound interceptor are unchanged.

## Measurement

Windows 11, Intel Core Ultra 9 285K, .NET 10.0.14, BenchmarkDotNet 0.15.8.
One in-process job, three warmups, five requested measurements and 250 ms requested iteration time.
The result consumes the classification observed inside each scope. Both runs contain the same earlier decoder optimization.

| Scenario           | Inner classification | Before allocated | After allocated | Before mean | After mean |
| ------------------ | -------------------- | ---------------- | --------------- | ----------- | ---------- |
| Enter and leave    | Public               | 192 B            | 0 B             | 27.39 ns    | 2.24 ns    |
| Enter and leave    | Internal owner       | 192 B            | 72 B            | 26.66 ns    | 19.19 ns   |
| Nested under owner | Public               | 384 B            | 144 B           | 51.26 ns    | 36.89 ns   |
| Nested under owner | Internal owner       | 384 B            | 72 B            | 53.50 ns    | 25.34 ns   |

These are isolated classification costs. Other ambient values can change execution-context allocation costs;
the complete RPC, gRPC framework, transport and cluster throughput were not compared for this change.
The large inclusive allocation category of the inbound interceptor is not all removed by this optimization.

## Validation

Seven new tests cover public classification, both nesting directions, flow across await,
child-to-parent isolation and overlapping public/internal invocations.
The final code passed 17 correlation tests, 2 forwarded-admission tests, 10 client-id resolver tests and 30 replica-log tests:
66 relevant tests in total, with no failures or skips. Server unit tests and benchmarks build with analyzers enabled and no warnings.

## Reproduction

Build `benchmarks/squirix.server.benchmarks/Squirix.Server.Benchmarks.csproj` in Release, then run its assembly with:

```text
--filter '*RemoteInvocationScopeBenchmarks*' --inProcess --warmupCount 3 --iterationCount 5 --iterationTime 250 --exporters json
```

Use distinct artifact directories for the original and changed implementation.
Local reports are in `BenchmarkDotNet.Artifacts/rpc-scope-before` and `BenchmarkDotNet.Artifacts/rpc-scope-final`.
The intermediate `rpc-scope-after` experiment kept boxed Boolean state and is not the final implementation or result.
