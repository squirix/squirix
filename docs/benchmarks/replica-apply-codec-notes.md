# Replica apply decoder allocations

The apply path decodes canonical records even though the append path already validates them without allocation.
This change makes the private decoder a stack-only span cursor and the three temporary sections readonly record structs.
The decoded strings and four byte fields keep their independent ownership. Canonical bytes and refusal behavior are unchanged.

## Local measurement

Windows 11, Intel Core Ultra 9 285K, .NET 10.0.14, BenchmarkDotNet 0.15.8.
One in-process job, three warmups, five requested measurements and 250 ms requested iteration time per payload size.
Only decoding is timed; setup creates the encoded record once. The returned log index consumes the result without boxing.
Both runs use the same benchmark and dependencies; the production decoder is the changed variable.

| Mutation bytes | Before allocated | After allocated | Before mean | After mean |
| -------------- | ---------------- | --------------- | ----------- | ---------- |
| 0              | 584 B            | 384 B           | 115.1 ns    | 112.3 ns   |
| 32             | 616 B            | 416 B           | 114.5 ns    | 104.3 ns   |
| 4096           | 4680 B           | 4480 B          | 263.2 ns    | 255.9 ns   |
| 65536          | 66120 B          | 65920 B         | 2515.0 ns   | 2567.7 ns  |

The repeatable result is 200 fewer allocated bytes and four fewer temporary objects per successful decode.
For the small 32-byte mutation this is a 32.5% allocation reduction within this codec benchmark.
The timing confidence intervals overlap for the 32-byte, 4-KB and 64-KB cases; this does not establish a general latency improvement.
The whole RPC and cluster throughput were not compared for this change. The earlier profile's full decoder allocation total is not all removable.

## Correctness checks

Thirty matching replica-log tests passed, including canonical golden hashes, truncations, malformed UTF-8, lengths and trailing bytes.
New checks mutate the backing input after decoding and verify the entire re-encoded record remains identical,
and verify that a memory slice cannot read outside its bounds or borrow bytes beyond its end.
The benchmark and server unit-test projects build with the repository analyzers enabled and no warnings.

## Reproduction

Build `benchmarks/squirix.server.benchmarks/Squirix.Server.Benchmarks.csproj` in Release, then run its assembly with:

```text
--filter '*ReplicaLogDecodeBenchmarks*' --inProcess --warmupCount 3 --iterationCount 5 --iterationTime 250 --exporters json
```

Use different `--artifacts` directories for the original and changed decoder.
Local raw reports are under `BenchmarkDotNet.Artifacts/apply-codec-before` and `BenchmarkDotNet.Artifacts/apply-codec-after`.
