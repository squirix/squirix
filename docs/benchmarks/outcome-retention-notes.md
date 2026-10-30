# Outcome table growth across retention windows

The short write profile included large dictionary and expiry-queue backing-array allocations.
This probe checks whether they recur once retained outcomes begin to expire. No collection implementation or retention policy was changed.

## Method

.NET 10.0.14, one thread, the real GroupIdempotencyState with its leader sequence and follower apply path.
FakeTimeProvider advances the unchanged production retry retention of 30 seconds, not the fallback one-hour standalone log default.
The production limits stay at 524288 records and 50331648 charged snapshot bytes.
Each operation advances monotonic and wall time consistently, and a new resolved outcome takes the place of the one now expired.

At each of 32768 and 131072 outcomes per window, six full windows run, followed by idle expiry and one refill window.
An unresolved reservation is kept throughout and must survive every expiry sweep.
The probe verifies record and heap counts, dictionary and heap capacities, and preservation of the unresolved pin after every phase.
Input identities and shared immutable fingerprint/outcome buffers are created before the allocation interval.
Collection capacities are inspected through reflection only outside that interval. The run performs 2293760 outcome operations.

## Results

| Outcomes per window | Path     | Initial allocated bytes | Later windows per operation | Refill per operation | Dictionary capacity | Heap capacity |
| ------------------- | -------- | ----------------------- | --------------------------- | -------------------- | ------------------- | ------------- |
| 32768               | Follower | 13537424                | 0 B                         | 0 B                  | 36353               | 32768         |
| 32768               | Leader   | 16682976                | 96 B                        | 96 B                 | 36353               | 32768         |
| 131072              | Follower | 57573128                | 0 B                         | 0 B                  | 156437              | 131072        |
| 131072              | Leader   | 70157736                | 96.00–96.06 B               | 96 B                 | 156437              | 131072        |

The dictionary and heap capacities never increase after the initial window, including refill after idle.
Exactly the outcomes of one window stay live, plus the unresolved pin. After idle only the pin remains, and the heap is empty.
The retained backing arrays keep their high-water capacities; expired outcome references are removed.

The steady leader allocation is accounted for by its owned copies of a 32-byte fingerprint and 16-byte outcome:
56 and 40 bytes including array headers and alignment. Small allocation-counter noise is visible in a few large leader phases.
Follower inputs already own those buffers, so its store calls allocate nothing in the measured steady phases.
This is not zero allocation for a complete RPC or for constructing the input records.

## Decision and limits

No new collection or eager maximum-capacity reservation is justified by this finding.
The growth arrays seen in the short profile are initial population costs for this healthy unique-operation workload.
The policy was not shortened, unexpired outcomes were not discarded, and unresolved pins were not evicted.

This is a deterministic in-memory lifecycle probe, not a long-running disk/network benchmark or a throughput improvement.
Snapshot install/rebuild, replacement churn, burst peaks and unlimited unresolved pins are outside its scope.
A policy for returning peak backing capacity after idle would be a separate memory/latency tradeoff.

Local data and reproducible source: `BenchmarkDotNet.Artifacts/outcome-retention/summary.json`,
`cycles.jsonl` and `reproduction/`. Diagnostic project paths are machine-specific and reference the built benchmark/test dependencies.
