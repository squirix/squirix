# Process chaos and register histories

The process chaos suite exercises RF=3 and RF=5 with persistence, replication, automatic failover and quorum reads enabled.
It is Jepsen-style fault testing, not a Jepsen certification or a proof of all cluster executions.

## Checker

`RegisterLinearizability` searches for a single legal order for each key, using a memoized backtracking search.
`RegisterHistory.Check(writes, reads)` remains the fast necessary-condition precheck; the recorded-history `Check` adds the complete search.
The model is a single-writer integer register with unique increasing write values, initially absent (represented by zero).
The public cache API currently has no CAS operation to include in this model.

Acknowledged writes must occur between invocation and response. A timed-out write can occur anywhere after invocation, including
after later acknowledgements, or be omitted. Successful reads must observe the current value. Failed reads are recorded but impose
no state constraint. Equal timestamps overlap. Calls on different keys are checked independently; cross-key transactions are not modeled.
Cancellation interrupts the search without returning a consistency verdict. Search cost can be exponential; a canceled or failed search
does not count as a pass.

Controls include the acknowledged-value resurrection anomaly that the pairwise checker misses, late ambiguous writes,
overlapping reads, exact timestamp boundaries and an independent permutation oracle over 128 small histories.
Artifact round-trip tests preserve both passing and failing verdicts.

`BrokenModeHistoryTests` projects deterministic client-bearing schedules for every non-`None` `BrokenMode` into the same register
history format and checker used by E2E. Every step is selected from `ExploreRunner.NextStates`; no node state is patched by the tests.
The schedules continue through actual client proposals, replication, commit/apply and completed reads rather than treating an internal
invariant violation itself as a register anomaly. The source of test cases enumerates the enum, so adding a mode requires a new schedule.

| Broken mode | Client-visible witness |
| --- | --- |
| `Vote` | A second vote permits two leaders in one term; one acknowledges the target write, the other later reads zero |
| `CurrentTermCommit` | An earlier-term client entry is acknowledged without a current-term majority entry; a later leader serves a read without it |
| `ReadIndex` | A deposed leader serves a stale read without confirming current authority; a separate case reads before recovery has applied an acknowledged entry |
| `RepairCeiling` | Repair without the majority term ceiling permits two leaders in the repaired term, followed by an acknowledged write and stale read |
| `LogOrder` | A longer older-term log wins against a newer-term log containing the acknowledged write, then serves a stale read |

The projection follows one explicit target-key proposal with value one. Election no-ops and filler proposals concern no target-key
write; other keys are omitted. Acknowledgement requires both commit and apply. A completed read requires `ReadReady` and derives the
value from the target entry's term/index and the reader's applied prefix. Logical ticks preserve invocation/response precedence.
The three-node schedules allow six terms, eight entries, 32 in-flight messages, disk repair and reads. Resends use the existing log-rules
catch-up transition with the same bounds; elections remain message-driven. These are deterministic model schedules, not production traces
or an exhaustive exploration of those combined bounds, and not a claim that every internal invariant violation is client-observable.

All five negative histories are rejected both directly and after JSON replay. Each has one well-formed acknowledged write followed by
a completed read returning zero. Paired `BrokenMode.None` executions stop at the protecting rule and leave legal history prefixes;
a non-vacuous normal write/read history passes. The additional recovery/apply pair covers the second `ReadIndex` bypass.

## Process configuration and faults

`tools/chaos/run.py` starts the built host in separate Linux Docker containers. Each node has a private 256 MiB ext4 filesystem,
backed by a loop-mounted image and retained across process/container restarts. Cluster mTLS and external HTTPS use a generated
test CA. Cache access uses an ephemeral cluster-specific JWT key. The runner never publishes ports. The client uses only the exported SDK.

The output directory must be on local Linux storage, including when using Docker Desktop. Keep it inside the Linux controller or VM
and copy artifacts to the host after cleanup. Windows/macOS shared folders (`9p`, `drvfs`, `cifs`, `smb2`, `virtiofs`) are rejected before
cluster allocation. Loop-backed disk I/O and uncached `libfaketime` clock-file reads otherwise cross the host file-sharing boundary,
which can stall startup and make a healthy persisted RF=5 cluster miss the 90-second recovery deadline.

Every schedule includes:

| Fault | Mechanism |
| --- | --- |
| Abrupt stop and recovery | Kill the observed leader with `SIGKILL`, verify a higher-term replacement, then restart on the same data filesystem |
| Process pause | Pause the observed leader, verify a higher-term replacement through survivors' public metrics, then resume the deposed leader |
| Majority/minority partition | Drop internal TCP traffic in both directions across the split |
| Bridge partition | A bridge node remains connected to both otherwise separated sides |
| One-way partition | Drop only one direction across the split |
| Latency, jitter and bandwidth | `tc netem`: 80 ms delay, 20 ms jitter, 1 Mbit/s on one node |
| Wall-clock step | Per-process libfaketime offset to +300 s and back; monotonic time is preserved |
| Disk full | Fill only the victim's bounded filesystem until actual ENOSPC, then free space and restart |

Each fault lasts at least ten seconds (default fifteen). A paced register workload starts before the fault and continues through
recovery. Every step uses fresh keys, one per replica group, selected with the existing pure key-routing helper.
At the end, the whole cluster restarts on its existing disks, and all earlier keys are read
again through every node to detect lost acknowledgements and incomplete recovery.
After each heal, the runner waits up to 90 seconds for every node's live endpoint; the final SDK reads retry transient RPC failures for
up to one minute per key. These bounded recovery checks fail the run when exceeded. Controller failure signals the client to cancel
outstanding calls and save the current history before cleanup.
The runner records the actual fault order and victims, and fails if the client exits early or its report does not contain exactly
one successful chaos test. Node logs and histories are retained on failure.

Process kill does **not** simulate a host power loss or discard the kernel page cache. Torn sectors and lying fsync are not injected.
Leader selection and replacement evidence use `/health/ready` to refresh `/metrics`, without accessing server internals. A recovering
follower's unavailable diagnostics are recorded; a replacement still requires an observed authoritative leader in a higher term.
This exercises a deposed leader
resuming under load, but does not prescribe the exact instruction or lease-boundary instant at which the process pauses.

## Running and reproducing

Use a disposable Linux runner with Docker, Python 3, OpenSSL, e2fsprogs and passwordless `sudo` for mounting the test filesystems.
The node containers need `NET_ADMIN` only for their own network namespaces. The normal CI runner does not use nested Docker.

```sh
bundle="$PWD/.git/chaos-bundle"
dotnet build tests/squirix.e2e.tests/Squirix.E2ETests.csproj -c Release -r linux-x64
mkdir -p "$bundle/client"
cp -a tests/squirix.e2e.tests/bin/Release/net10.0/linux-x64/. "$bundle/client/"
dotnet publish src/squirix.server.host/Squirix.Server.Host.csproj -c Release -r linux-x64 --self-contained false -o "$bundle/server"
cp tools/chaos/start.sh "$bundle/start.sh"
docker build -f tools/chaos/Dockerfile -t squirix-chaos:local "$bundle"
SQUIRIX_FAULT_SEED=1478 python3 tools/chaos/run.py --replicas 3 --output /tmp/chaos-rf3-1478 --commit "$(git rev-parse HEAD)"
```

The output directory must not already exist. Repeat with `--replicas 5` and a new output directory.
The seed fixes fault order and planned victims. Kill/pause target the observed leader of group `n0`, so their actual victims depend
on elections and are recorded separately. OS scheduling, elections, request timing and network jitter remain nondeterministic.
A rerun with the same seed is a reproduction recipe, not a guarantee of an identical execution.

To reproduce the checker verdict exactly, use the saved history without a cluster:

```sh
SQUIRIX_CHAOS_HISTORY=/absolute/path/0.history.json dotnet test \
  --project tests/squirix.e2e.tests/Squirix.E2ETests.csproj \
  --treenode-filter '/*/*/ProcessChaosTests/SavedHistoryHasLegalOrder'
```

`run.json` records the seed, source revision, replica count, fault schedule and elapsed event times. Each
`control/*.history.json` contains write outcomes and invocation/response timestamps, successful read observations,
and failed read intervals. Logs and TRX are uploaded for both passing and failing nightly runs. CA private keys and disk images
are not uploaded. After cleanup, the local disk image files remain for investigation; no filesystem remains mounted.

## Results and acceptance status

The workflow is `.github/workflows/chaos-nightly.yml`. Scheduled runs check out `develop`; manual runs use the selected ref.
Both replica factors run with a fresh seed unless a manual run supplies one. As with other GitHub scheduled workflows, scheduling becomes active when
the workflow is present on the repository's default branch.

The checker/workload/artifact validation passed 39 tests, including both permutation-oracle cases and a 20,000-operation history.
The ordinary failover suite passed all 70 tests with the complete checker; the protocol-model suite passed all 61 tests,
including 14 client-history controls (five negative modes, five safe counterparts, the recovery/apply pair, a healthy history and enum coverage).
The scheduler, metric-parser, diagnostic and failure-cleanup controls passed fourteen Python tests. These are local implementation checks, not nightly cluster results.

Local Linux-container validation on 2026-11-02 used seed `1478`, the working tree based on develop
`2710f51fa0751acdbdf68d34889a681bf596aea7`, .NET 10.0.14, and image
`sha256:6b4ecabb3c74aaa970fbc93e8001e0b2c51f3737e7885cd783342e6fae6d5bb8`.
Docker Desktop hosted a disposable Linux controller with a nested Docker daemon using the `vfs` storage driver;
this local environment differs from the native Linux nightly runner.

| Configuration | Result | Duration | Evidence |
| --- | --- | --- | --- |
| RF=3, all eight faults, full-cluster restart | Passed | 373 s | One executed/passing process test, eight histories, reads through every node |
| RF=5, all eight faults, full-cluster restart | Passed | 497 s | One executed/passing process test, eight histories, reads through every node |

Early integration runs exposed harness setup and synchronization defects: missing client native runtime assets, missing JWT setup,
unrefreshed diagnostic gauges, and reconnecting before restarted endpoints were ready. The final runner fixes those defects.
Local artifacts are retained under `.git/chaos-local/`; successful runs are `rf3-run6` and `rf5-run5`.
No production consistency anomaly was demonstrated or fixed in these runs.

A follow-up RF=5 run with the stronger per-key post-recovery acknowledgement check timed out after the final restart when its
output directory was a Windows bind mount. Startup threads blocked in `p9_client_rpc`. Reopening copies of the same persisted disks
on local Linux storage made all five endpoints live within 26 seconds including setup. The complete seed `1478` RF=5 schedule then
passed in 255 seconds with output on the Linux controller's own filesystem; the final restart took 10.5 seconds to reach live endpoints.
Its single process test passed, saved eight histories, required new acknowledgements for every key and read prior data through all five nodes.
Artifacts are retained under `.git/chaos-local/rf5-native/`. The image was
`sha256:2491294d69ac2640ccb7dd8760a087015f4543af3086415cef6efaa11f5a3827`; the controller includes the host-filesystem guard.
The 90-second endpoint deadline and all consistency assertions remain in effect. Scheduler controls now pass fourteen tests.

Seven consecutive green nightly runs have **not** been established. No cluster anomaly is claimed fixed by this change.
Keep the cluster-wide proof non-goal in `consistency.md`; publish run links and configuration-specific results here only after
actual process runs and the seven-run observation period. The BrokenMode client-history acceptance gap is covered by the controls above;
the seven-run observation period remains outstanding.
