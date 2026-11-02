# Try a leader failure

Run a three-node RF=3 cluster, kill a current leader, and verify that the same client session can read existing values
and write new ones through the two surviving nodes.

## Run

Prerequisites: Docker Desktop with Linux containers, Docker Compose 2.17 or newer, and Python 3.10 or newer.
No host .NET SDK, installed Squirix tool, Python packages, or host certificate changes are required.
From the repository root:

```text
python samples/docker-client/failover.py
```

The first run builds the server and client images and downloads their dependencies. It then:

1. Creates an isolated Compose project with fresh data volumes and automatically assigned loopback host ports.
2. Waits for all three replica groups and their followers to become ready.
3. Starts the public NuGet client, writes 96 unique keys, and verifies every value.
4. Uses readiness diagnostics to identify a current leader with replicated data and two caught-up followers.
5. Sends SIGKILL to that node's container, after verifying it belongs to this demo project.
6. Continues the same client session: reads all 96 old values, writes 96 updates, and verifies those updates.
7. Confirms a surviving node now leads the affected group and saves the evidence.
8. Removes only this demo's containers, network and disposable data volumes, including on failure.

Example output (the node names and duration can differ):

```text
READY: 96 values written and verified; waiting for leader termination
KILL: node A, leader of group A
PASS: 96 old values read; 96 updates written and verified; batchMs=9211
VERIFIED: group A moved from A to B
```

`batchMs` measures the whole post-failure client batch, not election latency. The demo adds no application-level retry
loop; the SDK may retry calls and switch bootstrap endpoints internally. A missing value, wrong value, failed RPC,
timeout or missing replacement leader makes the command fail instead of printing a successful final verification.

## Evidence and limits

The command prints its evidence directory under `artifacts/docker-failover/`. It contains `before.json` and `after.json`
readiness snapshots, `client.log`, `server.log`, the generated Compose configuration, and `result.json` on success.
In readiness diagnostics, `copies` is populated on the group leader; its entries show follower readiness and lag.

The client uses the sample's public development JWT key and normal TLS verification inside its container.
The Python controller uses self-signed HTTPS only for diagnostic requests on the disposable loopback ports;
it does not change your host trust store. Do not use this configuration or its credentials in production.

This demonstrates one abrupt process failure with a majority still reachable. It does not establish production readiness,
partition safety, recovery after a complete restart, or what happens when the old leader returns.
The script does not stop existing Squirix containers or reuse their volumes. If you forcibly terminate the Python process
and cleanup cannot run, use the project name and configuration path printed at startup to clean up that demo:

```text
docker compose -p <printed-project> -f <printed-evidence-directory>/compose.json down --volumes
```
