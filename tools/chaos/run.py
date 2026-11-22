"""Run real Linux process faults against RF3/RF5; keep seed, schedule, logs and histories."""

import argparse
import base64
from collections import deque
import hashlib
import hmac
import json
import os
from pathlib import Path
import random
import re
import secrets
import signal
import subprocess
import sys
import time
import uuid


FAULTS = ("graceful-stop", "kill", "pause", "partition", "bridge", "one-way", "latency", "clock", "disk-full")


class CommandFailed(RuntimeError):
    def __init__(self, executable, code, output):
        super().__init__(f"{executable} failed with exit code {code}: {output}")
        self.stdout = output


def schedule(seed, replicas):
    rng = random.Random(seed)
    kinds = list(FAULTS)
    rng.shuffle(kinds)
    result = []
    for kind in kinds:
        nodes = list(range(replicas))
        rng.shuffle(nodes)
        result.append({"kind": kind, "nodes": nodes})
    return result


def command(*args, check=True):
    try:
        result = subprocess.run([str(arg) for arg in args], check=False, text=True, timeout=120,
                                stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    except subprocess.TimeoutExpired:
        raise CommandFailed(args[0], "timeout", "Command exceeded 120 seconds") from None
    if check and result.returncode != 0:
        # Docker arguments can carry the ephemeral JWT key; never print the full command line.
        raise CommandFailed(args[0], result.returncode, result.stdout)
    return result.stdout


def privileged(*args):
    return command(*(() if os.geteuid() == 0 else ("sudo", "-n")), *args)


def wait_until(predicate, seconds, description):
    deadline = time.monotonic() + seconds
    while not predicate():
        if time.monotonic() >= deadline:
            raise TimeoutError(description)
        time.sleep(0.2)


class Cluster:
    def __init__(self, args):
        self.args = args
        self.scenario = getattr(args, "scenario", "chaos")
        self.leader_exit = self.scenario in ("leader-exit", "leader-dns-loss")
        self.root = args.output.resolve()
        self.root.mkdir(parents=True, exist_ok=False)
        self.name = "squirix-chaos-" + uuid.uuid4().hex[:12]
        self.nodes = [self.name + "-n" + str(i) for i in range(args.replicas)]
        self.client = self.name + "-client"
        self.mounted = []
        self.created = []
        self.events = []
        self.network_created = False
        self.plan = schedule(args.seed, args.replicas)
        if self.leader_exit:
            self.plan = [step for step in self.plan if step["kind"] in ("graceful-stop", "kill")]
        elif self.scenario == "disk-recovery":
            self.plan = [{"kind": "disk-full", "nodes": [1, 0, 2]}]
        self.signing_key = secrets.token_bytes(32)
        self.token = token_for(self.signing_key)

    def record(self, event, **details):
        self.events.append({"event": event, "elapsed": time.monotonic() - self.started, **details})
        (self.root / "run.json").write_text(json.dumps({
            "seed": self.args.seed, "replicas": self.args.replicas,
            "scenario": self.scenario,
            "schedule": self.plan, "events": self.events,
            "image": self.args.image, "commit": self.args.commit,
            "fault_seconds": self.args.fault_seconds,
        }, indent=2), encoding="utf-8")

    def certificates(self):
        certs = self.root / "certs"
        certs.mkdir()
        command("openssl", "req", "-x509", "-newkey", "rsa:2048", "-nodes", "-days", "2",
                "-keyout", certs / "ca.key", "-out", certs / "ca.crt", "-subj", "/CN=Chaos CA")
        for i, name in enumerate(self.nodes):
            command("openssl", "req", "-newkey", "rsa:2048", "-nodes", "-keyout", certs / f"{i}.key",
                    "-out", certs / f"{i}.csr", "-subj", f"/CN=n{i}")
            extensions = certs / f"{i}.ext"
            extensions.write_text(f"subjectAltName=DNS:{name}\nextendedKeyUsage=serverAuth,clientAuth\n", encoding="utf-8")
            command("openssl", "x509", "-req", "-in", certs / f"{i}.csr", "-CA", certs / "ca.crt",
                    "-CAkey", certs / "ca.key", "-CAcreateserial", "-days", "2", "-extfile", extensions,
                    "-out", certs / f"{i}.crt")
            command("openssl", "pkcs12", "-export", "-inkey", certs / f"{i}.key", "-in", certs / f"{i}.crt",
                    "-certfile", certs / "ca.crt", "-passout", "pass:", "-out", certs / f"{i}.pfx")

    def start(self):
        self.started = time.monotonic()
        if not self.leader_exit:
            filesystem = command("stat", "-f", "-c", "%T", self.root).strip()
            if filesystem in {"9p", "drvfs", "cifs", "smb2", "virtiofs"}:
                raise RuntimeError(
                    f"Process chaos requires local Linux storage; output is on {filesystem}. "
                    "Keep the run directory, clock files and disk images inside the Linux VM, "
                    "then copy artifacts to the host after cleanup.")
        self.record("starting", image_id=command("docker", "image", "inspect", "-f", "{{.Id}}", self.args.image).strip())
        self.certificates()
        command("docker", "network", "create", self.name)
        self.network_created = True
        peers = [{"NodeId": f"n{i}", "Uri": f"https://{name}:5000"} for i, name in enumerate(self.nodes)]
        for i, name in enumerate(self.nodes):
            directory = self.root / str(i)
            directory.mkdir()
            data_mount = self.data_mount(directory)
            (directory / "clock").write_text("+0", encoding="ascii")
            settings = {"Squirix": {"Cluster": {
                "ClusterId": self.name, "NodeId": f"n{i}", "Uri": peers[i]["Uri"], "Peers": peers,
                "ReplicaCount": self.args.replicas, "PersistenceEnabled": True, "ReplicationEnabled": True,
                # Static ownership keeps the full owner responsible for the regression's refused write.
                "AutomaticFailoverEnabled": self.scenario != "disk-recovery", "QuorumReadsEnabled": self.scenario != "disk-recovery",
                "DataDirectory": "/data",
            }, "PrometheusMetrics": {"enabled": True}}}
            (directory / "settings.json").write_text(json.dumps(settings), encoding="utf-8")
            environment = {
                "SQUIRIX_CLUSTER_MTLS_INTERNAL_PORT": "5001",
                "SQUIRIX_CLUSTER_MTLS_CERT_PFX_PATH": f"/certs/{i}.pfx",
                "SQUIRIX_CLUSTER_MTLS_CA_PATH": "/certs/ca.crt",
                "SQUIRIX_TLS_CERTIFICATE_PATH": f"/certs/{i}.pfx",
                "SQUIRIX_MANAGEMENT_URI": "https://127.0.0.1:5002",
                "DOTNET_PROCESSOR_COUNT": "2", "Logging__LogLevel__Default": "Warning",
                "LD_PRELOAD": "/usr/lib/x86_64-linux-gnu/faketime/libfaketime.so.1",
                "FAKETIME_TIMESTAMP_FILE": "/config/clock", "FAKETIME_NO_CACHE": "1",
                "FAKETIME_DONT_FAKE_MONOTONIC": "1",
                # The glibc workaround otherwise makes .NET native waits time out immediately, even at +0.
                "FAKETIME_FORCE_MONOTONIC_FIX": "0",
                "SQUIRIX_JWT_ISSUER": "squirix-chaos",
                "SQUIRIX_JWT_AUDIENCE": "squirix-chaos",
                "SQUIRIX_JWT_SIGNING_KEY": base64.b64encode(self.signing_key).decode("ascii"),
            }
            if self.leader_exit:
                del environment["LD_PRELOAD"]
                environment["Logging__LogLevel__Default"] = "Information"
            if self.scenario == "disk-recovery":
                environment["Logging__Console__FormatterName"] = "json"
            network = self.name
            if self.scenario == "leader-exit":
                # A process exit leaves the machine address routable. Docker otherwise unpublishes its DNS name too.
                anchor = name + "-address"
                self.created.append(anchor)
                command("docker", "run", "-d", "--name", anchor, "--network", self.name, "--network-alias", name,
                        "--entrypoint", "/bin/sleep", self.args.image, "infinity")
                network = "container:" + anchor
            arguments = ["docker", "run", "-d", "--name", name, "--network", network,
                         "--mount", f"type=bind,src={self.root / 'certs'},dst=/certs,readonly",
                         "--mount", f"type=bind,src={directory},dst=/config,readonly",
                         "--mount", data_mount]
            if not self.leader_exit:
                arguments.extend(["--cap-add", "NET_ADMIN"])
            for key, value in environment.items():
                arguments += ["-e", f"{key}={value}"]
            self.created.append(name)
            if self.scenario != "disk-recovery":
                arguments.extend(["-e", "SQUIRIX_EXPERIMENTAL_LEADERSHIP_TRANSFER=1"])
            command(*arguments, self.args.image, "dotnet", "/app/server/Squirix.Server.Host.dll", "run",
                    "--settings", "/config/settings.json", "--persist", "--enable-replication", "--metrics")
        addresses = [n + "-address" for n in self.nodes] if self.scenario == "leader-exit" else self.nodes
        self.ips = [command("docker", "inspect", "-f", "{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}", n).strip() for n in addresses]
        self.record("nodes-started", addresses=self.ips)
        time.sleep(10)
        for name in self.nodes:
            if command("docker", "inspect", "-f", "{{.State.Running}}", name).strip() != "true":
                raise RuntimeError("Node failed startup: " + command("docker", "logs", name))
        self.start_client()

    def data_mount(self, directory):
        if self.leader_exit:
            # The anonymous Linux volume survives stop/start and is removed with this container.
            return "type=volume,dst=/data"
        data = directory / "data"
        data.mkdir()
        disk = directory / "disk.img"
        with disk.open("wb") as stream:
            stream.truncate(256 * 1024 * 1024)
        command("mkfs.ext4", "-q", "-F", "-m", "0", disk)
        privileged("mount", "-o", "loop", disk, data)
        self.mounted.append(data)
        return f"type=bind,src={data},dst=/data"

    def start_client(self):
        control = self.root / "control"
        control.mkdir()
        test = "DiskFullRecoversConfirmedWrites" if self.scenario == "disk-recovery" else "ProcessFaultsKeepRegistersLinearizable"
        self.created.append(self.client)
        command("docker", "run", "-d", "--name", self.client, "--network", self.name,
                "--mount", f"type=bind,src={self.root / 'certs'},dst=/certs,readonly",
                "--mount", f"type=bind,src={control},dst=/control",
                "-e", "DOTNET_PROCESSOR_COUNT=2", "-e", "SQUIRIX_CHAOS_CONTROL=/control",
                "-e", f"SQUIRIX_CHAOS_STEPS={len(self.plan)}",
                "-e", "SQUIRIX_CHAOS_TOKEN=" + self.token,
                "-e", "SQUIRIX_CHAOS_ENDPOINTS=" + ";".join(f"https://{n}:5000" for n in self.nodes),
                self.args.image, "dotnet", "/app/client/Squirix.E2ETests.dll",
                "--treenode-filter", f"/*/*/ProcessChaosTests/{test}",
                "--report-trx", "--results-directory", "/control/results")

    def execute(self, node, *args):
        return command("docker", "exec", self.nodes[node], *args)

    def cut(self, source, target):
        address = command("docker", "inspect", "-f", "{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}",
                          self.nodes[target]).strip()
        self.execute(source, "iptables", "-A", "OUTPUT", "-d", address, "-p", "tcp",
                     "--dport", "5001", "-j", "DROP")

    def leader(self, excluded=None, diagnostics=None):
        leaders = []
        for index, name in enumerate(self.nodes):
            if index == excluded:
                continue
            observation_details = {"node": index}
            if diagnostics is not None:
                diagnostics.append(observation_details)
            # Readiness refreshes the read-only replication gauges before the scrape.
            try:
                readiness = self.http(index, "/health/ready", (200, 503))
                observation_details["readiness"] = readiness
                output = self.http(index, "/metrics", (200,))
            except RuntimeError as error:
                # A recovering follower need not answer diagnostics to observe a surviving leader.
                observation_details["error"] = str(error)
                self.record("diagnostic-unavailable", node=index, message=str(error))
                continue
            observation = parse_leader(output, "n0")
            if diagnostics is not None:
                observation_details["metrics"] = [
                    line for line in output.splitlines()
                    if line.startswith("squirix_replication_") and re.search(r'[{,]group="n0"[,}]', line)
                ]
                observation_details["leader_term"] = observation
            if observation is not None:
                leaders.append({"node": index, "term": observation, "group": "n0"})
        return leaders[0] if len(leaders) == 1 else None

    def http(self, index, path, statuses):
        name = self.nodes[index]
        request = (f"printf 'GET {path} HTTP/1.1\\r\\nHost: localhost\\r\\nConnection: close\\r\\n\\r\\n' | "
                   f"timeout 5 openssl s_client -quiet -connect 127.0.0.1:5002 -servername {name} "
                   f"-verify_hostname {name} -verify_return_error -CAfile /certs/ca.crt 2>/dev/null")
        # OpenSSL can report unexpected TLS EOF after a complete HTTP response.
        output = command("docker", "exec", name, "sh", "-c", request, check=False)
        if not any(output.startswith(f"HTTP/1.1 {status} ") for status in statuses):
            raise RuntimeError(f"Invalid diagnostic response from {name} for {path}: {output}")
        return output

    def target_leader(self, planned):
        if planned["kind"] not in ("graceful-stop", "kill", "pause"):
            return planned
        observed = None
        def ready():
            nonlocal observed
            observed = self.leader()
            return observed is not None
        wait_until(ready, 30, "No unique leader with authority before the fault")
        nodes = [observed["node"]] + [node for node in planned["nodes"] if node != observed["node"]]
        return {**planned, "nodes": nodes, "leader_before": observed}

    def await_replacement(self, step):
        if step["kind"] not in ("graceful-stop", "kill", "pause"):
            return
        observed = None
        recent_polls = deque(maxlen=5)

        def replaced():
            nonlocal observed
            diagnostics = []
            elapsed = time.monotonic() - self.started
            observed = self.leader(excluded=step["nodes"][0], diagnostics=diagnostics)
            recent_polls.append({"elapsed": elapsed, "observations": diagnostics})
            return observed is not None and observed["term"] > step["leader_before"]["term"]

        try:
            wait_until(replaced, 30, "The unavailable leader was not replaced by a higher-term leader")
        except TimeoutError:
            self.record("leader-replacement-timeout", before=step["leader_before"],
                        polls=list(recent_polls))
            raise
        self.record("leader-replaced-before-resume", before=step["leader_before"], after=observed)

    def transfer_witnesses(self, step):
        before = step["leader_before"]
        witness = re.compile(rf"Processed leadership transfer for group {re.escape(before['group'])} from term {before['term']}(?![0-9])")
        targets = []
        for index in step["nodes"][1:]:
            log = command("docker", "logs", self.nodes[index])
            targets.extend([index] * len(witness.findall(log)))
        if len(targets) != (1 if step["kind"] == "graceful-stop" else 0):
            raise RuntimeError(f"Unexpected transfer campaigns after {step['kind']}: {targets}")
        return targets

    def fault(self, step):
        kind, nodes = step["kind"], step["nodes"]
        victim = nodes[0]
        if kind == "kill":
            command("docker", "kill", "--signal", "KILL", self.nodes[victim])
        elif kind == "graceful-stop":
            command("docker", "stop", "--signal", "TERM", "--timeout", "20", self.nodes[victim])
            exit_code = command("docker", "inspect", "-f", "{{.State.ExitCode}}", self.nodes[victim]).strip()
            if exit_code != "0":
                raise RuntimeError(f"Leader failed to exit gracefully: exit code {exit_code}")
        elif kind == "pause":
            command("docker", "pause", self.nodes[victim])
        elif kind in ("partition", "bridge", "one-way"):
            minority = nodes[:self.args.replicas // 2]
            majority = nodes[self.args.replicas // 2:]
            if kind == "bridge":
                majority = majority[1:]
            for source in minority:
                for target in majority:
                    self.cut(source, target)
                    if kind != "one-way":
                        self.cut(target, source)
        elif kind == "latency":
            self.execute(victim, "tc", "qdisc", "add", "dev", "eth0", "root", "netem", "delay", "80ms", "20ms", "rate", "1mbit")
        elif kind == "clock":
            before = int(self.execute(victim, "date", "+%s").strip())
            self.set_clock(victim, "+300")
            after = int(self.execute(victim, "date", "+%s").strip())
            if after - before < 290:
                raise RuntimeError("Clock step was not observed inside the node")
        elif kind == "disk-full":
            output = self.execute(victim, "sh", "-c", "LC_ALL=C dd if=/dev/zero of=/data/chaos-fill bs=4096 conv=fsync; test $? -ne 0")
            if "No space left on device" not in output:
                raise RuntimeError("Disk full was not observed: " + output)
            # A large dd block can fail while enough smaller blocks remain for the journal to keep writing.
            available = int(self.execute(victim, "stat", "-f", "-c", "%a", "/data").strip())
            if available != 0:
                raise RuntimeError(f"Disk full left {available} available blocks")
        else:
            raise ValueError(kind)
        self.record("fault-active", **step)

    def heal(self, step):
        kind, nodes = step["kind"], step["nodes"]
        victim = nodes[0]
        if kind in ("graceful-stop", "kill"):
            command("docker", "start", self.nodes[victim])
        elif kind == "pause":
            command("docker", "unpause", self.nodes[victim])
        elif kind in ("partition", "bridge", "one-way"):
            for node in range(self.args.replicas):
                self.execute(node, "iptables", "-F", "OUTPUT")
        elif kind == "latency":
            self.execute(victim, "tc", "qdisc", "del", "dev", "eth0", "root")
        elif kind == "clock":
            self.set_clock(victim, "+0")
        elif kind == "disk-full":
            privileged("rm", self.root / str(victim) / "data/chaos-fill")
            command("docker", "restart", self.nodes[victim])
        self.record("healed", **step)

    def set_clock(self, victim, value):
        path = self.root / str(victim) / "clock"
        pending = path.with_suffix(".pending")
        pending.write_text(value, encoding="ascii")
        pending.replace(path)

    def client_alive(self):
        running = command("docker", "inspect", "-f", "{{.State.Running}}", self.client).strip()
        if running != "true":
            raise RuntimeError("Client exited before the schedule completed: " + command("docker", "logs", self.client))

    def await_recovery(self):
        pending = set(range(self.args.replicas))
        def recovered():
            self.client_alive()
            for node in tuple(pending):
                try:
                    self.http(node, "/health/live", (200,))
                except RuntimeError:
                    continue
                pending.remove(node)
            return not pending
        wait_until(recovered, 90, "Not all node endpoints recovered within 90 seconds")
        self.record("endpoints-recovered")

    def run(self):
        self.start()
        if self.scenario == "disk-recovery":
            self.run_disk_recovery()
            self.finish_client()
            return
        for index, step in enumerate(self.plan):
            ready = self.root / "control" / f"{index}.ready"
            def prepared():
                self.client_alive()
                return ready.exists()
            wait_until(prepared, 120, f"workload {index} did not become ready")
            step = self.target_leader(step)
            fault_start = time.monotonic()
            self.record("fault-start", **step)
            self.fault(step)
            active = time.monotonic()
            if self.scenario == "leader-dns-loss":
                network = json.loads(command("docker", "network", "inspect", self.name))[0]
                attached = {entry["Name"] for entry in network["Containers"].values()}
                expected = set(self.nodes) - {self.nodes[step["nodes"][0]]}
                if attached.intersection(self.nodes) != expected:
                    raise RuntimeError("Expected only the stopped leader to lose its Docker network endpoint")
                self.record("leader-endpoint-withdrawn", node=step["nodes"][0])
            self.await_replacement(step)
            if step["kind"] in ("graceful-stop", "kill"):
                elapsed = (time.monotonic() - fault_start) * 1000
                self.record("replacement-observed", kind=step["kind"], elapsed_ms=elapsed)
                if self.leader_exit:
                    self.record("transfer-witnesses", kind=step["kind"], targets=self.transfer_witnesses(step))
            time.sleep(max(0, self.args.fault_seconds - (time.monotonic() - active)))
            self.heal(step)
            if index == len(self.plan) - 1:
                command("docker", "restart", *self.nodes)
                self.record("full-cluster-restart")
            time.sleep(10)
            self.await_recovery()
            for name in self.nodes:
                if command("docker", "inspect", "-f", "{{.State.Running}}", name).strip() != "true":
                    raise RuntimeError("Node did not recover: " + command("docker", "logs", name))
            (self.root / "control" / f"{index}.done").write_text("done", encoding="ascii")
        self.finish_client()

    def wait_for_control(self, name):
        def observed():
            self.client_alive()
            return (self.root / "control" / name).exists()
        wait_until(observed, 120, f"client did not signal {name}")

    def await_ready(self):
        def ready():
            self.client_alive()
            try:
                for node in range(self.args.replicas):
                    self.http(node, "/health/ready", (200,))
                return True
            except RuntimeError:
                return False
        wait_until(ready, 90, "Nodes did not become ready after restart")

    def run_disk_recovery(self):
        control = self.root / "control"
        self.wait_for_control("disk-ready")
        self.record("confirmed-values-written")
        step = self.plan[0]
        self.fault(step)
        since = str(time.time())
        (control / "disk-full").write_text("full", encoding="ascii")
        self.wait_for_control("write-refused")
        self.record("write-refused", failure=(control / "write-refused").read_text(encoding="utf-8"))
        self.confirm_disk_write_failure(step["nodes"][0], since)
        self.heal(step)
        self.await_ready()
        (control / "owner-restarted").write_text("restarted", encoding="ascii")
        self.wait_for_control("recovery-verified")
        self.record("confirmed-values-recovered-and-new-write-acknowledged")
        command("docker", "restart", *self.nodes)
        self.record("full-cluster-restart")
        self.await_ready()
        (control / "cluster-restarted").write_text("restarted", encoding="ascii")

    def confirm_disk_write_failure(self, victim, since):
        name = self.nodes[victim]
        if command("docker", "inspect", "-f", "{{.State.Running}}", name).strip() != "true":
            raise RuntimeError("The owner exited instead of refusing the disk write")
        self.http(victim, "/health/live", (200,))
        evidence = None
        def observed():
            nonlocal evidence
            output = command("docker", "logs", "--since", since, name)
            evidence = find_disk_write_failure(output, f"n{victim}")
            return evidence is not None
        wait_until(observed, 5, "The refused write has no owner-log ENOSPC cause in its request window")
        self.record("disk-write-failure-confirmed", node=victim, **evidence)

    def finish_client(self):
        wait_until(lambda: command("docker", "inspect", "-f", "{{.State.Running}}", self.client).strip() == "false",
                   120, "client did not finish")
        code = int(command("docker", "inspect", "-f", "{{.State.ExitCode}}", self.client))
        if code != 0:
            raise RuntimeError(f"Client test failed with exit code {code}")
        import xml.etree.ElementTree as ET
        reports = list((self.root / "control/results").glob("*.trx"))
        if len(reports) != 1:
            raise RuntimeError("Expected exactly one TRX report")
        counters = ET.parse(reports[0]).find(".//{*}Counters")
        if counters is None or counters.get("total") != "1" or counters.get("passed") != "1":
            raise RuntimeError("Expected exactly one passing chaos test")
        self.record("passed")

    def close(self):
        failures = []
        for name in reversed(self.created):
            if not command("docker", "ps", "-aq", "--filter", f"name=^/{name}$").strip():
                continue
            (self.root / f"{name}.log").write_text(command("docker", "logs", "--timestamps", name, check=False),
                                                 encoding="utf-8")
            try:
                command("docker", "rm", "-f", "-v", name)
            except CommandFailed as error:
                failures.append(str(error))
        if self.network_created:
            try:
                command("docker", "network", "rm", self.name)
            except CommandFailed as error:
                failures.append(str(error))
        for data in reversed(self.mounted):
            try:
                privileged("umount", data)
            except CommandFailed as error:
                failures.append(str(error))
        if failures:
            raise RuntimeError("Cleanup failed: " + "; ".join(failures))

    def abort_client(self):
        control = self.root / "control"
        if self.client not in self.created or not control.exists():
            return
        (control / "abort").write_text("abort", encoding="ascii")
        try:
            wait_until(lambda: command("docker", "inspect", "-f", "{{.State.Running}}", self.client,
                                       check=False).strip() != "true", 15, "Client did not flush its history")
        except TimeoutError as error:
            self.record("history-flush-timeout", message=str(error))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--replicas", type=int, choices=(3, 5), required=True)
    parser.add_argument("--image", default="squirix-chaos:local")
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--commit", required=True)
    parser.add_argument("--seed", type=int, default=os.environ.get("SQUIRIX_FAULT_SEED") or str(secrets.randbits(31)))
    parser.add_argument("--fault-seconds", type=int, default=15)
    parser.add_argument("--scenario", choices=("chaos", "leader-exit", "leader-dns-loss", "disk-recovery"), default="chaos")
    args = parser.parse_args()
    if args.scenario not in ("leader-exit", "leader-dns-loss") and sys.platform != "linux":
        parser.error("Linux is required for network namespaces and bounded ext4 disks")
    if args.fault_seconds < 10:
        parser.error("faults must last at least ten seconds to cover leader expiry")
    print(f"seed={args.seed} RF={args.replicas}", flush=True)
    signal.signal(signal.SIGTERM, interrupted)
    cluster = Cluster(args)
    try:
        cluster.run()
    except Exception as error:
        cluster.record("failed", message=str(error), output=getattr(error, "stdout", None))
        cluster.abort_client()
        raise
    finally:
        cluster.close()


def interrupted(signum, frame):
    raise InterruptedError(f"Received signal {signum}")


def token_for(key):
    def encode(value):
        return base64.urlsafe_b64encode(value).rstrip(b"=")
    header = encode(b'{"alg":"HS256","typ":"JWT"}')
    claims = encode(json.dumps({"iss": "squirix-chaos", "aud": "squirix-chaos",
                                "nbf": int(time.time()) - 600, "exp": int(time.time()) + 7200}).encode())
    payload = header + b"." + claims
    return (payload + b"." + encode(hmac.new(key, payload, hashlib.sha256).digest())).decode("ascii")


def find_disk_write_failure(output, group):
    path = "/data/replication/grp-" + group.encode("utf-8").hex().upper() + "/group.log."
    for line in output.splitlines():
        try:
            event = json.loads(line)
        except json.JSONDecodeError:
            continue
        if not isinstance(event, dict) or event.get("EventId") != 4006:
            continue
        if event.get("Category") != "Squirix.Server.Node.Services.ReplicaGroupCommitter":
            continue
        cause = event.get("Exception", "")
        if not isinstance(cause, str) or not all(part in cause for part in (
                "GroupLogAppendUnconfirmedException", "System.IO.IOException: No space left on device", path)):
            continue
        operation = re.search(r"COMMIT_OUTCOME_UNKNOWN: operation '([0-9a-f]{32})'\.", cause)
        if operation is not None:
            return {"group": group, "operation_id": operation[1], "exception": cause}
    return None


def parse_leader(text, group):
    values = {}
    for line in text.splitlines():
        match = re.fullmatch(r'(squirix_replication_(?:leader_authority|term))\{([^}]+)\}\s+([0-9.eE+-]+)', line)
        if match is None:
            continue
        labels = dict(re.findall(r'(\w+)="([^"\\]*)"', match[2]))
        if labels.get("group") == group:
            values[match[1]] = float(match[3])
    if values.get("squirix_replication_leader_authority") == 1 and "squirix_replication_term" in values:
        return int(values["squirix_replication_term"])
    return None


if __name__ == "__main__":
    main()
