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


FAULTS = ("kill", "pause", "partition", "bridge", "one-way", "latency", "clock", "disk-full")


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
        self.signing_key = secrets.token_bytes(32)
        self.token = token_for(self.signing_key)

    def record(self, event, **details):
        self.events.append({"event": event, "elapsed": time.monotonic() - self.started, **details})
        (self.root / "run.json").write_text(json.dumps({
            "seed": self.args.seed, "replicas": self.args.replicas,
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
            data = directory / "data"
            data.mkdir()
            disk = directory / "disk.img"
            with disk.open("wb") as stream:
                stream.truncate(256 * 1024 * 1024)
            command("mkfs.ext4", "-q", "-F", "-m", "0", disk)
            privileged("mount", "-o", "loop", disk, data)
            self.mounted.append(data)
            (directory / "clock").write_text("+0", encoding="ascii")
            settings = {"Squirix": {"Cluster": {
                "ClusterId": self.name, "NodeId": f"n{i}", "Uri": peers[i]["Uri"], "Peers": peers,
                "ReplicaCount": self.args.replicas, "PersistenceEnabled": True, "ReplicationEnabled": True,
                "AutomaticFailoverEnabled": True, "QuorumReadsEnabled": True, "DataDirectory": "/data",
            }}}
            (directory / "settings.json").write_text(json.dumps(settings), encoding="utf-8")
            environment = {
                "SQUIRIX_CLUSTER_MTLS_INTERNAL_PORT": "5001",
                "SQUIRIX_CLUSTER_MTLS_CERT_PFX_PATH": f"/certs/{i}.pfx",
                "SQUIRIX_CLUSTER_MTLS_CA_PATH": "/certs/ca.crt",
                "ASPNETCORE_Kestrel__Certificates__Default__Path": f"/certs/{i}.pfx",
                "DOTNET_PROCESSOR_COUNT": "2", "Logging__LogLevel__Default": "Warning",
                "LD_PRELOAD": "/usr/lib/x86_64-linux-gnu/faketime/libfaketime.so.1",
                "FAKETIME_TIMESTAMP_FILE": "/config/clock", "FAKETIME_NO_CACHE": "1",
                "FAKETIME_DONT_FAKE_MONOTONIC": "1",
                "SQUIRIX_JWT_ISSUER": "squirix-chaos",
                "SQUIRIX_JWT_AUDIENCE": "squirix-chaos",
                "SQUIRIX_JWT_SIGNING_KEY": base64.b64encode(self.signing_key).decode("ascii"),
            }
            arguments = ["docker", "run", "-d", "--name", name, "--network", self.name, "--cap-add", "NET_ADMIN",
                         "--mount", f"type=bind,src={self.root / 'certs'},dst=/certs,readonly",
                         "--mount", f"type=bind,src={directory},dst=/config,readonly",
                         "--mount", f"type=bind,src={data},dst=/data"]
            for key, value in environment.items():
                arguments += ["-e", f"{key}={value}"]
            self.created.append(name)
            command(*arguments, self.args.image, "dotnet", "/app/server/Squirix.Server.Host.dll", "run",
                    "--settings", "/config/settings.json", "--persist", "--enable-replication")
        self.ips = [command("docker", "inspect", "-f", "{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}", n).strip() for n in self.nodes]
        self.record("nodes-started", addresses=self.ips)
        time.sleep(10)
        for name in self.nodes:
            if command("docker", "inspect", "-f", "{{.State.Running}}", name).strip() != "true":
                raise RuntimeError("Node failed startup: " + command("docker", "logs", name))
        self.start_client()

    def start_client(self):
        control = self.root / "control"
        control.mkdir()
        self.created.append(self.client)
        command("docker", "run", "-d", "--name", self.client, "--network", self.name,
                "--mount", f"type=bind,src={self.root / 'certs'},dst=/certs,readonly",
                "--mount", f"type=bind,src={control},dst=/control",
                "-e", "DOTNET_PROCESSOR_COUNT=2", "-e", "SQUIRIX_CHAOS_CONTROL=/control",
                "-e", f"SQUIRIX_CHAOS_STEPS={len(self.plan)}",
                "-e", "SQUIRIX_CHAOS_TOKEN=" + self.token,
                "-e", "SQUIRIX_CHAOS_ENDPOINTS=" + ";".join(f"https://{n}:5000" for n in self.nodes),
                self.args.image, "dotnet", "/app/client/Squirix.E2ETests.dll",
                "--treenode-filter", "/*/*/ProcessChaosTests/ProcessFaultsKeepRegistersLinearizable",
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
                   f"timeout 5 openssl s_client -quiet -connect localhost:5000 -servername {name} "
                   f"-verify_hostname {name} -verify_return_error -CAfile /certs/ca.crt 2>/dev/null")
        # OpenSSL can report unexpected TLS EOF after a complete HTTP response.
        output = command("docker", "exec", name, "sh", "-c", request, check=False)
        if not any(output.startswith(f"HTTP/1.1 {status} ") for status in statuses):
            raise RuntimeError(f"Invalid diagnostic response from {name} for {path}: {output}")
        return output

    def target_leader(self, planned):
        if planned["kind"] not in ("kill", "pause"):
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
        if step["kind"] not in ("kill", "pause"):
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

    def fault(self, step):
        kind, nodes = step["kind"], step["nodes"]
        victim = nodes[0]
        if kind == "kill":
            command("docker", "kill", "--signal", "KILL", self.nodes[victim])
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
            output = self.execute(victim, "sh", "-c", "LC_ALL=C dd if=/dev/zero of=/data/chaos-fill bs=1M; test $? -ne 0")
            if "No space left on device" not in output:
                raise RuntimeError("Disk full was not observed: " + output)
        else:
            raise ValueError(kind)
        self.record("fault-active", **step)

    def heal(self, step):
        kind, nodes = step["kind"], step["nodes"]
        victim = nodes[0]
        if kind == "kill":
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
        for index, step in enumerate(self.plan):
            ready = self.root / "control" / f"{index}.ready"
            def prepared():
                self.client_alive()
                return ready.exists()
            wait_until(prepared, 120, f"workload {index} did not become ready")
            step = self.target_leader(step)
            self.fault(step)
            time.sleep(self.args.fault_seconds)
            self.await_replacement(step)
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
                command("docker", "rm", "-f", name)
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
    args = parser.parse_args()
    if sys.platform != "linux":
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
