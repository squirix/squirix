"""Run a disposable three-node failover demonstration using Docker Compose."""

import base64
import hashlib
import hmac
import json
from pathlib import Path
import queue
import ssl
import subprocess
import threading
import time
import urllib.error
import urllib.request
import uuid


ROOT = Path(__file__).resolve().parents[2]
PROJECT = "squirix-failover-" + uuid.uuid4().hex[:12]
OUTPUT = ROOT / "artifacts" / "docker-failover" / PROJECT
OUTPUT.mkdir(parents=True)
COMPOSE = ["docker", "compose", "-p", PROJECT, "-f", str(OUTPUT / "compose.json")]
NODES = ("a", "b", "c")


def run(arguments, capture=False):
    return subprocess.run(arguments, cwd=ROOT, check=True, text=True,
                          stdout=subprocess.PIPE if capture else None).stdout


def development_token():
    def encode(value):
        return base64.urlsafe_b64encode(value).rstrip(b"=")

    payload = {"iss": "https://squirix.docker.dev", "aud": "squirix",
               "sub": "failover-demo", "exp": int(time.time()) + 600}
    unsigned = encode(b'{"alg":"HS256","typ":"JWT"}') + b"." + encode(json.dumps(payload).encode())
    key = base64.b64decode("ZGV2LXNxdWlyaXgtZG9ja2VyLWp3dC1rZXkhISEhISE=")
    return (unsigned + b"." + encode(hmac.new(key, unsigned, hashlib.sha256).digest())).decode()


def details(port, token):
    request = urllib.request.Request(f"https://127.0.0.1:{port}/health/ready/details",
                                     headers={"Authorization": "Bearer " + token})
    # Only disposable loopback diagnostic probes use the self-signed development certificate.
    # The .NET client separately verifies TLS using the certificate trusted inside its container.
    with urllib.request.urlopen(request, context=ssl._create_unverified_context(), timeout=3) as response:
        return json.load(response)


def snapshot(ports, token, label):
    result = {node: details(port, token) for node, port in ports.items()}
    (OUTPUT / (label + ".json")).write_text(json.dumps(result, indent=2), encoding="utf-8")
    return result


def leaders(states):
    return [(node, group) for node, state in states.items() for group in state.get("replicaGroups", [])
            if group.get("copies")]


def container_id(node):
    identifier = run(COMPOSE + ["ps", "--all", "-q", "node-" + node], capture=True).strip()
    if not identifier or "\n" in identifier:
        raise RuntimeError("Expected exactly one container for node " + node)
    inspected = json.loads(run(["docker", "inspect", identifier], capture=True))[0]
    if inspected["Config"]["Labels"].get("com.docker.compose.project") != PROJECT:
        raise RuntimeError("Refusing to stop a container outside the demo project")
    return identifier


def next_line(lines, process, deadline):
    while time.monotonic() < deadline:
        try:
            return lines.get(timeout=1)
        except queue.Empty:
            if process.poll() is not None:
                raise RuntimeError("Client exited before completing the demonstration")
    raise TimeoutError("Client did not complete within the demo deadline")


def main():
    raw = run(["docker", "compose", "-p", PROJECT, "-f", "docker/docker-compose.yml",
               "-f", "docker/docker-compose.client.yml", "config", "--format", "json"], capture=True)
    config = json.loads(raw)
    for node in NODES:
        service = config["services"]["node-" + node]
        service.pop("container_name", None)
        service["ports"] = [{"target": 5000, "published": "0", "host_ip": "127.0.0.1", "protocol": "tcp"}]
        service["networks"]["squirix-net"] = {"aliases": ["squirix-node-" + node]}
    if any(volume.get("external") or not volume.get("name", "").startswith(PROJECT + "_")
           for volume in config.get("volumes", {}).values()):
        raise RuntimeError("Refusing to use volumes outside the disposable demo project")
    (OUTPUT / "compose.json").write_text(json.dumps(config, indent=2), encoding="utf-8")
    print("Disposable project:", PROJECT, flush=True)
    print("Evidence:", OUTPUT, flush=True)
    process = None
    try:
        run(COMPOSE + ["build"])
        run(COMPOSE + ["up", "-d", "node-a", "node-b", "node-c"])
        ports = {node: int(run(COMPOSE + ["port", "node-" + node, "5000"], capture=True).strip().rsplit(":", 1)[1])
                 for node in NODES}
        token = development_token()
        deadline = time.monotonic() + 120
        while True:
            try:
                states = snapshot(ports, token, "ready")
                if len(leaders(states)) == 3 and all(copy["state"] == "ready"
                        for _, group in leaders(states) for copy in group["copies"]):
                    break
            except (OSError, urllib.error.URLError):
                pass
            if time.monotonic() >= deadline:
                raise TimeoutError("Three replica groups did not become ready")
            time.sleep(1)

        process = subprocess.Popen(COMPOSE + ["run", "--rm", "--no-deps", "-T", "client", "--failover"],
                                   cwd=ROOT, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                                   stderr=subprocess.STDOUT, text=True, bufsize=1)
        lines = queue.Queue()

        def read_output():
            for line in process.stdout:
                print(line, end="", flush=True)
                with (OUTPUT / "client.log").open("a", encoding="utf-8") as log:
                    log.write(line)
                lines.put(line)

        threading.Thread(target=read_output, daemon=True).start()
        deadline = time.monotonic() + 180
        while not next_line(lines, process, deadline).startswith("READY:"):
            pass
        before = snapshot(ports, token, "before")
        candidates = [(node, group) for node, group in leaders(before)
                      if group["retainedEntries"] > 1 and len(group["copies"]) == 2
                      and all(copy["state"] == "ready" and copy["lagEntries"] == 0 for copy in group["copies"])]
        if not candidates:
            raise RuntimeError("No current leader with replicated data and two caught-up followers")
        node, group = candidates[0]
        print(f"KILL: node {node.upper()}, leader of group {group['groupId']}", flush=True)
        run(["docker", "kill", "--signal", "KILL", container_id(node)])
        process.stdin.write("continue\n")
        process.stdin.flush()
        result = ""
        while not result.startswith("PASS:"):
            result = next_line(lines, process, deadline).strip()
        if process.wait(timeout=60) != 0:
            raise RuntimeError("Client failed after its final operations")
        survivors = {other: port for other, port in ports.items() if other != node}
        after = snapshot(survivors, token, "after")
        elected = [other for other, candidate in leaders(after) if candidate["groupId"] == group["groupId"]]
        if len(elected) != 1:
            raise RuntimeError("Expected exactly one surviving leader for the affected group")
        summary = {"project": PROJECT, "killedNode": node.upper(), "group": group["groupId"],
                   "newLeader": elected[0].upper(), "clientResult": result}
        (OUTPUT / "result.json").write_text(json.dumps(summary, indent=2), encoding="utf-8")
        print(f"VERIFIED: group {group['groupId']} moved from {node.upper()} to {elected[0].upper()}", flush=True)
    finally:
        if process is not None and process.poll() is None:
            process.terminate()
        try:
            logs = subprocess.run(COMPOSE + ["logs", "--no-color"], cwd=ROOT, text=True,
                                  stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
            (OUTPUT / "server.log").write_text(logs.stdout, encoding="utf-8")
        finally:
            # Only this random project and its verified, newly created volumes are removed.
            run(COMPOSE + ["down", "--volumes", "--remove-orphans"])


if __name__ == "__main__":
    main()
