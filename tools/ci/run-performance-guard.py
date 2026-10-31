#!/usr/bin/env python3
"""Measure the pinned release and current sources with identical benchmark fixtures."""

import argparse
import importlib.util
import json
import os
from pathlib import Path
import shutil
import signal
import subprocess
import sys
import tempfile


SPEC = importlib.util.spec_from_file_location("performance_check", Path(__file__).with_name("check-performance.py"))
CHECK = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(CHECK)


def repo_path(root, relative):
    path = (root / relative).resolve()
    if not path.is_relative_to(root.resolve()):
        raise ValueError(f"Path escapes the source checkout: {relative}")
    return path


def execute(arguments, cwd, log, environment, timeout=1800):
    print(f"Running {arguments[0]} in {cwd}; log: {log}", flush=True)
    log.parent.mkdir(parents=True, exist_ok=True)
    with log.open("w", encoding="utf-8") as stream:
        process = subprocess.Popen(
            arguments, cwd=cwd, env=environment, stdout=stream, stderr=subprocess.STDOUT,
            start_new_session=os.name != "nt")
        try:
            return_code = process.wait(timeout=timeout)
        except subprocess.TimeoutExpired:
            # Stop the benchmark process tree before removing its source checkout.
            try:
                if os.name == "nt":
                    subprocess.run(
                        ["taskkill", "/PID", str(process.pid), "/T", "/F"],
                        stdout=stream, stderr=subprocess.STDOUT, check=False, timeout=30)
                else:
                    try:
                        os.killpg(process.pid, signal.SIGKILL)
                    except ProcessLookupError:
                        pass
            finally:
                if process.poll() is None:
                    process.kill()
                process.wait(timeout=30)
            raise
        if return_code:
            raise subprocess.CalledProcessError(return_code, arguments)


def benchmark_arguments(assembly, filters, artifacts):
    return [
        "dotnet", str(assembly), "--filter", *filters,
        "--launchCount", "1", "--warmupCount", "10", "--iterationCount", "10",
        "--iterationTime", "500", "--outliers", "DontRemove",
        "--exporters", "json", "--artifacts", str(artifacts),
    ]


def benchmark_environment():
    environment = os.environ.copy()
    for variable in ("SQUIRIX_BENCHMARK_SMOKE", "SQUIRIX_E2E_BENCHMARK_IN_PROCESS", "BDN_ARTIFACTS"):
        environment.pop(variable, None)
    return environment


def run(manifest_path, artifacts):
    root = Path(__file__).resolve().parents[2]
    manifest = CHECK.load_manifest(manifest_path)
    artifacts = Path(artifacts).resolve()
    if artifacts == root or root.is_relative_to(artifacts):
        raise ValueError("Artifacts must not be a checkout or its ancestor.")
    artifacts.mkdir(parents=True, exist_ok=True)
    reports = artifacts / "reports"
    if reports.exists():
        raise ValueError("Use a fresh artifact directory; stale reports must not satisfy the guard.")
    logs = artifacts / "logs"
    environment = benchmark_environment()
    baseline = Path(tempfile.mkdtemp(prefix="squirix-perf-release-", dir=os.environ.get("RUNNER_TEMP"))).resolve()
    registered = False
    created_markers = []
    current_sha = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=root, text=True, timeout=60).strip()
    try:
        execute(["git", "worktree", "add", "--detach", str(baseline), manifest["baselineCommit"]], root, logs / "checkout.log", environment, timeout=120)
        registered = True
        # Both builds use the current SDK pin and the same selected benchmark source files.
        shutil.copyfile(root / "global.json", baseline / "global.json")
        for suite in manifest["suites"]:
            project = repo_path(root, suite["project"])
            relative_directory = project.parent.relative_to(root)
            for source in suite["sources"]:
                original = repo_path(project.parent, source)
                target = repo_path(baseline, relative_directory / source)
                target.parent.mkdir(parents=True, exist_ok=True)
                shutil.copyfile(original, target)
        for index, suite in enumerate(manifest["suites"]):
            # Alternate the order between suites to reduce systematic thermal/order bias.
            order = (("baseline", baseline), ("current", root)) if index % 2 == 0 else (("current", root), ("baseline", baseline))
            for label, checkout in order:
                project = repo_path(checkout, suite["project"])
                # Bound BenchmarkDotNet project discovery to this project, including in worktree-based local runs.
                marker = project.parent / "global.json"
                if not marker.exists():
                    shutil.copyfile(root / "global.json", marker)
                    created_markers.append(marker)
                execute(
                    ["dotnet", "build", str(project), "-c", "Release", "-p:RunAnalyzers=false", "-p:EnforceCodeStyleInBuild=false"],
                    checkout, logs / f'{suite["name"]}-{label}-build.log', environment, timeout=600)
                assembly = project.parent / "bin" / "Release" / "net10.0" / f"{project.stem}.dll"
                execute(
                    benchmark_arguments(assembly, suite["filters"], reports / label / suite["name"]),
                    project.parent, logs / f'{suite["name"]}-{label}-benchmark.log', environment)
        metadata = {"baselineCommit": manifest["baselineCommit"], "currentCommit": current_sha, "manifest": manifest}
        (artifacts / "run.json").write_text(json.dumps(metadata, indent=2) + "\n", encoding="utf-8")
        report, failures = CHECK.compare(manifest, reports / "baseline", reports / "current")
        (artifacts / "summary.md").write_text(report, encoding="utf-8")
        print(report)
        summary = environment.get("GITHUB_STEP_SUMMARY")
        if summary:
            with open(summary, "a", encoding="utf-8") as stream:
                stream.write(report)
        for failure in failures:
            print(failure, file=sys.stderr)
        return 1 if failures else 0
    finally:
        for marker in created_markers:
            marker.unlink(missing_ok=True)
        # Only the exact temporary checkout created above is eligible for recursive worktree removal.
        if registered and baseline.name.startswith("squirix-perf-release-") and baseline != root and not root.is_relative_to(baseline):
            subprocess.run(["git", "worktree", "remove", "--force", str(baseline)], cwd=root, check=True, timeout=120)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("manifest")
    parser.add_argument("artifacts")
    args = parser.parse_args(argv)
    try:
        return run(Path(args.manifest).resolve(), args.artifacts)
    except (ValueError, KeyError, OSError, subprocess.SubprocessError) as error:
        print(f"Performance guard failed: {error}. See the preserved artifact logs.", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
