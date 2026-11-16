#!/usr/bin/env python3
"""Measure the pinned reference and current sources with identical benchmark fixtures."""

import argparse
from contextlib import contextmanager
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
        except (subprocess.TimeoutExpired, KeyboardInterrupt):
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
        "--buildTimeout", "600", "--stopOnFirstError",
    ]


def benchmark_environment():
    environment = os.environ.copy()
    for variable in ("SQUIRIX_BENCHMARK_SMOKE", "SQUIRIX_E2E_BENCHMARK_IN_PROCESS", "SQUIRIX_BENCH_QUICK", "BDN_ARTIFACTS"):
        environment.pop(variable, None)
    # BDN builds another generated project; keep its referenced builds consistent with the outer build.
    # Analysis remains the responsibility of the regular CI analyzed-build jobs.
    environment["RunAnalyzers"] = "false"
    environment["EnforceCodeStyleInBuild"] = "false"
    return environment


@contextmanager
def benchmark_entry_point(root, project, suite):
    if "entryPoint" not in suite:
        yield
        return
    source = repo_path(root, suite["entryPoint"])
    target = repo_path(project.parent, "Program.cs")
    original = target.read_bytes()
    try:
        target.write_bytes(source.read_bytes())
        yield
    finally:
        target.write_bytes(original)


def failure_reasons(failures):
    return {name: set(reasons.removesuffix(".").split(", "))
            for failure in failures for name, reasons in [failure.split(": ", 1)]}


def confirmation_suite(suite, names):
    cases = [case for case in suite["cases"] if case["name"] in names]
    # BDN full names use '(Parameter: value, ...)' rather than the report key syntax.
    filters = ["*" + case["name"].replace("=", ": ").replace("&", ", ") for case in cases]
    return {**suite, "cases": cases, "filters": filters}


def confirm_regressions(manifest, reports, run_pair):
    report, failures = CHECK.compare(manifest, reports / "baseline", reports / "current")
    if not failures:
        return report, failures
    initial = failure_reasons(failures)
    regression_kinds = {"time regression", "allocation regression"}
    suspects = {name for name, reasons in initial.items() if reasons & regression_kinds}
    if not suspects:
        return report, failures
    selected = [confirmation_suite(suite, suspects) for suite in manifest["suites"]]
    selected = [suite for suite in selected if suite["cases"]]
    confirmation_manifest = {**manifest, "suites": selected}
    initial_host, _ = CHECK.read_results(reports / "baseline", manifest)
    attempts = [initial]
    sections = [report, "### Independent process confirmation\n",
                "Three pairs; a regression requires the same budget to be exceeded in at least two pairs. "
                "Pair 2 reverses the original order; pair 3 restores it. "
                "Invalid or noisy confirmation measurements fail the guard.\n"]
    for attempt in (2, 3):
        directory = reports / f"confirmation-{attempt}"
        for index, suite in enumerate(manifest["suites"]):
            matching = next((item for item in selected if item["name"] == suite["name"]), None)
            if matching is not None:
                run_pair(index, matching, attempt, directory)
        summary, repeated_failures = CHECK.compare(
            confirmation_manifest, directory / "baseline", directory / "current")
        host, _ = CHECK.read_results(directory / "baseline", confirmation_manifest)
        if host != initial_host:
            raise ValueError("Confirmation must use the initial host, SDK and runtime.")
        sections.extend([f"#### Pair {attempt}\n", summary])
        attempts.append(failure_reasons(repeated_failures))
    final = []
    sections.extend(["### Final verdict\n", "| Benchmark | Verdict |", "|---|---|"])
    for name in sorted(initial):
        reasons = set(initial[name] - regression_kinds)
        if name in suspects:
            for attempt in attempts[1:]:
                reasons.update(attempt.get(name, set()) - regression_kinds)
            votes = {kind: sum(kind in attempt.get(name, set()) for attempt in attempts)
                     for kind in regression_kinds}
            reasons.update(kind for kind, count in votes.items() if count >= 2)
            details = "; ".join(f"{kind}: {votes[kind]}/3" for kind in sorted(regression_kinds))
        else:
            details = "initial measurement failed validation"
        verdict = ", ".join(sorted(reasons)) if reasons else "regression not confirmed"
        sections.append(f"| {name} | {verdict}; {details} |")
        if reasons:
            final.append(f"{name}: {', '.join(sorted(reasons))}.")
    return "\n".join(sections) + "\n", final


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
        def run_pair(index, suite, attempt, destination):
            # Alternate the order between suites to reduce systematic thermal/order bias.
            order = (("baseline", baseline), ("current", root)) if index % 2 == 0 else (("current", root), ("baseline", baseline))
            if attempt % 2 == 0:
                order = tuple(reversed(order))
            for label, checkout in order:
                suffix = "" if attempt == 1 else f"-confirmation-{attempt}"
                project = repo_path(checkout, suite["project"])
                # Bound BenchmarkDotNet project discovery to this project, including in worktree-based local runs.
                marker = project.parent / "global.json"
                if not marker.exists():
                    shutil.copyfile(root / "global.json", marker)
                    created_markers.append(marker)
                # Keep the same entry point installed while BDN builds its generated child project.
                with benchmark_entry_point(root, project, suite):
                    execute(
                        ["dotnet", "build", str(project), "-c", "Release", "-p:RunAnalyzers=false", "-p:EnforceCodeStyleInBuild=false"],
                        checkout, logs / f'{suite["name"]}-{label}{suffix}-build.log', environment, timeout=600)
                    assembly = project.parent / "bin" / "Release" / "net10.0" / f"{project.stem}.dll"
                    execute(
                        benchmark_arguments(assembly, suite["filters"], destination / label / suite["name"]),
                        project.parent, logs / f'{suite["name"]}-{label}{suffix}-benchmark.log', environment)
        for index, suite in enumerate(manifest["suites"]):
            run_pair(index, suite, 1, reports)
        metadata = {"baselineCommit": manifest["baselineCommit"], "currentCommit": current_sha, "manifest": manifest,
                    "confirmationPolicy": {"totalPairs": 3, "requiredRegressions": 2, "reverseOrderOnPair": 2}}
        (artifacts / "run.json").write_text(json.dumps(metadata, indent=2) + "\n", encoding="utf-8")
        report, failures = confirm_regressions(manifest, reports, run_pair)
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
