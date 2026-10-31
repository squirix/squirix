#!/usr/bin/env python3
"""Compare native BenchmarkDotNet reports with a release measured on the same runner."""

import argparse
import json
import math
import os
from pathlib import Path
import re
import statistics
import sys


HOST_FIELDS = (
    "ProcessorName", "LogicalCoreCount", "Architecture", "OsVersion",
    "RuntimeVersion", "DotNetCliVersion", "BenchmarkDotNetVersion",
)


def finite_number(value, label, minimum=0):
    if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value) or value < minimum:
        raise ValueError(f"{label} must be a finite number >= {minimum}.")
    return float(value)


def load_manifest(path):
    manifest = json.loads(Path(path).read_text(encoding="utf-8"))
    if not re.fullmatch(r"[0-9a-f]{40}", manifest["baselineCommit"]):
        raise ValueError("The baseline must be an immutable full commit SHA.")
    for key in ("timeTolerance", "allocationTolerance", "maxRelativeStandardError"):
        if not 0 <= finite_number(manifest[key], key) < 1:
            raise ValueError(f"Invalid {key}.")
    if not isinstance(manifest["minSamples"], int) or manifest["minSamples"] < 5:
        raise ValueError("At least five measured iterations are required.")
    cases = {}
    suite_names = set()
    for suite in manifest["suites"]:
        if not isinstance(suite["name"], str) or not re.fullmatch(r"[a-z][a-z0-9-]*", suite["name"]) or suite["name"] in suite_names:
            raise ValueError("Suite names must be unique lowercase path components using letters, digits and hyphens.")
        suite_names.add(suite["name"])
        if not suite["filters"] or not suite["sources"]:
            raise ValueError("Each suite needs filters and shared benchmark sources.")
        for case in suite["cases"]:
            if not case["name"] or case["name"] in cases or not isinstance(case["checkTime"], bool):
                raise ValueError("Benchmark cases must have unique names and an explicit timing policy.")
            cases[case["name"]] = case["checkTime"]
    if not cases:
        raise ValueError("The guard must select benchmark cases.")
    return manifest


def expected_cases(manifest):
    return {case["name"]: case["checkTime"] for suite in manifest["suites"] for case in suite["cases"]}


def read_results(directory, manifest):
    results = {}
    host = None
    for path in sorted(Path(directory).glob("*/results/*.json")):
        report = json.loads(path.read_text(encoding="utf-8-sig"))
        info = report["HostEnvironmentInfo"]
        identity = tuple(info[field] for field in HOST_FIELDS)
        if any(value is None or value == "" for value in identity):
            raise ValueError(f"Missing host identity in {path}.")
        if host is not None and host != identity:
            raise ValueError("Reports came from different hosts or runtimes.")
        host = identity
        for benchmark in report.get("Benchmarks", []):
            if "InProcess" in benchmark.get("DisplayInfo", ""):
                raise ValueError("Timing comparisons require native child processes, not in-process reports.")
            parameters = benchmark.get("Parameters") or ""
            name = f'{benchmark["Type"]}.{benchmark["Method"]}' + (f"({parameters})" if parameters else "")
            if name in results:
                raise ValueError(f"Duplicate result for {name}.")
            stats = benchmark.get("Statistics") or {}
            samples = stats.get("OriginalValues") or []
            if len(samples) < manifest["minSamples"] or stats.get("N") != len(samples):
                raise ValueError(f"{name} has insufficient measurement samples; Dry/Smoke output is not valid.")
            values = [finite_number(value, name) for value in samples]
            mean = finite_number(stats.get("Mean"), f"{name} mean")
            error = finite_number(stats.get("StandardError"), f"{name} standard error")
            allocated = finite_number((benchmark.get("Memory") or {}).get("BytesAllocatedPerOperation"), f"{name} allocated bytes")
            if mean == 0 and expected_cases(manifest).get(name, True):
                raise ValueError(f"{name} has no measurable execution time.")
            results[name] = {"median": statistics.median(values), "mean": mean, "error": error, "allocated": allocated}
    expected = set(expected_cases(manifest))
    if set(results) != expected:
        raise ValueError(f"Benchmark set mismatch: missing={sorted(expected - set(results))}; unexpected={sorted(set(results) - expected)}.")
    return host, results


def compare(manifest, baseline_directory, current_directory):
    baseline_host, baseline = read_results(baseline_directory, manifest)
    current_host, current = read_results(current_directory, manifest)
    if baseline_host != current_host:
        raise ValueError("Baseline and current reports must use the same host, SDK, runtime and BenchmarkDotNet version.")
    failures = []
    lines = [
        "### Preview.10 performance guard", "",
        f'Baseline commit: {manifest["baselineCommit"]}.',
        f'Time budget: +{manifest["timeTolerance"]:.0%}; allocation budget: +{manifest["allocationTolerance"]:.0%}.',
        "Times are medians from native child processes on this runner; these are local hot paths, not RPC p99.", "",
        "| Benchmark | Release ns/op | Current ns/op | Time change | Release B/op | Current B/op | Result |",
        "|---|---:|---:|---:|---:|---:|---|",
    ]
    for name, check_time in sorted(expected_cases(manifest).items()):
        old, new = baseline[name], current[name]
        reasons = []
        change = new["median"] / old["median"] - 1 if check_time and old["median"] else None
        if new["allocated"] > old["allocated"] * (1 + manifest["allocationTolerance"]):
            reasons.append("allocation regression")
        if check_time:
            for label, sample in (("release", old), ("current", new)):
                if sample["mean"] == 0 or sample["error"] / sample["mean"] > manifest["maxRelativeStandardError"]:
                    reasons.append(f"noisy {label} measurement; rerun")
            if old["median"] == 0 or change > manifest["timeTolerance"] + 1e-12:
                reasons.append("time regression")
        if reasons:
            failures.append(f"{name}: {', '.join(reasons)}.")
        delta = f"{change:+.1%}" if change is not None else "allocation only"
        verdict = ", ".join(reasons) if reasons else "ok"
        lines.append(f'| {name} | {old["median"]:.3f} | {new["median"]:.3f} | {delta} | {old["allocated"]:g} | {new["allocated"]:g} | {verdict} |')
    return "\n".join(lines) + "\n", failures


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("manifest")
    parser.add_argument("baseline")
    parser.add_argument("current")
    parser.add_argument("--report")
    args = parser.parse_args(argv)
    try:
        manifest = load_manifest(args.manifest)
        report, failures = compare(manifest, args.baseline, args.current)
    except (ValueError, KeyError, TypeError, OSError, json.JSONDecodeError) as error:
        print(f"Performance guard could not validate its measurements: {error}", file=sys.stderr)
        return 1
    print(report)
    if args.report:
        Path(args.report).write_text(report, encoding="utf-8")
    summary = os.environ.get("GITHUB_STEP_SUMMARY")
    if summary:
        with open(summary, "a", encoding="utf-8") as stream:
            stream.write(report)
    for failure in failures:
        print(failure, file=sys.stderr)
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
