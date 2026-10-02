#!/usr/bin/env python3
"""Compare the allocations of the CI wire allocation benchmarks with their baseline.

Usage:
  check-wire-alloc.py <BenchmarkDotNet artifacts dir> <baseline json>            check
  check-wire-alloc.py <BenchmarkDotNet artifacts dir> <baseline json> --write    rewrite the baseline

The baseline holds allocated bytes per operation, measured on the CI runner, for every benchmark
the CI filter selects. A check fails when a benchmark allocates more than its baseline plus the
tolerance, when a benchmark has no baseline, or when a baseline entry was not measured (a filter
that selects nothing must not pass). Allocations below the baseline by more than the tolerance
pass with a note to lower the baseline. A table of every benchmark goes to stdout and, in CI,
to the job summary.
"""

import glob
import json
import os
import sys


def read_results(artifacts):
    results = {}
    for path in glob.glob(os.path.join(artifacts, "results", "*.json")):
        with open(path, encoding="utf-8-sig") as report:
            for benchmark in json.load(report).get("Benchmarks", []):
                allocated = (benchmark.get("Memory") or {}).get("BytesAllocatedPerOperation")
                if allocated is not None:
                    parameters = benchmark.get("Parameters") or ""
                    name = f'{benchmark["Type"]}.{benchmark["Method"]}' + (f"({parameters})" if parameters else "")
                    results[name] = int(allocated)
    return results


def main(argv):
    if len(argv) not in (3, 4) or (len(argv) == 4 and argv[3] != "--write"):
        print(__doc__, file=sys.stderr)
        return 2

    artifacts, baseline_path = argv[1], argv[2]
    results = read_results(artifacts)
    if not results:
        print(f"No benchmark results with allocation data under {artifacts}/results.", file=sys.stderr)
        return 1

    with open(baseline_path, encoding="utf-8") as file:
        baseline = json.load(file)

    if len(argv) == 4:
        baseline["bytesPerOperation"] = dict(sorted(results.items()))
        with open(baseline_path, "w", encoding="utf-8", newline="\n") as file:
            json.dump(baseline, file, indent=2)
            file.write("\n")
        print(f"Wrote {len(results)} baseline entries to {baseline_path}.")
        return 0

    tolerance = float(baseline["tolerance"])
    expected = baseline["bytesPerOperation"]
    failures = []
    lines = [
        "### Wire allocation guard",
        "",
        f"Tolerance: {tolerance:.0%} over the baseline in `{baseline_path}`.",
        "",
        "| Benchmark | Baseline (B/op) | Current (B/op) | Change | Result |",
        "|---|---:|---:|---:|---|",
    ]
    for name in sorted(set(expected) | set(results)):
        base, current = expected.get(name), results.get(name)
        if base is None:
            failures.append(f"{name} has no baseline.")
            lines.append(f"| {name} | - | {current} | - | no baseline |")
            continue
        if current is None:
            failures.append(f"{name} was not measured.")
            lines.append(f"| {name} | {base} | - | - | not measured |")
            continue
        change = current / base - 1 if base else 0.0
        if change > tolerance:
            verdict = "regression"
            failures.append(f"{name} allocates {current} B/op, {change:+.1%} over the baseline of {base} B/op.")
        elif change < -tolerance:
            verdict = "improved, lower the baseline"
        else:
            verdict = "ok"
        lines.append(f"| {name} | {base} | {current} | {change:+.1%} | {verdict} |")

    if failures:
        lines += ["", "To accept new allocations, rerun this script with `--write` on the results of the CI run and commit the baseline."]
    report = "\n".join(lines) + "\n"
    print(report)
    summary = os.environ.get("GITHUB_STEP_SUMMARY")
    if summary:
        with open(summary, "a", encoding="utf-8") as file:
            file.write(report)
    for failure in failures:
        print(failure, file=sys.stderr)
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
