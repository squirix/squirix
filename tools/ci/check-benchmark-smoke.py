#!/usr/bin/env python3
"""Require successful BenchmarkDotNet results for every smoke-run filter.

Usage: check-benchmark-smoke.py <artifacts dir> <filter>...
The same filters select the benchmarks and check their reports. Timings are not
compared: a smoke run only proves that setup and execution produce a result.
"""

import fnmatch
import json
import math
from pathlib import Path
import sys


def main(argv):
    if len(argv) < 3:
        print(__doc__, file=sys.stderr)
        return 2

    results = []
    failures = []
    for path in sorted((Path(argv[1]) / "results").glob("*.json")):
        if path.name.endswith("-compressed.json") and path.with_name(path.name.replace("-compressed.json", ".json")).exists():
            continue
        with path.open(encoding="utf-8-sig") as report:
            benchmarks = json.load(report).get("Benchmarks", [])
        for benchmark in benchmarks:
            name = f'{benchmark.get("Type", "")}.{benchmark.get("Method", "")}'
            mean = (benchmark.get("Statistics") or {}).get("Mean")
            if not isinstance(mean, (int, float)) or not math.isfinite(mean) or mean < 0:
                failures.append(f'{name} ({benchmark.get("Parameters") or "no parameters"}) has no successful measurement in {path}.')
            else:
                results.append(name)

    for pattern in argv[2:]:
        if not any(fnmatch.fnmatchcase(name, pattern) for name in results):
            failures.append(f"No successful benchmark result matches {pattern}.")

    for failure in failures:
        print(failure, file=sys.stderr)
    if failures:
        return 1

    print(f"Benchmark smoke passed: {len(results)} results cover all {len(argv) - 2} filters.")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
