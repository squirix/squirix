#!/usr/bin/env python3
"""Assign every solution project to an analyzer shard for the pull request analyzed build.

The analyzed build is a serial chain of large projects, so a single job cannot use more than one
project's worth of parallelism. Each shard job first builds its projects (and their dependencies)
without analyzers, then re-compiles only its own projects with analyzers.

Usage:
  analyze-shards.py list <shard>   print the shard's projects, one per line
  analyze-shards.py verify         fail unless every project in squirix.slnx is in exactly one shard

`verify` is the safety net: a project that belongs to no shard would silently skip analysis.
"""

import sys
import xml.etree.ElementTree as ET
from pathlib import Path

# Shard id -> projects. Keep the shards roughly balanced by analysis time: the two large server test
# projects and Squirix.Server dominate, everything else is small.
SHARDS = {
    "server-unit": [
        "tests/squirix.server/squirix.server.unit-tests/Squirix.Server.UnitTests.csproj",
    ],
    "server-integration": [
        "tests/squirix.server/squirix.server.integration-tests/Squirix.Server.IntegrationTests.csproj",
        "tests/squirix.server/squirix.server.smoke-tests/Squirix.Server.SmokeTests.csproj",
    ],
    "server": [
        "src/squirix.server/Squirix.Server.csproj",
        "src/squirix.server.host/Squirix.Server.Host.csproj",
        "src/squirix/Squirix.csproj",
        "src/squirix.protocol-model/Squirix.ProtocolModel.csproj",
        "tests/squirix.server/squirix.server.testkit/Squirix.Server.TestKit.csproj",
    ],
    "clients": [
        "tests/squirix/squirix.testkit/Squirix.TestKit.csproj",
        "tests/squirix/squirix.unit-tests/Squirix.UnitTests.csproj",
        "tests/squirix/squirix.integration-tests/Squirix.IntegrationTests.csproj",
        "tests/squirix.protocol-model/squirix.protocol-model.tests/Squirix.ProtocolModel.Tests.csproj",
        "tests/squirix.e2e.tests/Squirix.E2ETests.csproj",
        "benchmarks/squirix.benchmarks/Squirix.Benchmarks.csproj",
        "benchmarks/squirix.e2e.benchmarks/Squirix.E2EBenchmarks.csproj",
        "benchmarks/squirix.server.benchmarks/Squirix.Server.Benchmarks.csproj",
        "samples/external-package-smoke/ExternalPackageSmoke.csproj",
    ],
}


def solution_projects() -> set[str]:
    root = Path(__file__).resolve().parents[2]
    tree = ET.parse(root / "squirix.slnx")
    return {e.attrib["Path"].replace("\\", "/") for e in tree.iter() if e.attrib.get("Path", "").endswith(".csproj")}


def verify() -> int:
    assigned: dict[str, str] = {}
    problems = []
    for shard, projects in SHARDS.items():
        for project in projects:
            if project in assigned:
                problems.append(f"{project} is in both '{assigned[project]}' and '{shard}'")
            assigned[project] = shard

    in_solution = solution_projects()
    problems += [f"{p} is in squirix.slnx but in no shard" for p in sorted(in_solution - assigned.keys())]
    problems += [f"{p} is in shard '{assigned[p]}' but not in squirix.slnx" for p in sorted(assigned.keys() - in_solution)]
    for problem in problems:
        print(problem, file=sys.stderr)

    if problems:
        return 1

    print(f"{len(assigned)} projects across {len(SHARDS)} shards cover squirix.slnx.")
    return 0


def main(argv: list[str]) -> int:
    if len(argv) == 3 and argv[1] == "list" and argv[2] in SHARDS:
        print("\n".join(SHARDS[argv[2]]))
        return 0

    if len(argv) == 2 and argv[1] == "verify":
        return verify()

    print(f"usage: {argv[0]} list <{'|'.join(SHARDS)}> | verify", file=sys.stderr)
    return 1


if __name__ == "__main__":
    sys.exit(main(sys.argv))
