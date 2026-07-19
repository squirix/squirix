#!/usr/bin/env python3
"""ND2012: rename server/multi-node homonyms; keep client/SingleNode canonical names."""

from __future__ import annotations

import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]

# (old, new, roots relative to repo) — longer old names first within each batch.
RENAMES: list[tuple[str, str, tuple[str, ...]]] = [
    (
        "PublicApiGoldenSnapshotTests",
        "NodePublicApiGoldenSnapshotTests",
        ("tests/squirix.server/squirix.server.unit-tests/ApiSnapshots",),
    ),
    (
        "ExportedApiMetadata",
        "NodeExportedApiMetadata",
        (
            "tests/squirix.server/squirix.server.testkit",
            "tests/squirix.server/squirix.server.unit-tests",
        ),
    ),
    (
        "IntegrationTestBase",
        "NodeIntegrationTestBase",
        ("tests/squirix.server/squirix.server.integration-tests",),
    ),
    (
        "MeasurementSink",
        "NodeMeasurementSink",
        (
            "tests/squirix.server/squirix.server.testkit",
            "tests/squirix.server/squirix.server.unit-tests",
            "tests/squirix.server/squirix.server.integration-tests",
            "tests/squirix.server/squirix.server.smoke-tests",
            "tests/squirix.e2e.tests",
        ),
    ),
    (
        "UnitTestBase",
        "NodeUnitTestBase",
        ("tests/squirix.server/squirix.server.unit-tests",),
    ),
    (
        "PathKit",
        "NodePathKit",
        (
            "tests/squirix.server/squirix.server.testkit",
            "tests/squirix.server/squirix.server.unit-tests",
            "tests/squirix.server/squirix.server.integration-tests",
            "tests/squirix.server/squirix.server.smoke-tests",
            "tests/squirix.e2e.tests",
        ),
    ),
    # Architecture type currently named Tests after ND2013.
    (
        "Tests",
        "NodeArchitectureTests",
        ("tests/squirix.server/squirix.server.unit-tests/Architecture",),
    ),
    (
        "TypedValueTests",
        "CrossNodeTypedValueTests",
        ("tests/squirix.e2e.tests/Cache/MultiNode",),
    ),
    (
        "ExpirationTests",
        "CrossNodeExpirationTests",
        ("tests/squirix.e2e.tests/Cache/MultiNode",),
    ),
    (
        "CrudTests",
        "CrossNodeCrudTests",
        ("tests/squirix.e2e.tests/Cache/MultiNode",),
    ),
    (
        "TestBase",
        "CrossNodeTestBase",
        ("tests/squirix.e2e.tests/Cache/MultiNode",),
    ),
    (
        "TestBase",
        "LoadTestBase",
        ("tests/squirix.e2e.tests/Support/Stress", "tests/squirix.e2e.tests/Stress"),
    ),
]


def iter_cs(roots: tuple[str, ...]) -> list[Path]:
    files: list[Path] = []
    for rel in roots:
        base = ROOT / rel
        if not base.exists():
            print(f"missing root: {rel}", file=sys.stderr)
            continue
        for p in base.rglob("*.cs"):
            if "bin" in p.parts or "obj" in p.parts:
                continue
            files.append(p)
    return files


def main() -> int:
    total = 0
    renamed_files = 0
    for old, new, roots in RENAMES:
        pattern = re.compile(rf"\b{re.escape(old)}\b")
        for path in iter_cs(roots):
            text = path.read_text(encoding="utf-8")
            updated, n = pattern.subn(new, text)
            if n:
                path.write_text(updated, encoding="utf-8", newline="\n")
                total += n
                print(f"  {path.relative_to(ROOT)}: {old} -> {new} ({n})")

        for rel in roots:
            base = ROOT / rel
            for path in list(base.rglob(f"{old}.cs")):
                if "bin" in path.parts or "obj" in path.parts:
                    continue
                dest = path.with_name(f"{new}.cs")
                if dest.exists():
                    print(f"SKIP exists: {dest}", file=sys.stderr)
                    continue
                path.rename(dest)
                renamed_files += 1
                print(f"  rename {path.relative_to(ROOT)} -> {dest.name}")

    print(f"Done: {total} replacements, {renamed_files} file renames")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
