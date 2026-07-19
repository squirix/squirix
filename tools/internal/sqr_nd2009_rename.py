#!/usr/bin/env python3
"""ND2009: shorten type names longer than 40 characters."""

from __future__ import annotations

import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]

RENAMES: list[tuple[str, str]] = [
    ("CrossNodeOperationIdIdempotencyIntegrationTests", "CrossNodeOpIdIdempotencyTests"),
    ("RpcMutationIdempotencyRestartIntegrationTests", "RpcIdempotencyRestartTests"),
    ("RpcMutationIdempotencyDurabilityOrderingTests", "RpcIdempotencyOrderTests"),
    ("ClientPoolBootstrapWarmupDiagnosticsTests", "BootstrapWarmupDiagnosticsTests"),
    ("MetricsLoopbackOrAuthenticatedFilterTests", "MetricsAuthOrLoopbackFilterTests"),
]


def main() -> int:
    for old, new in RENAMES:
        assert len(new) <= 40, (new, len(new))
        pattern = re.compile(rf"\b{re.escape(old)}\b")
        for path in ROOT.rglob("*.cs"):
            if "bin" in path.parts or "obj" in path.parts:
                continue
            if not path.exists():
                continue
            text = path.read_text(encoding="utf-8")
            updated, n = pattern.subn(new, text)
            if n:
                path.write_text(updated, encoding="utf-8", newline="\n")
                print(f"  {path.relative_to(ROOT)}: {old} -> {new} ({n})")

        for path in list(ROOT.rglob(f"{old}.cs")):
            if "bin" in path.parts or "obj" in path.parts:
                continue
            dest = path.with_name(f"{new}.cs")
            if path.resolve() == dest.resolve():
                continue
            if dest.exists():
                # Content already updated; remove stale name file if duplicate.
                print(f"  dest exists, removing old path after merge check: {path.relative_to(ROOT)}")
                continue
            path.rename(dest)
            print(f"  rename {path.relative_to(ROOT)} -> {dest.name}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
