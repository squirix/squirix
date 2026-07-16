"""Flatten sparse ND1305 namespaces into assembly-root test namespaces."""

from __future__ import annotations

import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]

MAPPINGS = {
    "Squirix.UnitTests.Support": "Squirix.UnitTests",
    "Squirix.UnitTests.Internal.Cluster.Transport": "Squirix.UnitTests",
    "Squirix.UnitTests.Internal.Cluster.Observability": "Squirix.UnitTests",
    "Squirix.UnitTests.Internal.Cluster.Bootstrap": "Squirix.UnitTests",
    "Squirix.UnitTests.Internal": "Squirix.UnitTests",
    "Squirix.UnitTests.Serialization": "Squirix.UnitTests",
    "Squirix.UnitTests.Core": "Squirix.UnitTests",
    "Squirix.UnitTests.Architecture": "Squirix.UnitTests",
    "Squirix.Server.TestKit.Cluster": "Squirix.Server.TestKit",
    "Squirix.Server.TestKit.Testing": "Squirix.Server.TestKit",
    "Squirix.Server.TestKit.Journaling": "Squirix.Server.TestKit",
    "Squirix.Server.TestKit.Environment": "Squirix.Server.TestKit",
    "Squirix.Server.TestKit.Diagnostics": "Squirix.Server.TestKit",
    "Squirix.Server.TestKit.XUnit": "Squirix.Server.TestKit",
    "Squirix.Server.TestKit.Auth": "Squirix.Server.TestKit",
    "Squirix.E2ETests.Security": "Squirix.E2ETests",
    "Squirix.E2ETests.Support": "Squirix.E2ETests",
    "Squirix.E2ETests.Stress.SingleNode": "Squirix.E2ETests",
    "Squirix.E2ETests.Persistence": "Squirix.E2ETests",
    "Squirix.E2ETests.Support.Restart": "Squirix.E2ETests",
    "Squirix.E2ETests.Support.Auth": "Squirix.E2ETests",
    "Squirix.E2ETests.Support.Cluster.Fixtures": "Squirix.E2ETests",
    "Squirix.E2ETests.Support.Stress": "Squirix.E2ETests",
    "Squirix.Server.IntegrationTests.Persistence": "Squirix.Server.IntegrationTests",
    "Squirix.Server.IntegrationTests.Options": "Squirix.Server.IntegrationTests",
    "Squirix.Server.IntegrationTests.Limits": "Squirix.Server.IntegrationTests",
    "Squirix.Server.IntegrationTests.Core": "Squirix.Server.IntegrationTests",
    "Squirix.Server.IntegrationTests.Ops": "Squirix.Server.IntegrationTests",
    "Squirix.Server.IntegrationTests.Reliability": "Squirix.Server.IntegrationTests",
    "Squirix.Server.SmokeTests.Config": "Squirix.Server.SmokeTests",
    "Squirix.Server.SmokeTests.Support": "Squirix.Server.SmokeTests",
    "Squirix.Server.SmokeTests.Observability": "Squirix.Server.SmokeTests",
    "Squirix.Server.SmokeTests.Health": "Squirix.Server.SmokeTests",
    "Squirix.Server.SmokeTests.Grpc": "Squirix.Server.SmokeTests",
    "Squirix.Server.UnitTests.Utils": "Squirix.Server.UnitTests",
    "Squirix.Server.UnitTests.Serialization": "Squirix.Server.UnitTests",
    "Squirix.Server.UnitTests.Errors": "Squirix.Server.UnitTests",
    "Squirix.Server.UnitTests.Persistence.Snapshot": "Squirix.Server.UnitTests",
    "Squirix.Server.UnitTests.Persistence.Snapshot.Binary": "Squirix.Server.UnitTests",
    "Squirix.Server.UnitTests.Persistence.Manifest": "Squirix.Server.UnitTests",
    "Squirix.Server.UnitTests.Security": "Squirix.Server.UnitTests",
    "Squirix.Server.UnitTests.Limits": "Squirix.Server.UnitTests",
}

OLD_NSS = sorted(MAPPINGS.keys(), key=len, reverse=True)


def rewrite_text(text: str) -> str:
    for old in OLD_NSS:
        new = MAPPINGS[old]
        text = re.sub(rf"^namespace\s+{re.escape(old)}\s*;", f"namespace {new};", text, flags=re.M)
        text = re.sub(rf"^namespace\s+{re.escape(old)}\s*\{{", f"namespace {new}\n{{", text, flags=re.M)

    for old in OLD_NSS:
        new = MAPPINGS[old]
        text = re.sub(rf"^using\s+{re.escape(old)}\s*;", f"using {new};", text, flags=re.M)
        text = text.replace(f"global using {old};", f"global using {new};")

    for old in OLD_NSS:
        new = MAPPINGS[old]
        text = re.sub(rf"(?<![\w.]){re.escape(old)}\.", new + ".", text)

    return text


def dedupe_usings(text: str) -> str:
    lines = text.splitlines(keepends=True)
    seen: set[str] = set()
    out: list[str] = []
    for line in lines:
        stripped = line.strip()
        match = re.match(r"^(using\s+[\w.]+\s*;)", stripped)
        if match:
            key = match.group(1)
            if key in seen:
                continue
            seen.add(key)
        out.append(line)
    return "".join(out)


def main() -> None:
    changed = 0
    scanned = 0
    for path in ROOT.rglob("*.cs"):
        if "obj" in path.parts or "bin" in path.parts:
            continue
        scanned += 1
        original = path.read_text(encoding="utf-8")
        updated = dedupe_usings(rewrite_text(original))
        if updated != original:
            path.write_text(updated, encoding="utf-8", newline="\n")
            changed += 1
    print(f"scanned={scanned} changed={changed}")


if __name__ == "__main__":
    main()
