"""Fix StyleCop SA1208/SA1210 using order after ND1305 flatten."""

from __future__ import annotations

import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2] / "tests"


def using_sort_key(line: str) -> tuple[int, str]:
    ns = line.strip()[6:-1].strip()
    if ns == "System":
        return (0, ns.lower())
    if ns.startswith("System."):
        return (1, ns.lower())
    return (2, ns.lower())


def fix_usings(text: str) -> str:
    lines = text.splitlines(keepends=True)
    start = None
    for j, line in enumerate(lines):
        s = line.strip()
        if s.startswith("using ") and s.endswith(";") and not s.startswith("using static") and "=" not in s:
            start = j
            break
        if s.startswith("namespace"):
            return text
    if start is None:
        return text

    i = start
    usings: list[str] = []
    while i < len(lines):
        s = lines[i].strip()
        if s.startswith("using ") and s.endswith(";") and not s.startswith("using static") and "=" not in s:
            usings.append(lines[i] if lines[i].endswith("\n") else lines[i] + "\n")
            i += 1
            continue
        break

    if len(usings) < 1:
        return text

    sorted_u = sorted(usings, key=using_sort_key)
    seen: set[str] = set()
    dedup: list[str] = []
    for u in sorted_u:
        key = u.strip()
        if key in seen:
            continue
        seen.add(key)
        dedup.append(u if u.endswith("\n") else u + "\n")

    if dedup == usings:
        return text
    return "".join(lines[:start] + dedup + lines[i:])


def ensure_using(path: Path, ns: str) -> None:
    text = path.read_text(encoding="utf-8")
    directive = f"using {ns};"
    if directive in text:
        return
    lines = text.splitlines(keepends=True)
    insert_at = 0
    for j, line in enumerate(lines):
        s = line.strip()
        if s.startswith("using ") and s.endswith(";"):
            insert_at = j + 1
            continue
        if s.startswith("namespace") or s.startswith("["):
            break
        if insert_at and s == "":
            break
    lines.insert(insert_at, directive + "\n")
    path.write_text(fix_usings("".join(lines)), encoding="utf-8", newline="\n")


def main() -> None:
    changed = 0
    for path in ROOT.rglob("*.cs"):
        if "obj" in path.parts or "bin" in path.parts:
            continue
        original = path.read_text(encoding="utf-8")
        updated = fix_usings(original)
        if updated != original:
            path.write_text(updated, encoding="utf-8", newline="\n")
            changed += 1
    print(f"sorted={changed}")

    for rel in (
        "squirix.server/squirix.server.unit-tests/Persistence/Manifest/RetentionBurstTests.cs",
        "squirix.server/squirix.server.unit-tests/Persistence/Manifest/StoreTests.cs",
        "squirix.server/squirix.server.unit-tests/Persistence/Journaling/JournalSegmentRollTests.cs",
    ):
        ensure_using(ROOT / rel, "Squirix.Server.UnitTests.Persistence")
        print(f"ensured Persistence using: {rel}")


if __name__ == "__main__":
    main()
