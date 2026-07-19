"""Move test .cs files so folder path matches namespace; delete empty dirs."""

from __future__ import annotations

import re
import shutil
from pathlib import Path

TESTS = Path(__file__).resolve().parents[2] / "tests"


def root_namespace(csproj: Path) -> str:
    text = csproj.read_text(encoding="utf-8")
    match = re.search(r"<RootNamespace>([^<]+)</RootNamespace>", text)
    if match:
        return match.group(1)
    match = re.search(r"<AssemblyName>([^<]+)</AssemblyName>", text)
    if match:
        return match.group(1)
    return csproj.stem


def file_namespace(path: Path) -> str | None:
    text = path.read_text(encoding="utf-8")
    match = re.search(r"^namespace\s+([\w.]+)\s*;", text, re.M)
    if match:
        return match.group(1)
    match = re.search(r"^namespace\s+([\w.]+)\s*\{", text, re.M)
    return match.group(1) if match else None


def expected_path(project_dir: Path, root_ns: str, ns: str, file_name: str) -> Path | None:
    if ns == root_ns:
        return project_dir / file_name
    if not ns.startswith(root_ns + "."):
        return None
    suffix = ns[len(root_ns) + 1 :]
    return project_dir.joinpath(*suffix.split(".")) / file_name


def delete_empty_dirs(root: Path) -> int:
    removed = 0
    # deepest first
    for path in sorted(root.rglob("*"), key=lambda p: len(p.parts), reverse=True):
        if not path.is_dir():
            continue
        if "obj" in path.parts or "bin" in path.parts:
            continue
        try:
            if any(path.iterdir()):
                continue
            path.rmdir()
            removed += 1
            print(f"rmdir {path.relative_to(TESTS)}")
        except OSError:
            pass
    return removed


def main() -> None:
    moved = 0
    skipped = 0
    for csproj in TESTS.rglob("*.csproj"):
        if "obj" in csproj.parts:
            continue
        project_dir = csproj.parent
        root_ns = root_namespace(csproj)
        for source in list(project_dir.rglob("*.cs")):
            if "obj" in source.parts or "bin" in source.parts:
                continue
            ns = file_namespace(source)
            if ns is None:
                continue
            dest = expected_path(project_dir, root_ns, ns, source.name)
            if dest is None:
                skipped += 1
                continue
            if source.resolve() == dest.resolve():
                continue
            if dest.exists():
                raise SystemExit(f"Collision: {dest} already exists (from {source})")
            dest.parent.mkdir(parents=True, exist_ok=True)
            shutil.move(str(source), str(dest))
            print(f"move {source.relative_to(TESTS)} -> {dest.relative_to(TESTS)}")
            moved += 1

    removed = delete_empty_dirs(TESTS)
    print(f"moved={moved} skipped={skipped} empty_dirs_removed={removed}")


if __name__ == "__main__":
    main()
