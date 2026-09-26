#!/usr/bin/env python3
"""Decide which projects the pull request analyzed build has to cover.

Roslyn diagnostics of a project depend only on its own sources, its analyzer configuration, and the metadata of
the projects it references. So a pull request only needs the analyzers on the projects it changed and on every
project that (transitively) references them; everything else was already analyzed on develop.

Usage:
  select-analysis-scope.py <base-commit>   diff <base-commit> against HEAD
  select-analysis-scope.py --files         read changed paths from stdin, one per line

Output (stdout): the first line is the scope, the following lines are the projects to analyze.
  full    analyze the whole solution (a shared setting or the CI mechanism changed, or the change reaches so many
          projects that the per-project flow would cost more than a plain analyzed build)
  subset  analyze only the listed projects
  none    no project is affected

Anything unexpected must be treated as `full` by the caller.
"""

import subprocess
import sys
import xml.etree.ElementTree as ET
from pathlib import Path, PurePosixPath

ROOT = Path(__file__).resolve().parents[2]

# Changing any of these alters the analysis of every project.
GLOBAL_BASENAMES = {".editorconfig", ".globalconfig", "global.json", "stylecop.json", "nuget.config", "squirix.slnx"}
GLOBAL_BASENAME_PREFIXES = ("Directory.Build.", "Directory.Packages.", "BannedSymbols")
# The CI mechanism itself: never trust a partial analysis of a change to it.
GLOBAL_PREFIXES = (".github/", "tools/ci/")

# Squirix.Server is the root of the heavy serial chain: touching it reaches almost everything.
HEAVY_ROOT = "src/squirix.server/Squirix.Server.csproj"
FULL_WHEN_AT_LEAST = 12


def solution_projects() -> list[str]:
    tree = ET.parse(ROOT / "squirix.slnx")
    return [e.attrib["Path"].replace("\\", "/") for e in tree.iter() if e.attrib.get("Path", "").endswith(".csproj")]


def references(project: str) -> list[str]:
    tree = ET.parse(ROOT / project)
    base = PurePosixPath(project).parent
    found = []
    for e in tree.iter("ProjectReference"):
        target = (base / e.attrib["Include"].replace("\\", "/")).as_posix()
        found.append(str(PurePosixPath(*_normalize(target))))
    return found


def _normalize(path: str) -> list[str]:
    parts: list[str] = []
    for part in path.split("/"):
        if part == "..":
            if parts:
                parts.pop()
        elif part not in ("", "."):
            parts.append(part)
    return parts


def is_global(path: str) -> bool:
    name = PurePosixPath(path).name
    return (
        name in GLOBAL_BASENAMES
        or name.startswith(GLOBAL_BASENAME_PREFIXES)
        or path.startswith(GLOBAL_PREFIXES)
    )


def owner(path: str, projects: list[str]) -> str | None:
    best, best_len = None, -1
    for project in projects:
        directory = str(PurePosixPath(project).parent) + "/"
        if path.startswith(directory) and len(directory) > best_len:
            best, best_len = project, len(directory)
    return best


def scope_for(changed: list[str]) -> tuple[str, list[str]]:
    projects = solution_projects()
    changed = [c.strip().replace("\\", "/") for c in changed if c.strip()]

    if any(is_global(c) for c in changed):
        return "full", []

    direct = {p for p in (owner(c, projects) for c in changed) if p is not None}

    # Reverse closure: a project is affected when it references an affected one.
    referenced_by: dict[str, set[str]] = {p: set() for p in projects}
    for project in projects:
        for target in references(project):
            if target in referenced_by:
                referenced_by[target].add(project)

    affected = set(direct)
    frontier = list(direct)
    while frontier:
        for dependent in referenced_by[frontier.pop()]:
            if dependent not in affected:
                affected.add(dependent)
                frontier.append(dependent)

    if not affected:
        return "none", []

    if HEAVY_ROOT in affected or len(affected) >= FULL_WHEN_AT_LEAST:
        return "full", []

    return "subset", [p for p in projects if p in affected]


def changed_files(argv: list[str]) -> list[str]:
    if argv[1] == "--files":
        return sys.stdin.read().splitlines()

    output = subprocess.run(
        ["git", "diff", "--name-only", "--no-renames", argv[1], "HEAD"],
        cwd=ROOT,
        check=True,
        capture_output=True,
        text=True,
    ).stdout
    return output.splitlines()


def main(argv: list[str]) -> int:
    if len(argv) != 2:
        print(f"usage: {argv[0]} <base-commit> | --files", file=sys.stderr)
        return 1

    scope, projects = scope_for(changed_files(argv))
    print(scope)
    for project in projects:
        print(project)

    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
