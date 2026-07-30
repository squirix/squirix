#!/usr/bin/env python3
"""Sync squirix.ndrules into squirix.ndproj (CDATA-safe via PowerShell XmlDocument)."""

from __future__ import annotations

import argparse
import shutil
import subprocess
import sys
from pathlib import Path


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo", type=Path, default=None)
    args = parser.parse_args()
    script_dir = Path(__file__).resolve().parent
    repo = args.repo.resolve() if args.repo else script_dir.parents[1]
    ps1 = script_dir / "sync-ndproj.ps1"
    if not ps1.is_file():
        print(f"Missing {ps1}", file=sys.stderr)
        return 1

    shell = shutil.which("pwsh") or shutil.which("powershell")
    if shell is None:
        print("pwsh/powershell is required for CDATA-safe ndproj sync.", file=sys.stderr)
        return 1

    # sync-ndproj.ps1 resolves repo from its own location; run from repo root for clarity.
    completed = subprocess.run(
        [shell, "-NoProfile", "-File", str(ps1)],
        cwd=str(repo),
        check=False,
    )
    return completed.returncode


if __name__ == "__main__":
    raise SystemExit(main())
