#!/usr/bin/env python3
"""Negative self-tests for verify_ndepend_regression.py."""

from __future__ import annotations

import argparse
import json
import shutil
import subprocess
import sys
from pathlib import Path


def run(cmd: list[str]) -> subprocess.CompletedProcess[str]:
    print("+", " ".join(cmd), flush=True)
    return subprocess.run(cmd, check=False, text=True, capture_output=True)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--ndepend-console", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--repo", type=Path, default=None)
    args = parser.parse_args()

    repo = args.repo.resolve() if args.repo else Path(__file__).resolve().parents[2]
    out = args.output.resolve()
    out.mkdir(parents=True, exist_ok=True)

    verify = repo / "build" / "ndepend" / "verify_ndepend_regression.py"
    # Reuse two product analyses if present; otherwise require caller to pass dirs via env later.
    # For self-test we synthesize CodeRuleResult.xml pairs (and dummy .ndar files).
    clean = out / "clean"
    dirty = out / "dirty"
    for path in (clean, dirty):
        if path.exists():
            shutil.rmtree(path)
        (path / "XmlFilesUsedToBuildReport").mkdir(parents=True)
        (path / "dummy.ndar").write_bytes(b"NDAR")

    clean_xml = """<?xml version="1.0" encoding="utf-8"?>
<RuleResult NbRules="1" NbErrors="0" NbWarns="0" NbWarnsCritical="0">
  <Group Name="Architecture" FullName="Architecture">
    <Query Status="Ok" Name="Enforcing Clean Architecture" RuleId="ND1412" FullName="Project Rules \\ Architecture \\ Enforcing Clean Architecture" NbNodeMatched="0" />
  </Group>
</RuleResult>
"""
    dirty_xml = """<?xml version="1.0" encoding="utf-8"?>
<RuleResult NbRules="2" NbErrors="0" NbWarns="1" NbWarnsCritical="0">
  <Group Name="Architecture" FullName="Architecture">
    <Query Status="Ok" Name="Enforcing Clean Architecture" RuleId="ND1412" FullName="Project Rules \\ Architecture \\ Enforcing Clean Architecture" NbNodeMatched="0" />
    <Query Status="RuleWarn" Name="Avoid namespaces dependency cycles" RuleId="ND1400" FullName="Project Rules \\ Architecture \\ Avoid namespaces dependency cycles" NbNodeMatched="2" />
  </Group>
  <Group Name="Design" FullName="Design">
    <Query Status="RuleWarn" Name="Types with too many methods" RuleId="ND1201" FullName="Project Rules \\ Design \\ Types with too many methods" NbNodeMatched="1" />
  </Group>
  <Group Name="Visibility" FullName="Visibility">
    <Query Status="RuleWarn" Name="API breaking" RuleId="ND1800" FullName="Project Rules \\ Visibility \\ Potentially dead types" NbNodeMatched="1" />
  </Group>
</RuleResult>
"""
    (clean / "XmlFilesUsedToBuildReport" / "CodeRuleResult.xml").write_text(clean_xml, encoding="utf-8")
    (dirty / "XmlFilesUsedToBuildReport" / "CodeRuleResult.xml").write_text(dirty_xml, encoding="utf-8")

    # Clean vs clean must pass.
    ok = run(
        [
            sys.executable,
            str(verify),
            "--baseline",
            str(clean),
            "--candidate",
            str(clean),
            "--output",
            str(out / "clean-vs-clean.json"),
        ]
    )
    if ok.returncode != 0:
        print(ok.stdout)
        print(ok.stderr, file=sys.stderr)
        raise RuntimeError("clean vs clean unexpectedly failed")

    # Clean vs dirty must fail.
    bad = run(
        [
            sys.executable,
            str(verify),
            "--baseline",
            str(clean),
            "--candidate",
            str(dirty),
            "--output",
            str(out / "clean-vs-dirty.json"),
        ]
    )
    if bad.returncode == 0:
        print(bad.stdout)
        raise RuntimeError("clean vs dirty unexpectedly passed")

    report = json.loads((out / "clean-vs-dirty.json").read_text(encoding="utf-8"))
    if not report.get("failures"):
        raise RuntimeError("dirty candidate produced no failures")

    evidence = {
        "cleanVsCleanExit": ok.returncode,
        "cleanVsDirtyExit": bad.returncode,
        "failures": report["failures"],
        "ndependConsole": str(args.ndepend_console),
    }
    evidence_path = out / "evidence.json"
    evidence_path.write_text(json.dumps(evidence, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(evidence, indent=2))
    print(f"Wrote {evidence_path}")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:  # noqa: BLE001
        print(exc, file=sys.stderr)
        raise SystemExit(1)
