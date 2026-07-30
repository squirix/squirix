#!/usr/bin/env python3
"""Compare two NDepend analyses (.ndar + CodeRuleResult.xml) for regression."""

from __future__ import annotations

import argparse
import json
import sys
import xml.etree.ElementTree as ET
from pathlib import Path

DESIGN_ARCH_GROUPS = {"Design", "Architecture"}
EXPLICIT_RULES = {
    "ND1800",
    "ND1807",
    "ND2008",
    "ND2012",
    "ND2013",
    "ND2016",
    "ND2206",
}
HIGH_SEVERITIES = {"Blocker", "Critical", "High"}


def find_code_rule_result(analysis_dir: Path) -> Path:
    candidates = list(analysis_dir.rglob("CodeRuleResult.xml"))
    if not candidates:
        raise FileNotFoundError(f"CodeRuleResult.xml not found under {analysis_dir}")
    return max(candidates, key=lambda p: p.stat().st_mtime)


def find_ndar(analysis_dir: Path) -> Path:
    candidates = list(analysis_dir.rglob("*.ndar"))
    if not candidates:
        raise FileNotFoundError(f".ndar not found under {analysis_dir}")
    return max(candidates, key=lambda p: p.stat().st_mtime)


def load_issues(code_rule_result: Path) -> list[dict]:
    root = ET.parse(code_rule_result).getroot()
    issues: list[dict] = []
    for query in root.iter("Query"):
        matched = int(query.get("NbNodeMatched") or "0")
        if matched <= 0:
            continue
        status = query.get("Status") or ""
        if status in {"Error", "RuleError"}:
            continue
        group = ""
        parent = query
        # climb to nearest Group
        # ElementTree has no parent; reconstruct via path scan
        issues.append(
            {
                "ruleId": query.get("RuleId") or "",
                "name": query.get("Name") or "",
                "status": status,
                "matched": matched,
                "fullName": query.get("FullName") or "",
            }
        )
    # Attach group names from FullName when present: "Project Rules \ Architecture \ ..."
    for issue in issues:
        parts = [p.strip() for p in issue["fullName"].split("\\")]
        if len(parts) >= 2:
            issue["group"] = parts[1] if parts[0].endswith("Rules") or "Rules" in parts[0] else parts[0]
        else:
            issue["group"] = ""
    return issues


def issue_key(issue: dict) -> str:
    return f"{issue.get('ruleId')}|{issue.get('name')}"


def compare(baseline_dir: Path, candidate_dir: Path) -> dict:
    baseline_ndar = find_ndar(baseline_dir)
    candidate_ndar = find_ndar(candidate_dir)
    baseline_xml = find_code_rule_result(baseline_dir)
    candidate_xml = find_code_rule_result(candidate_dir)

    baseline_issues = {issue_key(i): i for i in load_issues(baseline_xml)}
    candidate_issues = {issue_key(i): i for i in load_issues(candidate_xml)}

    new_keys = sorted(set(candidate_issues) - set(baseline_issues))
    new_issues = [candidate_issues[k] for k in new_keys]
    worsened_keys = sorted(
        k
        for k in set(candidate_issues) & set(baseline_issues)
        if int(candidate_issues[k].get("matched") or 0) > int(baseline_issues[k].get("matched") or 0)
    )
    worsened_issues = [candidate_issues[k] for k in worsened_keys]

    failures: list[str] = []
    worsened_key_set = set(worsened_keys)
    for key in new_keys + worsened_keys:
        issue = candidate_issues[key]
        group = issue.get("group") or ""
        rule_id = issue.get("ruleId") or ""
        status = issue.get("status") or ""
        full = issue.get("fullName") or ""
        name = issue.get("name") or ""
        kind = "worsened" if key in worsened_key_set else "new"

        if any(g in full or g == group for g in DESIGN_ARCH_GROUPS):
            failures.append(f"{kind} Design/Architecture issue: {rule_id} {name} ({status})")
        if rule_id in EXPLICIT_RULES:
            failures.append(f"{kind} explicit rule issue: {rule_id} {name}")
        if "RuleWarnCritical" in status or "Critical" in status:
            failures.append(f"{kind} critical/high-status issue: {rule_id} {name} ({status})")
        # Cycles / mutual dependencies often named explicitly
        lowered = f"{name} {full}".lower()
        if "cycle" in lowered or "mutual" in lowered:
            failures.append(f"{kind} cycle/mutual dependency issue: {rule_id} {name}")

    return {
        "baselineNdar": str(baseline_ndar),
        "candidateNdar": str(candidate_ndar),
        "baselineXml": str(baseline_xml),
        "candidateXml": str(candidate_xml),
        "baselineIssueCount": len(baseline_issues),
        "candidateIssueCount": len(candidate_issues),
        "newIssues": new_issues,
        "worsenedIssues": worsened_issues,
        "failures": failures,
        "ok": len(failures) == 0,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--baseline", required=True, type=Path)
    parser.add_argument("--candidate", required=True, type=Path)
    parser.add_argument("--output", type=Path, default=None)
    args = parser.parse_args()

    report = compare(args.baseline.resolve(), args.candidate.resolve())
    text = json.dumps(report, indent=2)
    print(text)
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(text + "\n", encoding="utf-8")
    return 0 if report["ok"] else 2


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:  # noqa: BLE001
        print(exc, file=sys.stderr)
        raise SystemExit(1)
