#!/usr/bin/env python3
"""Request draft release notes from the notes repository and download them.

Generation lives outside this repository: this script only dispatches the notes
workflow, polls the matching run, and downloads the resulting artifact. Every
expected failure degrades to a static template body so a release never blocks
on notes.

Usage:
    request_release_notes.py --notes-repo OWNER/REPO --project NAME
        --source-repo OWNER/REPO --base-branch BRANCH --new-tag vX.Y.Z
        --prev-tag vA.B.C --version X.Y.Z --workflow FILE --client-run ID
        --output FILE [--timeout-secs N]

Requires: gh, python3 (stdlib only). Token via RELEASE_NOTES_TOKEN
(Actions read/write on the notes repo).
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
import sys
import tempfile
import time
from datetime import datetime, timezone
from pathlib import Path

PREVIEW_LINE = "Still an early preview — not production-ready."
POLL_INTERVAL_SECONDS = 20


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """Parse CLI arguments."""
    parser = argparse.ArgumentParser(description="Request draft release notes.")
    parser.add_argument("--notes-repo", required=True, help="OWNER/REPO hosting the notes workflow")
    parser.add_argument("--project", required=True, help="Project directory receiving the notes file")
    parser.add_argument("--source-repo", required=True, help="OWNER/REPO the release is cut from")
    parser.add_argument("--base-branch", required=True, help="Integration branch PRs merge into")
    parser.add_argument("--new-tag", required=True, help="Tag being released, for example v0.1.0")
    parser.add_argument("--prev-tag", required=True, help="Previous tag starting the range")
    parser.add_argument("--version", required=True, help="Package version without the leading v")
    parser.add_argument("--workflow", required=True, help="Notes workflow file name")
    parser.add_argument("--client-run", required=True, help="Caller run id used to match the run")
    parser.add_argument("--output", required=True, help="File receiving the draft notes")
    parser.add_argument("--timeout-secs", type=int, default=720, help="Polling deadline in seconds")
    return parser.parse_args(argv)


def fallback_body(args: argparse.Namespace) -> str:
    """Static template body used when automatic notes are unavailable."""
    lines = ["## Summary", "", "TODO: describe what shipped and why in 1-3 sentences.", ""]
    if "preview" in args.version:
        lines += [PREVIEW_LINE, ""]
    lines += [
        "## Changes",
        "",
        "- Change: TODO: automatic notes unavailable; curate from the changelog link below",
        "",
        "## Install",
        "",
    ]
    if args.project == "squirix":
        lines += [
            f"dotnet add package squirix --version {args.version}",
            f"dotnet add package squirix.server --version {args.version}",
            f"dotnet tool install --global squirix.server.tool --version {args.version}",
            "",
            "## Links",
            "",
            f"- [Release notes](https://github.com/{args.source_repo}/blob/main/docs/release-notes/v0.1.0.md)",
            "- [NuGet profile](https://www.nuget.org/profiles/squirix)",
        ]
    else:
        lines += [
            f"- TODO: add install instructions for {args.project} {args.version}",
            "",
            "## Links",
            "",
            f"- TODO: add release notes link for {args.project} {args.new_tag}",
        ]
    lines += [
        f"- Full changelog: [{args.prev_tag}...{args.new_tag}]"
        f"(https://github.com/{args.source_repo}/compare/{args.prev_tag}...{args.new_tag})",
        "",
    ]
    return "\n".join(lines)


def write_fallback(args: argparse.Namespace, reason: str) -> int:
    """Write the static body with a CI warning; always succeeds."""
    print(f"::warning::{reason}; using static fallback notes.", file=sys.stderr)
    output = Path(args.output)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(fallback_body(args), encoding="utf-8")
    print(f"Static fallback notes written to {output}.")
    return 0


def run_gh(arguments: list[str], env: dict[str, str]) -> subprocess.CompletedProcess[str]:
    """Run gh with the notes token; failures propagate to the caller."""
    return subprocess.run(["gh", *arguments], capture_output=True, text=True, check=True, env=env)


def find_run(args: argparse.Namespace, env: dict[str, str], start: str) -> dict | None:
    """Return the newest matching dispatch run, or None when it has not appeared yet."""
    try:
        completed = run_gh(
            [
                "run", "list", "--repo", args.notes_repo, "--workflow", args.workflow,
                "--limit", "20", "--json",
                "databaseId,event,displayTitle,status,conclusion,createdAt",
            ],
            env,
        )
        runs = json.loads(completed.stdout or "[]")
    except Exception:
        return None
    matches = [
        run
        for run in runs
        if run.get("event") == "workflow_dispatch"
        and args.client_run in (run.get("displayTitle") or "")
        and (run.get("createdAt") or "") >= start
    ]
    if not matches:
        return None
    return max(matches, key=lambda run: run.get("createdAt") or "")


def download_notes(args: argparse.Namespace, env: dict[str, str], run_id: int) -> bool:
    """Download the notes artifact of a run into the output file."""
    tmpdir = tempfile.mkdtemp(prefix="release-notes-")
    try:
        run_gh(
            [
                "run", "download", "--repo", args.notes_repo, str(run_id),
                "--name", f"release-notes-{args.version}", "--dir", tmpdir,
            ],
            env,
        )
        candidate = Path(tmpdir) / f"release-notes-{args.version}.md"
        if candidate.is_file() and candidate.stat().st_size > 0:
            output = Path(args.output)
            output.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(candidate, output)
            return True
        return False
    except Exception:
        return False
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)


def main(argv: list[str] | None = None) -> int:
    """Dispatch, poll, and download; returns the process exit code."""
    args = parse_args(argv)
    token = os.environ.get("RELEASE_NOTES_TOKEN", "")
    if not token:
        return write_fallback(args, "RELEASE_NOTES_TOKEN is not configured")
    env = {**os.environ, "GH_TOKEN": token}
    start = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    try:
        run_gh(
            [
                "workflow", "run", "--repo", args.notes_repo, "--ref", "main", args.workflow,
                "-f", f"project={args.project}",
                "-f", f"source_repo={args.source_repo}",
                "-f", f"base_branch={args.base_branch}",
                "-f", f"new_tag={args.new_tag}",
                "-f", f"prev_tag={args.prev_tag}",
                "-f", f"version={args.version}",
                "-f", f"client_run={args.client_run}",
            ],
            env,
        )
    except Exception:
        return write_fallback(args, "Could not dispatch the notes workflow")

    deadline = time.monotonic() + args.timeout_secs
    run_id: int | None = None
    while time.monotonic() < deadline:
        run = find_run(args, env, start)
        if run is None:
            time.sleep(POLL_INTERVAL_SECONDS)
            continue
        if run.get("status") != "completed":
            time.sleep(POLL_INTERVAL_SECONDS)
            continue
        if run.get("conclusion") != "success":
            return write_fallback(
                args, f"Notes run {run.get('databaseId')} ended with {run.get('conclusion')}"
            )
        run_id = run.get("databaseId")
        break

    if run_id is None:
        return write_fallback(args, "Timed out waiting for the notes run")
    if download_notes(args, env, run_id):
        print(f"Draft notes from run {run_id} written to {args.output}.")
        return 0
    return write_fallback(args, f"Could not download notes artifact from run {run_id}")


if __name__ == "__main__":
    raise SystemExit(main())
