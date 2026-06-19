#!/usr/bin/env bash
set -euo pipefail

repo_root="$(git rev-parse --show-toplevel)"
cd "${repo_root}"

git config core.hooksPath .githooks

echo "Git hooks path set to .githooks for ${repo_root}"
echo "pre-commit: auto-fix staged Markdown with markdownlint-cli2 (same config as CI)"
