#!/usr/bin/env bash
set -euo pipefail

repo_root="$(git rev-parse --show-toplevel)"
cd "${repo_root}"

config=".markdownlint-cli2.jsonc"

staged_md=()
while IFS= read -r path; do
    staged_md+=("${path}")
done < <(git diff --cached --name-only --diff-filter=ACM | grep -E '\.md$' || true)

if [[ ${#staged_md[@]} -eq 0 ]]; then
    exit 0
fi

if ! command -v npx >/dev/null 2>&1; then
    echo "pre-commit: npx not found; install Node.js to lint staged Markdown." >&2
    exit 1
fi

echo "markdownlint: ${#staged_md[@]} staged Markdown file(s)"

npx --yes markdownlint-cli2 --config "${config}" --fix "${staged_md[@]}"

git add -- "${staged_md[@]}"

if npx --yes markdownlint-cli2 --config "${config}" "${staged_md[@]}"; then
    echo "markdownlint: ok"
    exit 0
fi

echo "pre-commit: markdownlint reported issues that were not auto-fixed; fix them manually." >&2
exit 1
