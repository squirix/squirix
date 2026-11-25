#!/usr/bin/env bash
set -euo pipefail

dotnet="$(dirname "$0")/dotnet-original"
if [[ $# == 0 || ${1##*/} != GitHubActionAnalyzer.MultiOS.dll ]]; then
  exec "$dotnet" "$@"
fi

# Keep the original analyzer report and stderr. Only omit missing obj source paths from stdout.
"$dotnet" "$@" | awk '
  /^Warning +The source (directory|file) \{/ {
    path = $0
    sub(/^Warning +The source (directory|file) \{/, "", path)
    sub(/\}.*/, "", path)
    if (index(path, ENVIRON["GITHUB_WORKSPACE"] "/") == 1 &&
        path ~ /\/obj\// &&
        $0 ~ /\} referenced by the PDB file of the assembly \{[^{}]+\} (can\047t be found|does not exist)\.[[:space:]]*$/) next
  }
  { print; fflush() }
'
