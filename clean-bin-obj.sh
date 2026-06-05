#!/usr/bin/env bash
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$root"

removed=0

while IFS= read -r -d '' dir; do
  rm -rf "$dir"
  echo "Removed $dir"
  removed=1
done < <(find . -name .git -prune -o -type d \( -name bin -o -name obj \) -print0)

if [[ "$removed" -eq 0 ]]; then
  echo "No bin or obj folders found under $root"
else
  echo "Cleaned bin and obj folders under $root"
fi
