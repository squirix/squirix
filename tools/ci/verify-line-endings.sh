#!/usr/bin/env bash
set -euo pipefail

event_name="${GITHUB_EVENT_NAME:-}"

if [[ "${event_name}" != "pull_request" ]]; then
    echo "verify-line-endings: skipped (pull_request only)"
    exit 0
fi

collect_changed_paths() {
    local base_ref="${GITHUB_BASE_REF:-develop}"
    git fetch origin "${base_ref}" --depth=1
    git diff --name-only --diff-filter=ACMRT "origin/${base_ref}...HEAD"
}

path_extension() {
    local path="$1"
    local name="${path##*/}"

    if [[ "${name}" == "${path##*/}" && "${name}" != *.* ]]; then
        printf '%s\n' "${name}"
        return
    fi

    printf '%s\n' "${name##*.}" | tr '[:upper:]' '[:lower:]'
}

is_text_path() {
    local path="$1"
    local ext
    ext="$(path_extension "${path}")"

    case "${ext}" in
        cs|csproj|props|targets|sln|slnx|json|md|yml|yaml|editorconfig|xml|txt|proto|ps1|runsettings|config|toml|http|svg|cshtml|razor|sql|sh|cmd|bat|mdc|csx|dockerfile|gitignore|gitattributes|dockerignore|license|.editorconfig)
            return 0
            ;;
        *)
            return 1
            ;;
    esac
}

is_binary_path() {
    local path="$1"
    local ext
    ext="$(path_extension "${path}")"

    case "${ext}" in
        png|jpg|jpeg|gif|webp|ico|woff|woff2|zip|nupkg|snupkg|dll|exe|pdf|pdb|trx)
            return 0
            ;;
        *)
            return 1
            ;;
    esac
}

verify_lf_in_git_blob() {
    local path="$1"
    local blob_ref="HEAD:${path}"

    if ! git cat-file -e "${blob_ref}" 2>/dev/null; then
        return 0
    fi

    if git show "${blob_ref}" | grep -q $'\r'; then
        echo "error: ${path} must use LF line endings (CR found in git blob; see .editorconfig end_of_line = lf)" >&2
        return 1
    fi

    return 0
}

mapfile -t changed_paths < <(collect_changed_paths || true)

if [[ ${#changed_paths[@]} -eq 0 ]]; then
    echo "verify-line-endings: no changed paths"
    exit 0
fi

failures=0
checked=0

for path in "${changed_paths[@]}"; do
    if is_binary_path "${path}"; then
        continue
    fi

    if ! is_text_path "${path}"; then
        continue
    fi

    checked=$((checked + 1))
    if ! verify_lf_in_git_blob "${path}"; then
        failures=$((failures + 1))
    fi
done

echo "verify-line-endings: checked ${checked} changed text file(s)"

if [[ ${failures} -ne 0 ]]; then
    echo "verify-line-endings: ${failures} file(s) failed" >&2
    exit 1
fi

