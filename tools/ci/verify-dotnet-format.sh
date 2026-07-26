#!/usr/bin/env bash
set -euo pipefail

SLN="squirix.slnx"

GLOBAL_PATHS=(
    .editorconfig
    stylecop.json
    Directory.Build.props
    Directory.Packages.props
    global.json
    squirix.slnx
)

SOLUTION_SOURCE_PREFIXES=(
    src/
    tests/
    benchmarks/
    samples/
)

run_full() {
    echo "dotnet format: full solution"
    dotnet format "${SLN}" --verify-no-changes --no-restore
}

is_solution_source() {
    local path="$1"
    local prefix
    for prefix in "${SOLUTION_SOURCE_PREFIXES[@]}"; do
        if [[ "${path}" == "${prefix}"* ]]; then
            return 0
        fi
    done

    return 1
}

requires_full_format() {
    local path="$1"

    for global_path in "${GLOBAL_PATHS[@]}"; do
        if [[ "${path}" == "${global_path}" ]]; then
            return 0
        fi
    done

    if [[ "${path}" == *.csproj ]] || [[ "${path}" == */packages.lock.json ]]; then
        return 0
    fi

    return 1
}

collect_changed_paths() {
    local event_name="${GITHUB_EVENT_NAME:-}"

    if [[ "${event_name}" == "pull_request" ]]; then
        local base_ref="${GITHUB_BASE_REF:-develop}"
        git fetch origin "${base_ref}" --depth=1
        git diff --name-only --diff-filter=ACMRT "origin/${base_ref}...HEAD"
        return
    fi

    if [[ "${event_name}" == "push"
        && -n "${GITHUB_EVENT_BEFORE:-}"
        && "${GITHUB_EVENT_BEFORE}" != "0000000000000000000000000000000000000000" ]]; then
        git diff --name-only --diff-filter=ACMRT "${GITHUB_EVENT_BEFORE}" "${GITHUB_SHA}"
        return
    fi

    return 1
}

event_name="${GITHUB_EVENT_NAME:-}"

if [[ "${event_name}" != "pull_request" ]]; then
    run_full
    exit 0
fi

mapfile -t changed_paths < <(collect_changed_paths || true)

if [[ ${#changed_paths[@]} -eq 0 ]]; then
    echo "dotnet format: no changed paths detected; running full solution"
    run_full
    exit 0
fi

for path in "${changed_paths[@]}"; do
    if requires_full_format "${path}"; then
        echo "dotnet format: ${path} affects global conventions; running full solution"
        run_full
        exit 0
    fi
done

mapfile -t cs_files < <(
    for path in "${changed_paths[@]}"; do
        if [[ "${path}" == *.cs ]] && is_solution_source "${path}"; then
            printf '%s\n' "${path}"
        fi
    done | sort -u
)

if [[ ${#cs_files[@]} -eq 0 ]]; then
    echo "dotnet format: no changed solution C# sources; skipping"
    exit 0
fi

includes=()
for path in "${cs_files[@]}"; do
    includes+=(--include "${path}")
done

echo "dotnet format: ${#cs_files[@]} changed C# file(s)"
dotnet format "${SLN}" --verify-no-changes --no-restore "${includes[@]}"
