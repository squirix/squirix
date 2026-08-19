#!/usr/bin/env bash
set -euo pipefail

# Ensure the ASP.NET Core HTTPS development certificate exists.
dotnet dev-certs https >/dev/null

os="$(uname -s)"

case "${os}" in
    Linux)
        # On Linux, --trust may exit 4 until SSL_CERT_DIR includes the ASP.NET dev-cert store.
        dotnet dev-certs https --trust || true

        aspnet_trust="${HOME}/.aspnet/dev-certs/trust"
        system_cert_dir=""
        for cert_dir in /etc/ssl/certs /etc/pki/tls/certs /usr/lib/ssl/certs; do
            if [[ -d "${cert_dir}" ]]; then
                system_cert_dir="${cert_dir}"
                break
            fi
        done
        if [[ -z "${SSL_CERT_DIR:-}" ]]; then
            if [[ -n "${system_cert_dir}" ]]; then
                export SSL_CERT_DIR="${aspnet_trust}:${system_cert_dir}"
            else
                export SSL_CERT_DIR="${aspnet_trust}"
            fi
        else
            export SSL_CERT_DIR="${aspnet_trust}:${SSL_CERT_DIR}"
        fi

        if [[ -n "${GITHUB_ENV:-}" ]]; then
            echo "SSL_CERT_DIR=${SSL_CERT_DIR}" >> "${GITHUB_ENV}"
        fi

        dotnet dev-certs https --check --trust
        ;;
    MINGW*|MSYS*|CYGWIN*)
        # Windows: 'dotnet dev-certs https --trust' installs into the CurrentUser root
        # store headlessly (no GUI prompt), so trust it and drop the client-side bypass.
        dotnet dev-certs https --trust
        ;;
    Darwin)
        # macOS shows a keychain password prompt for --trust that hangs in CI; the cert is
        # present but untrusted, so tests opt in to the untrusted dev cert path.
        echo "Skipping interactive 'dotnet dev-certs https --trust' on macOS (keychain prompt hangs in CI)."
        if [[ -n "${GITHUB_ENV:-}" ]]; then
            echo "SQUIRIX_ALLOW_UNTRUSTED_DEV_HTTPS=1" >> "${GITHUB_ENV}"
        fi
        export SQUIRIX_ALLOW_UNTRUSTED_DEV_HTTPS=1
        exit 0
        ;;
    *)
        # Unknown OS: fall back to the untrusted-dev-cert path rather than trusting blindly.
        echo "Skipping interactive 'dotnet dev-certs https --trust' on ${os} (CI has no UI for trust prompts)."
        if [[ -n "${GITHUB_ENV:-}" ]]; then
            echo "SQUIRIX_ALLOW_UNTRUSTED_DEV_HTTPS=1" >> "${GITHUB_ENV}"
        fi
        export SQUIRIX_ALLOW_UNTRUSTED_DEV_HTTPS=1
        exit 0
        ;;
esac
