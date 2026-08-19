#!/usr/bin/env bash
set -euo pipefail

# Ensure the ASP.NET Core HTTPS development certificate exists.
dotnet dev-certs https >/dev/null

os="$(uname -s)"

# Windows and macOS show a GUI / keychain password prompt for --trust and hang in CI.
# Linux can trust without a GUI; OpenSSL then needs SSL_CERT_DIR for the aspnet trust store.
if [[ "${os}" != "Linux" ]]; then
    echo "Skipping interactive 'dotnet dev-certs https --trust' on ${os} (CI has no UI for trust prompts)."
    if [[ -n "${GITHUB_ENV:-}" ]]; then
        echo "SQUIRIX_ALLOW_UNTRUSTED_DEV_HTTPS=1" >> "${GITHUB_ENV}"
    fi
    export SQUIRIX_ALLOW_UNTRUSTED_DEV_HTTPS=1
    # Cert exists; OS trust is intentionally not required when SQUIRIX_ALLOW_UNTRUSTED_DEV_HTTPS=1.
    exit 0
fi

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
