#!/usr/bin/env bash
set -euo pipefail

# On Linux, --trust may exit 4 until SSL_CERT_DIR includes the ASP.NET dev-cert store.
dotnet dev-certs https --trust || true

if [[ "$(uname -s)" == "Linux" ]]; then
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
fi

dotnet dev-certs https --check --trust
