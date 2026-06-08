#!/usr/bin/env bash
set -euo pipefail

# On Linux, --trust may exit 4 until SSL_CERT_DIR includes the ASP.NET dev-cert store.
dotnet dev-certs https --trust || true

if [[ "$(uname -s)" == "Linux" ]]; then
    aspnet_trust="${HOME}/.aspnet/dev-certs/trust"
    if [[ -z "${SSL_CERT_DIR:-}" ]]; then
        export SSL_CERT_DIR="${aspnet_trust}:/usr/lib/ssl/certs"
    else
        export SSL_CERT_DIR="${aspnet_trust}:${SSL_CERT_DIR}"
    fi

    if [[ -n "${GITHUB_ENV:-}" ]]; then
        echo "SSL_CERT_DIR=${SSL_CERT_DIR}" >> "${GITHUB_ENV}"
    fi
fi

dotnet dev-certs https --check --trust
