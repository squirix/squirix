#!/usr/bin/env bash
set -euo pipefail

os="$(uname -s)"

if [[ "${os}" == "Darwin" && -n "${GITHUB_ENV:-}" ]]; then
    # The runner login keychain can block even when dev-certs is invoked without --trust.
    umask 077
    cert_dir="$(mktemp -d "${RUNNER_TEMP:-${TMPDIR:-/tmp}}/squirix-https.XXXXXXXX")"
    trap 'rm -f -- "${cert_dir}/localhost.key" "${cert_dir}/localhost.pem" "${cert_dir}/localhost.cnf"' EXIT
    cert_password="$(openssl rand -hex 24)"
    echo "::add-mask::${cert_password}"
    cat > "${cert_dir}/localhost.cnf" <<'EOF'
[req]
distinguished_name = subject
[subject]
[v3]
basicConstraints = critical,CA:FALSE
keyUsage = critical,digitalSignature,keyEncipherment
extendedKeyUsage = serverAuth
subjectAltName = DNS:localhost,IP:127.0.0.1,IP:::1
EOF
    openssl req -x509 -newkey rsa:2048 -nodes -days 2 -subj /CN=localhost \
        -config "${cert_dir}/localhost.cnf" -extensions v3 \
        -keyout "${cert_dir}/localhost.key" -out "${cert_dir}/localhost.pem" 2>/dev/null
    SQUIRIX_DEV_TLS_SECRET="${cert_password}" openssl pkcs12 -export \
        -in "${cert_dir}/localhost.pem" -inkey "${cert_dir}/localhost.key" \
        -out "${cert_dir}/localhost.pfx" -passout env:SQUIRIX_DEV_TLS_SECRET
    {
        echo "SQUIRIX_ALLOW_UNTRUSTED_DEV_HTTPS=1"
        echo "SQUIRIX_TLS_CERTIFICATE_PATH=${cert_dir}/localhost.pfx"
        echo "SQUIRIX_TLS_CERTIFICATE_PASSWORD=${cert_password}"
    } >> "${GITHUB_ENV}"
    exit 0
fi

# Ensure the .NET SDK HTTPS development certificate exists.
dotnet dev-certs https >/dev/null

# Windows and interactive macOS installs can show a trust prompt.
if [[ "${os}" != "Linux" ]]; then
    echo "Skipping interactive 'dotnet dev-certs https --trust' on ${os}."
    if [[ -n "${GITHUB_ENV:-}" ]]; then
        echo "SQUIRIX_ALLOW_UNTRUSTED_DEV_HTTPS=1" >> "${GITHUB_ENV}"
    fi
    export SQUIRIX_ALLOW_UNTRUSTED_DEV_HTTPS=1
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
