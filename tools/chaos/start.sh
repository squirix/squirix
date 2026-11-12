#!/bin/sh
set -eu
cp /certs/ca.crt /usr/local/share/ca-certificates/squirix-chaos.crt
update-ca-certificates >/dev/null
exec "$@"
