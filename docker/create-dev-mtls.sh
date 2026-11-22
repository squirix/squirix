#!/bin/sh
set -eu

mkdir -p /mtls
openssl genrsa -out /mtls/cluster-ca.key 4096
openssl req -x509 -new -nodes -key /mtls/cluster-ca.key -sha256 -days 825 \
    -out /mtls/cluster-ca.crt -subj "/CN=Squirix Docker Cluster CA"
for NODE in A B C; do
    openssl genrsa -out /mtls/node-${NODE}.key 2048
    openssl req -new -key /mtls/node-${NODE}.key -out /mtls/node-${NODE}.csr -subj "/CN=${NODE}"
    openssl x509 -req -in /mtls/node-${NODE}.csr -CA /mtls/cluster-ca.crt -CAkey /mtls/cluster-ca.key \
        -CAcreateserial -out /mtls/node-${NODE}.crt -days 825 -sha256
    openssl pkcs12 -export -out /mtls/node-${NODE}.pfx \
        -inkey /mtls/node-${NODE}.key -in /mtls/node-${NODE}.crt \
        -certfile /mtls/cluster-ca.crt -passout pass:dev-docker-mtls
done
rm -f /mtls/*.csr /mtls/*.srl
