# Build squirix-server from repository sources (matches current checkout).
FROM mcr.microsoft.com/dotnet/sdk:10.0 AS build
WORKDIR /src

COPY squirix.slnx ./
COPY Directory.Build.props ./
COPY Directory.Packages.props ./
COPY global.json ./
COPY stylecop.json ./
COPY .editorconfig ./
COPY src/squirix/Squirix.csproj src/squirix/
COPY src/squirix.server/Squirix.Server.csproj src/squirix.server/
COPY src/squirix.server.host/Squirix.Server.Host.csproj src/squirix.server.host/

RUN dotnet restore src/squirix.server.host/Squirix.Server.Host.csproj -v minimal

COPY src/squirix/ src/squirix/
COPY src/shared/ src/shared/
COPY src/squirix.server/ src/squirix.server/
COPY src/squirix.server.host/ src/squirix.server.host/

RUN dotnet publish src/squirix.server.host/Squirix.Server.Host.csproj \
    -c Release \
    -o /app/publish \
    -p:PublishReadyToRun=false \
    -p:PublishSingleFile=false

# Development HTTPS PFX for Kestrel. SANs cover Docker Compose DNS names and localhost host access.
FROM mcr.microsoft.com/dotnet/sdk:10.0 AS https-cert
RUN mkdir -p /https \
    && openssl req -x509 -nodes -days 825 -newkey rsa:2048 \
        -keyout /https/aspnetapp.key \
        -out /https/aspnetapp.crt \
        -subj "/CN=localhost" \
        -addext "subjectAltName=DNS:localhost,DNS:squirix-node-a,DNS:squirix-node-b,IP:127.0.0.1" \
    && openssl pkcs12 -export -out /https/aspnetapp.pfx \
        -inkey /https/aspnetapp.key -in /https/aspnetapp.crt \
        -passout pass:

FROM mcr.microsoft.com/dotnet/sdk:10.0 AS cluster-mtls-certs
RUN mkdir -p /mtls \
    && openssl genrsa -out /mtls/cluster-ca.key 4096 \
    && openssl req -x509 -new -nodes -key /mtls/cluster-ca.key -sha256 -days 825 \
        -out /mtls/cluster-ca.crt -subj "/CN=Squirix Docker Cluster CA" \
    && for NODE in A B; do \
        openssl genrsa -out /mtls/node-${NODE}.key 2048 \
        && openssl req -new -key /mtls/node-${NODE}.key -out /mtls/node-${NODE}.csr -subj "/CN=${NODE}" \
        && openssl x509 -req -in /mtls/node-${NODE}.csr -CA /mtls/cluster-ca.crt -CAkey /mtls/cluster-ca.key \
            -CAcreateserial -out /mtls/node-${NODE}.crt -days 825 -sha256 \
        && openssl pkcs12 -export -out /mtls/node-${NODE}.pfx \
            -inkey /mtls/node-${NODE}.key -in /mtls/node-${NODE}.crt \
            -certfile /mtls/cluster-ca.crt -passout pass:dev-docker-mtls; \
       done \
    && rm -f /mtls/*.csr /mtls/*.srl

FROM mcr.microsoft.com/dotnet/aspnet:10.0 AS final
WORKDIR /app

VOLUME ["/data"]
RUN mkdir -p /data /https /mtls && chown "$APP_UID:$APP_UID" /data /https /mtls

COPY --from=build --chown=$APP_UID:$APP_UID /app/publish .
COPY --from=https-cert --chown=$APP_UID:$APP_UID /https/aspnetapp.pfx /https/aspnetapp.pfx
COPY --from=cluster-mtls-certs --chown=$APP_UID:$APP_UID /mtls /mtls

ENV ASPNETCORE_Kestrel__Certificates__Default__Path=/https/aspnetapp.pfx

EXPOSE 5000

USER $APP_UID

ENTRYPOINT ["dotnet", "Squirix.Server.Host.dll"]
