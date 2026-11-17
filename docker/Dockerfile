# Build squirix-server from repository sources (matches current checkout).
FROM mcr.microsoft.com/dotnet/sdk:11.0 AS build
WORKDIR /src

COPY squirix.slnx ./
COPY Directory.Build.props ./
COPY Directory.Packages.props ./
COPY global.json ./
COPY stylecop.json ./
COPY BannedSymbols.txt ./
COPY BannedSymbols.Reflection.txt ./
COPY BannedSymbols.Time.txt ./
COPY .globalconfig ./
COPY .editorconfig ./
COPY src/squirix/Squirix.csproj src/squirix/
COPY src/squirix.server/Squirix.Server.csproj src/squirix.server/
COPY src/squirix.server.host/Squirix.Server.Host.csproj src/squirix.server.host/

RUN dotnet restore src/squirix.server.host/Squirix.Server.Host.csproj -v minimal

COPY src/squirix/ src/squirix/
COPY src/shared/ src/shared/
COPY src/squirix.server/ src/squirix.server/
COPY src/squirix.server.host/ src/squirix.server.host/
COPY src/squirix.attributes/ src/squirix.attributes/

# Analyzers only report diagnostics, and the CI analyzed build already runs them over these projects, so the image build skips them.
RUN dotnet publish src/squirix.server.host/Squirix.Server.Host.csproj \
    -c Release \
    -o /app/publish \
    -p:PublishReadyToRun=false \
    -p:PublishSingleFile=false \
    -p:RunAnalyzers=false \
    -p:EnforceCodeStyleInBuild=false

# Development HTTPS PFX for Kestrel. SANs cover Docker Compose DNS names and localhost host access.
FROM mcr.microsoft.com/dotnet/sdk:11.0 AS https-cert
RUN mkdir -p /https \
    && openssl req -x509 -nodes -days 825 -newkey rsa:2048 \
        -keyout /https/aspnetapp.key \
        -out /https/aspnetapp.crt \
        -subj "/CN=localhost" \
        -addext "subjectAltName=DNS:localhost,DNS:squirix-node-a,DNS:squirix-node-b,DNS:squirix-node-c,IP:127.0.0.1" \
    && openssl pkcs12 -export -out /https/aspnetapp.pfx \
        -inkey /https/aspnetapp.key -in /https/aspnetapp.crt \
        -passout pass:

FROM mcr.microsoft.com/dotnet/sdk:11.0 AS cluster-mtls-certs
COPY docker/create-dev-mtls.sh /tmp/create-dev-mtls.sh
RUN sh /tmp/create-dev-mtls.sh

FROM mcr.microsoft.com/dotnet/aspnet:10.0.15 AS final

# Selects which node certificate this image carries (A, B, or C). Each service image receives only
# its assigned node PFX plus the cluster CA certificate; the CA key and other node identities stay
# in the build stage and never reach runtime images.
ARG NODE_LETTER=A
WORKDIR /app

VOLUME ["/data"]
RUN mkdir -p /data /https /mtls && chown "$APP_UID:$APP_UID" /data /https /mtls

COPY --from=build --chown=$APP_UID:$APP_UID /app/publish .
COPY --from=https-cert --chown=$APP_UID:$APP_UID /https/aspnetapp.pfx /https/aspnetapp.pfx
COPY --from=cluster-mtls-certs --chown=$APP_UID:$APP_UID /mtls/cluster-ca.crt /mtls/cluster-ca.crt
COPY --from=cluster-mtls-certs --chown=$APP_UID:$APP_UID /mtls/node-${NODE_LETTER}.pfx /mtls/node-${NODE_LETTER}.pfx

ENV ASPNETCORE_Kestrel__Certificates__Default__Path=/https/aspnetapp.pfx

EXPOSE 5000

USER $APP_UID

ENTRYPOINT ["dotnet", "Squirix.Server.Host.dll"]
