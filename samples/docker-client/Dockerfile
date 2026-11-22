# The Compose additional context provides the exact node image used by this local cluster.
FROM server AS certificates

FROM mcr.microsoft.com/dotnet/sdk:11.0 AS build
WORKDIR /src
COPY Directory.Build.props Directory.Packages.props global.json ./
COPY samples/docker-client/ samples/docker-client/
COPY src/squirix/ src/squirix/
COPY src/shared/ src/shared/
COPY src/squirix.attributes/ src/squirix.attributes/
RUN dotnet publish samples/docker-client/DockerClient.csproj -c Release -o /out -p:RunAnalyzers=false -p:EnforceCodeStyleInBuild=false
COPY --from=certificates /https/squirix.pfx /tmp/server.pfx
RUN openssl pkcs12 -in /tmp/server.pfx -passin pass: -nokeys -out /tmp/server.crt

FROM mcr.microsoft.com/dotnet/runtime:10.0.15
COPY --from=build /tmp/server.crt /usr/local/share/ca-certificates/squirix-local.crt
RUN update-ca-certificates
WORKDIR /app
COPY --from=build /out ./
USER $APP_UID
ENTRYPOINT ["dotnet", "DockerClient.dll"]
