# The Compose additional context provides the exact node image used by this local cluster.
FROM server AS certificates

FROM mcr.microsoft.com/dotnet/sdk:10.0 AS build
WORKDIR /sample
COPY samples/docker-client/DockerClient.csproj ./
RUN dotnet restore DockerClient.csproj
COPY samples/docker-client/Program.cs ./
RUN dotnet publish DockerClient.csproj -c Release --no-restore -o /out
COPY --from=certificates /https/aspnetapp.pfx /tmp/server.pfx
RUN openssl pkcs12 -in /tmp/server.pfx -passin pass: -nokeys -out /tmp/server.crt

FROM mcr.microsoft.com/dotnet/runtime:11.0
COPY --from=build /tmp/server.crt /usr/local/share/ca-certificates/squirix-local.crt
RUN update-ca-certificates
WORKDIR /app
COPY --from=build /out ./
USER $APP_UID
ENTRYPOINT ["dotnet", "DockerClient.dll"]
