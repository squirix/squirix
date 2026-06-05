# Build stage
FROM mcr.microsoft.com/dotnet/sdk:10.0 AS build
WORKDIR /src

# copy csproj and restore as distinct layers
COPY squirix.slnx ./
COPY Directory.Build.props ./
COPY Directory.Packages.props ./
COPY global.json ./
COPY src/squirix/Squirix.csproj src/squirix/
COPY src/squirix.server/Squirix.Server.csproj src/squirix.server/
COPY src/squirix.server.host/Squirix.Server.Host.csproj src/squirix.server.host/

RUN dotnet restore src/squirix.server.host/Squirix.Server.Host.csproj -v minimal

# Copy only build inputs required by the container host.
COPY src/squirix/ src/squirix/
COPY src/shared/ src/shared/
COPY src/squirix.server/ src/squirix.server/
COPY src/squirix.server.host/ src/squirix.server.host/

# publish the container host app which starts the squirix node
RUN dotnet publish src/squirix.server.host/Squirix.Server.Host.csproj \
    -c Release \
    -o /app/publish \
    -p:PublishReadyToRun=false \
    -p:PublishSingleFile=false

# Runtime stage
FROM mcr.microsoft.com/dotnet/aspnet:10.0 AS final
WORKDIR /app

# App data directory (can be mounted)
VOLUME ["/data"]
RUN mkdir -p /data && chown "$APP_UID:$APP_UID" /data

# Copy published output
COPY --from=build --chown=$APP_UID:$APP_UID /app/publish .

# Ports: default HTTP2 5000 (or as configured in settings)
EXPOSE 5000

USER $APP_UID

# Default working dir will contain Squirix.settings.json mounted by docker-compose
ENTRYPOINT ["dotnet", "Squirix.Server.Host.dll"]
