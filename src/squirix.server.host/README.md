# squirix.server.tool

Standalone squirix server host (`squirix-server`).

```powershell
dotnet tool install --global squirix.server.tool --version 0.1.0-preview.4
squirix-server run
squirix-server run --persist --data-dir ./data
```

Or run from the repository:

```powershell
dotnet run --project src/squirix.server.host/Squirix.Server.Host.csproj -- run
```

Docker images: see [containerization](../../docs/containerization.md) (`docker/Dockerfile` and `docker/Dockerfile.release`).

Run `squirix-server help` for operational commands and flags.
