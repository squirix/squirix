# squirix.server.tool

NuGet global tool package for the **`squirix-server`** CLI (standalone process host).

```bash
dotnet tool install --global squirix.server.tool --version <version>
squirix-server run
squirix-server run --persist --data-dir ./data
```

Pin `<version>` from the [root README](../../README.md).

Or run from the repository:

```bash
dotnet run --project src/squirix.server.host/Squirix.Server.Host.csproj -- run
```

Docker images: see [containerization](../../docs/containerization.md) (`docker/Dockerfile` and `docker/Dockerfile.release`).

Run `squirix-server help` for operational commands and flags.
