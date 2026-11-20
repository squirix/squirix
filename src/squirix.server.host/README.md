# squirix.server.tool

NuGet global tool package for the **`squirix-server`** CLI (standalone process host).

For the .NET 10 build on Linux and macOS, use .NET Runtime 10.0.15 or a later .NET 10 patch.

```bash
dotnet tool install --global squirix.server.tool --version 0.2.0-preview.1
squirix-server run
squirix-server run --persist --data-dir data
```

See the [root README](../../README.md) for release details.

Tool packages built by the current release workflow contain both `net10.0` and `net11.0`.
Add `--framework net11.0` to the install command to select the .NET 11 build of a package that includes it.
The host requires the corresponding .NET runtime.

Or run from the repository root with the SDK selected by [global.json](../../global.json):

```bash
dotnet run --project src/squirix.server.host/Squirix.Server.Host.csproj -- run
```

Docker images: see [containerization](../../docs/containerization.md) (`docker/Dockerfile` and `docker/Dockerfile.release`).

Run `squirix-server help` for operational commands and flags.

## Logging

The host ships `appsettings.defaults.json` next to its executable and loads it as the lowest-priority configuration.
The default level is `Information`.
These defaults apply to the global tool, published executable, repository runs and Docker images, regardless of the
current working directory.

The standard `appsettings.json` and `appsettings.{Environment}.json` in the working directory override the packaged
defaults at startup. Restart the standalone host to apply changes to these files. Environment variables have higher
priority. Set `Logging__LogLevel__Default=Warning` to reduce application logs. Use `Squirix.settings.json` to configure the node.
