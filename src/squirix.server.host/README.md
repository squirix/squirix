# squirix.server.tool

NuGet global tool package for the **`squirix-server`** CLI (standalone process host).

For the .NET 10 build on Linux and macOS, use ASP.NET Core Runtime 10.0.15 or a later .NET 10 patch.

```bash
dotnet tool install --global squirix.server.tool --version 0.1.0-preview.10
squirix-server run
squirix-server run --persist --data-dir data
```

These commands install the published preview.10. See the [root README](../../README.md) for the current release.

Tool packages built by the current release workflow contain both `net10.0` and `net11.0`.
Add `--framework net11.0` to the install command to select the .NET 11 build of a package that includes it.
The host requires the corresponding ASP.NET Core runtime.

Or run from the repository root with the SDK selected by [global.json](../../global.json):

```bash
dotnet run --project src/squirix.server.host/Squirix.Server.Host.csproj -- run
```

Docker images: see [containerization](../../docs/containerization.md) (`docker/Dockerfile` and `docker/Dockerfile.release`).

Run `squirix-server help` for operational commands and flags.

## Logging

The host ships `appsettings.defaults.json` next to its executable and loads it as the lowest-priority configuration.
The default level is `Information`, with `Microsoft.AspNetCore` set to `Warning` to avoid per-request framework logs.
These defaults apply to the global tool, published executable, repository runs and Docker images, regardless of the
current working directory.

The standard `appsettings.json` and `appsettings.{Environment}.json` in the working directory override the packaged
defaults at startup. Restart the standalone host to apply changes to these files. Environment variables have higher
priority. For example, set
`Logging__LogLevel__Microsoft.AspNetCore=Information` to enable detailed framework request logs, or
`Logging__LogLevel__Default=Warning` to reduce ordinary application messages. Warning, Error and Critical framework
messages remain enabled by default. `Squirix.settings.json` continues to configure Squirix itself.
