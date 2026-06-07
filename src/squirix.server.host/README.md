# squirix.server.tool

Standalone squirix server host (`squirix-server`).

**Not published to NuGet yet** in the 0.1 preview line. Run from the repository:

```powershell
dotnet run --project src/squirix.server.host/Squirix.Server.Host.csproj -- run --dev --data-dir ./data
```

Or use Docker — see [containerization](../../docs/containerization.md).

When published:

```powershell
dotnet tool install --global squirix.server.tool --version 0.1.0-preview.3
squirix-server run --dev --data-dir ./data
```

Run `squirix-server help` for operational commands and flags.
