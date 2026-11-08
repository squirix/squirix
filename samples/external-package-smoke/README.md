# External package smoke

This standalone sample starts an embedded Squirix server, connects through the public client SDK, and checks named-cache
isolation and expiration. It is not part of the main solution.

By default, the project references the client and server source projects. Set `SmokeUsePackages=true` to exercise the
packed `squirix` and `squirix.server` NuGet packages instead.

## Prerequisites

- The .NET SDK selected by [global.json](../../global.json), including when building this sample from the repository.
- A trusted ASP.NET Core development HTTPS certificate: `dotnet dev-certs https --trust`.
- A free local port 51907, as configured in [Squirix.settings.json](Squirix.settings.json).

## Build the packages

From the repository root, pack both runtime roles after changing either one:

```bash
dotnet pack src/squirix/Squirix.csproj -c Release -o artifacts/packages
dotnet pack src/squirix.server/Squirix.Server.csproj -c Release -o artifacts/packages
```

## Run against the packages

Change to the sample directory so its settings file is found, then keep the package-mode property on both commands:

```bash
cd samples/external-package-smoke
dotnet restore -p:SmokeUsePackages=true
dotnet run -c Release --no-restore -p:SmokeUsePackages=true
```

The local [nuget.config](nuget.config) maps both Squirix packages to `../../artifacts/packages`. Their versions come from
`SquirixPackageVersion` in the repository root [Directory.Build.props](../../Directory.Build.props), through
[Directory.Packages.props](../../Directory.Packages.props). A successful run exits with code 0; a failed isolation or
expiration check throws an exception.

If NuGet already cached packages with the same version, repacking that version does not replace the cached copies.
Use a fresh isolated NuGet package directory when verifying rebuilt packages of an unchanged version; otherwise a
successful restore can still exercise the older cached package.
