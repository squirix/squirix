# External package smoke

This sample is **not** part of the main solution. It references the **packed** `squirix` NuGet package only (no project
references), to simulate a third-party consumer on the v0.1 client SDK surface.

After changing the client SDK, repack before `SmokeUsePackages=true`:

```powershell
dotnet pack src/squirix/Squirix.csproj -c Release -o artifacts/packages
```

## Prerequisites

- .NET 10 SDK
- A local package built at `artifacts/packages/squirix.*.nupkg` (folder is gitignored)

## Build the package

From the repository root:

```powershell
dotnet pack src/squirix/Squirix.csproj -c Release -o artifacts/packages
```

## Run

```powershell
cd samples/external-package-smoke
dotnet restore
dotnet run
```

`nuget.config` points `squirix-local` at `../../artifacts/packages`. The sample resolves `squirix` `PackageReference` to
`$(SquirixPackageVersion)` from the repository root `Directory.Build.props` (same default as
`src/squirix/Squirix.csproj`). To bump the shipped version, change `SquirixPackageVersion` once at the repo root.
