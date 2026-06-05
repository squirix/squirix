# Naming: squirix product vs `Squirix` .NET surface

This repository uses **lowercase `squirix`** for the **product, repository, and ecosystem** identity in prose, URLs, and
repo-oriented paths. It uses **PascalCase `Squirix`** for **.NET-specific** identifiers that follow official .NET
conventions.

## Lowercase `squirix`

Use for:

- Running text about the project (“squirix is a distributed cache…”).
- Repository and Git hosting paths in documentation (e.g. `https://github.com/squirix/squirix`).
- Directory names under `src/`, `tests/`, and `benchmarks/` use lowercase dotted package-style folders such as
  `squirix`, `squirix.server`, `squirix.unit-tests`, and `squirix.e2e.benchmarks`.
- Test layout: bucket folders `tests/squirix` and `tests/squirix.server`, then lowercase dotted project folders (for
  example `tests/squirix/squirix.unit-tests/Squirix.UnitTests.csproj`). Do not use PascalCase project directories.
- File-based tooling names such as `squirix-runner` (examples) and `tools/sqr-*.cs` scripts.

For **.NET global/local tools**, **`ToolCommandName` is lowercase `squirix`** (or another explicit lowercase command)
while **`PackageId` / assembly names remain PascalCase** `Squirix.*` per .NET conventions.

Install uses the NuGet **package id**; invocation uses **`ToolCommandName`**: `dotnet tool install -g <PackageId>` then
`squirix --help`, or `dotnet tool run squirix` when the manifest maps the command to `squirix`.

## PascalCase `Squirix`

Keep for:

- C# namespaces: `Squirix`, `Squirix.Server.Storage.Journaling`, etc.
- exported types: `SquirixClient`, `SquirixException`, etc.
- Assembly names and `.csproj` file names: `Squirix`, `Squirix.Server.csproj`, etc.
- NuGet package IDs when publishing under .NET conventions: `Squirix`, `Squirix.Server`, etc.
- XML documentation `cref` / type references.
- Configuration JSON sections and environment prefixes where the runtime binds them (e.g. `"Squirix"` settings object,
  `SQUIRIX_*` env vars).
- Observability logical names where adopted (e.g. meter name `Squirix`), unless a separate product-wide rename is
  decided.

## Ambiguous cases

If a string could be either ecosystem or API (for example a title line or a badge), prefer consistency with nearby
context or leave unchanged and discuss in review.
