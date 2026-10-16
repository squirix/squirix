# ILLink Roslyn analyzer with the type namespace fix

`ILLink.RoslynAnalyzer.dll` taken unmodified from `Microsoft.NET.ILLink.Tasks` `12.0.0-alpha.1.26515.111`
(NuGet feed `dotnet12`), 504 KB. Licensed MIT as part of the .NET runtime, see `LICENSE.txt`.

The ILLink analyzer is not part of the SDK: it ships as a NuGet pack, and the SDK resolves the pack version from the
target framework (`KnownILLinkPack` in the SDK's `BundledVersions.props` maps `net10.0` to `10.0.x`). The `10.0.x` and
`11.0.x` packs format a namespace name for every type symbol they inspect; only the `12.0` line dropped that. On
`Squirix.Server.UnitTests` the swap takes the ILLink analyzer time from about 22 s to about 13 s.

`SquirixReplaceIllinkAnalyzer` in `Directory.Build.props` swaps the analyzer of the resolved pack for this file, so the
resolved package and its targets stay untouched, and a shard that drops `Microsoft.NET.ILLink.Tasks` drops this
replacement with it.

Remove this folder and that target once the pack the SDK resolves for the targeted framework ships the fix: a backport
that the SDK's `net10.0` entry picks up, or a retarget to a framework whose pack carries it.
