using System;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Attributes;
using Squirix.Testing;
using TUnit.Core;

namespace Squirix.UnitTests;

/// <summary>
/// v0.1: golden snapshot of exported public API identities for the main <c language="csharp">Squirix</c> assembly.
/// When the public surface changes intentionally, update <c language="csharp">ApiSnapshots/SquirixPublicTypes.golden.txt</c>.
/// </summary>
[Immutable]
public sealed class PublicApiGoldenSnapshotTests
{
    /// <summary>Ensures the on-disk golden snapshot matches the assembly; fails on unexpected additions or removals.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task SnapshotMatchesMainAssemblyExportsAsync(CancellationToken cancellationToken)
    {
        // Compare the live exported-type identity set against the committed golden file.
        var assemblyPath = PathKit.Combine(AppContext.BaseDirectory, "Squirix.dll");
        var actual = ExportedApiMetadata.GetExportedApiIdentitySet(assemblyPath);
        var path = PathKit.Combine(AppContext.BaseDirectory, "ApiSnapshots", "SquirixPublicTypes.golden.txt");
        await GoldenSnapshotAssert.MatchesAsync(actual, path, cancellationToken);
    }
}
