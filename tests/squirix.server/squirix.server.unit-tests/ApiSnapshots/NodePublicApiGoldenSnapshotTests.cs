using System;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.UnitTests.Support;
using Squirix.Testing;
using TUnit.Core;

namespace Squirix.Server.UnitTests.ApiSnapshots;

/// <summary>Golden snapshot and method allowlist for the intentionally minimal <c language="csharp">Squirix.Server</c> CLR API.</summary>
[Immutable]
public sealed class NodePublicApiGoldenSnapshotTests : ServerUnitTestBase
{
    /// <summary>Ensures the on-disk golden snapshot matches the server assembly; fails on unexpected additions or removals.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task MatchesServerAssemblyExportsAsync(CancellationToken cancellationToken)
    {
        var assemblyPath = PathKit.Combine(AppContext.BaseDirectory, "Squirix.Server.dll");
        var actual = ExportedApiMetadata.GetExportedApiIdentitySet(assemblyPath);
        var path = PathKit.Combine(AppContext.BaseDirectory, "ApiSnapshots", "SquirixServerPublicTypes.golden.txt");
        await GoldenSnapshotAssert.MatchesAsync(actual, path, cancellationToken);
    }
}
