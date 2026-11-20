using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Adapters.Endpoint;
using Squirix.Server.Attributes;
using Squirix.Server.UnitTests.Support;
using Squirix.Testing;
using TUnit.Core;

namespace Squirix.Server.UnitTests.ApiSnapshots;

/// <summary>Golden snapshot for the native management probe routes.</summary>
[Immutable]
public sealed class RestEndpointSurfaceGoldenSnapshotTests : ServerUnitTestBase
{
    /// <summary>Ensures the on-disk golden snapshot matches the production REST route surface.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task MatchesProductionRestEndpointSurface(CancellationToken cancellationToken)
    {
        var actual = new HashSet<string>(StringComparer.Ordinal);
        foreach (var route in NativeManagementEndpoint.Probes)
            _ = actual.Add($"GET {route}");
        var path = PathKit.Combine(AppContext.BaseDirectory, "ApiSnapshots", "SquirixRestEndpointSurface.golden.txt");
        await GoldenSnapshotAssert.MatchesAsync(actual, path, cancellationToken);
    }
}
