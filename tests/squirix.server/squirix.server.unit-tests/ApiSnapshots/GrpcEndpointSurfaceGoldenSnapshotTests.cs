using System;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Squirix.Server.TestKit.IO;
using Squirix.Server.UnitTests.Support;
using Xunit;

namespace Squirix.Server.UnitTests.ApiSnapshots;

/// <summary>Golden snapshot for the gRPC service surface exposed by <c>MapSquirixServer</c>.</summary>
public sealed class GrpcEndpointSurfaceGoldenSnapshotTests : UnitTestBase
{
    /// <summary>Ensures the on-disk golden snapshot matches the production gRPC service surface.</summary>
    [Fact]
    public async Task GoldenSnapshotMatchesProductionGrpcEndpointSurface()
    {
        var actual = (await GrpcEndpointSurfaceCollector.CollectProductionGrpcMethodsAsync()).ToHashSet(StringComparer.Ordinal);
        var path = PathKit.Combine(AppContext.BaseDirectory, "ApiSnapshots", "SquirixGrpcEndpointSurface.golden.txt");
        Assert.True(File.Exists(path), $"Golden file missing: {path}");

        var expected = (await File.ReadAllLinesAsync(path, DefaultCancellationToken))
            .Select(static line => line.Trim())
            .Where(static line => line.Length > 0)
            .ToHashSet(StringComparer.Ordinal);
        var unexpected = actual.Except(expected, StringComparer.OrdinalIgnoreCase).Order(StringComparer.Ordinal).ToArray();
        var missing = expected.Except(actual, StringComparer.OrdinalIgnoreCase).Order(StringComparer.Ordinal).ToArray();
        if (unexpected.Length is 0 && missing.Length is 0)
            return;

        var sb = new StringBuilder();
        _ = sb.AppendLine("Golden gRPC endpoint surface mismatch. Update ApiSnapshots/SquirixGrpcEndpointSurface.golden.txt if the change is intentional.");
        foreach (var method in unexpected)
            _ = sb.Append("  + ").AppendLine(method);
        foreach (var method in missing)
            _ = sb.Append("  - ").AppendLine(method);

        Assert.Fail(sb.ToString());
    }
}
