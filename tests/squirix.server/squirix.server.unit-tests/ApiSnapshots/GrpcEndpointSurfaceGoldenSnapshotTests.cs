using System;
using System.Collections.Generic;
using System.IO;
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
        var actual = new HashSet<string>(await GrpcEndpointSurfaceCollector.CollectProductionGrpcMethodsAsync(), StringComparer.OrdinalIgnoreCase);
        var path = PathKit.Combine(AppContext.BaseDirectory, "ApiSnapshots", "SquirixGrpcEndpointSurface.golden.txt");
        Assert.True(File.Exists(path), $"Golden file missing: {path}");

        var expected = new HashSet<string>(StringComparer.Ordinal);
        foreach (var line in await File.ReadAllLinesAsync(path, DefaultCancellationToken))
        {
            var trimmed = line.Trim();
            if (trimmed.Length > 0)
                _ = expected.Add(trimmed);
        }

        var unexpected = CollectSetDifference(actual, expected, StringComparer.OrdinalIgnoreCase);
        var missing = CollectSetDifference(expected, actual, StringComparer.OrdinalIgnoreCase);
        if (unexpected.Count is 0 && missing.Count is 0)
            return;

        var sb = new StringBuilder();
        _ = sb.AppendLine("Golden gRPC endpoint surface mismatch. Update ApiSnapshots/SquirixGrpcEndpointSurface.golden.txt if the change is intentional.");
        for (var i = 0; i < unexpected.Count; i++)
            _ = sb.Append("  + ").AppendLine(unexpected[i]);

        for (var i = 0; i < missing.Count; i++)
            _ = sb.Append("  - ").AppendLine(missing[i]);

        Assert.Fail(sb.ToString());
    }

    private static List<string> CollectSetDifference(IEnumerable<string> left, IReadOnlySet<string> right, StringComparer comparer)
    {
        var result = new List<string>();
        foreach (var item in left)
        {
            if (!SetContains(right, item, comparer))
                result.Add(item);
        }

        result.Sort(StringComparer.Ordinal);
        return result;
    }

    private static bool SetContains(IReadOnlySet<string> set, string item, StringComparer comparer)
    {
        foreach (var candidate in set)
        {
            if (comparer.Equals(candidate, item))
                return true;
        }

        return false;
    }
}
