using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Threading.Tasks;
using Squirix.Server.TestKit.IO;
using Squirix.Server.UnitTests.Support;
using Xunit;

namespace Squirix.Server.UnitTests.ApiSnapshots;

/// <summary>
/// Golden snapshot for the REST endpoint surface exposed by <c>MapSquirixServer</c>.
/// </summary>
public sealed class RestEndpointSurfaceGoldenSnapshotTests : UnitTestBase
{
    /// <summary>Ensures the on-disk golden snapshot matches the production REST route surface.</summary>
    [Fact]
    public async Task GoldenSnapshotMatchesProductionRestEndpointSurface()
    {
        var actual = new HashSet<string>(await RestEndpointSurfaceCollector.CollectProductionRestRoutesAsync(), StringComparer.Ordinal);
        var path = PathKit.Combine(AppContext.BaseDirectory, "ApiSnapshots", "SquirixRestEndpointSurface.golden.txt");
        Assert.True(File.Exists(path), $"Golden file missing: {path}");

        var expected = new HashSet<string>(StringComparer.Ordinal);
        foreach (var line in await File.ReadAllLinesAsync(path, DefaultCancellationToken))
        {
            var trimmed = line.Trim();
            if (trimmed.Length > 0)
                expected.Add(trimmed);
        }

        var unexpected = CollectSetDifference(actual, expected);
        var missing = CollectSetDifference(expected, actual);
        if (unexpected.Length is 0 && missing.Length is 0)
            return;

        var sb = new StringBuilder();
        _ = sb.AppendLine("Golden REST endpoint surface mismatch. Update ApiSnapshots/SquirixRestEndpointSurface.golden.txt if the change is intentional.");
        foreach (var route in unexpected)
            _ = sb.Append("  + ").AppendLine(route);
        foreach (var route in missing)
            _ = sb.Append("  - ").AppendLine(route);

        Assert.Fail(sb.ToString());
    }

    private static string[] CollectSetDifference(IEnumerable<string> left, HashSet<string> right)
    {
        var result = new List<string>();
        foreach (var item in left)
        {
            if (!right.Contains(item))
                result.Add(item);
        }

        result.Sort(StringComparer.Ordinal);
        return result.ToArray();
    }
}
