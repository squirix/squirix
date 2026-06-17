using System;
using System.IO;
using System.Linq;
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
        var actual = (await RestEndpointSurfaceCollector.CollectProductionRestRoutesAsync()).ToHashSet(StringComparer.Ordinal);
        var path = PathKit.Combine(AppContext.BaseDirectory, "ApiSnapshots", "SquirixRestEndpointSurface.golden.txt");
        Assert.True(File.Exists(path), $"Golden file missing: {path}");

        var expected = (await File.ReadAllLinesAsync(path, DefaultCancellationToken))
            .Select(static line => line.Trim())
            .Where(static line => line.Length > 0)
            .ToHashSet(StringComparer.Ordinal);
        var unexpected = actual.Except(expected, StringComparer.Ordinal).Order(StringComparer.Ordinal).ToArray();
        var missing = expected.Except(actual, StringComparer.Ordinal).Order(StringComparer.Ordinal).ToArray();
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
}
