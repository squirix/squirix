using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.TestKit.IO;
using Squirix.Server.TestKit.Testing;
using Xunit;

namespace Squirix.Server.UnitTests.ApiSnapshots;

/// <summary>
/// Golden snapshot and method allowlist for the intentionally minimal <c>Squirix.Server</c> CLR API.
/// </summary>
public sealed class PublicApiGoldenSnapshotTests
{
    /// <summary>Ensures the on-disk golden snapshot matches the server assembly; fails on unexpected additions or removals.</summary>
    [Fact]
    public void GoldenSnapshotMatchesServerAssemblyExports()
    {
        var assemblyPath = PathKit.Combine(AppContext.BaseDirectory, "Squirix.Server.dll");
        var actual = ExportedApiMetadata.GetExportedApiIdentitySet(assemblyPath);
        var path = PathKit.Combine(AppContext.BaseDirectory, "ApiSnapshots", "SquirixServerPublicTypes.golden.txt");
        Assert.True(File.Exists(path), $"Golden file missing: {path}");

        var expected = new HashSet<string>(StringComparer.Ordinal);
        var lines = File.ReadAllLines(path);
        for (var i = 0; i < lines.Length; i++)
        {
            var line = lines[i];
            if (line.Length is 0)
                continue;

            _ = expected.Add(line);
        }

        if (actual.SetEquals(expected))
            return;

        var unexpected = CollectSetDifference(actual, expected);
        var missing = CollectSetDifference(expected, actual);

        var sb = new StringBuilder();
        _ = sb.AppendLine("Golden public API snapshot mismatch. Update ApiSnapshots/SquirixServerPublicTypes.golden.txt if the change is intentional.");
        for (var i = 0; i < unexpected.Count; i++)
            _ = sb.Append("  + ").AppendLine(unexpected[i]);

        for (var i = 0; i < missing.Count; i++)
            _ = sb.Append("  - ").AppendLine(missing[i]);

        Assert.Fail(sb.ToString());
    }

    /// <summary>Ensures the server package exposes the canonical lifetime methods.</summary>
    [Fact]
    public void ServerShouldExposeCanonicalLifetimeMethods()
    {
        Assert.NotNull((Func<CancellationToken, ValueTask<SquirixServer>>)StartAsync);
        Assert.NotNull((Func<SquirixServer, ValueTask>)DisposeAsync);
        return;

        static ValueTask DisposeAsync(SquirixServer server)
        {
            return server.DisposeAsync();
        }

        static ValueTask<SquirixServer> StartAsync(CancellationToken cancellationToken)
        {
            return SquirixServer.StartAsync(cancellationToken);
        }
    }

    private static List<string> CollectSetDifference(HashSet<string> left, HashSet<string> right)
    {
        var result = new List<string>();
        foreach (var item in left)
        {
            if (!right.Contains(item))
                result.Add(item);
        }

        result.Sort(StringComparer.Ordinal);
        return result;
    }
}
