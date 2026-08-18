using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Attributes;
using Squirix.Server.TestKit;
using Squirix.Server.TestKit.IO;
using Xunit;

namespace Squirix.Server.UnitTests.ApiSnapshots;

/// <summary>
/// Golden snapshot and method allowlist for the intentionally minimal <c>Squirix.Server</c> CLR API.
/// </summary>
[Immutable]
public sealed class NodePublicApiGoldenSnapshotTests
{
    /// <summary>Ensures the on-disk golden snapshot matches the server assembly; fails on unexpected additions or removals.</summary>
    [Fact]
    public async Task GoldenSnapshotMatchesServerAssemblyExportsAsync()
    {
        var assemblyPath = NodePathKit.Combine(AppContext.BaseDirectory, "Squirix.Server.dll");
        var actual = NodeExportedApiMetadata.GetExportedApiIdentitySet(assemblyPath);
        var path = NodePathKit.Combine(AppContext.BaseDirectory, "ApiSnapshots", "SquirixServerPublicTypes.golden.txt");
        Assert.True(File.Exists(path));

        var expected = await LoadIdentityLinesAsync(path);
        if (actual.SetEquals(expected))
            return;

        Assert.Fail(FormatGoldenMismatch(actual, expected));
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

    private static string FormatGoldenMismatch(HashSet<string> actual, HashSet<string> expected)
    {
        var unexpected = CollectSetDifference(actual, expected);
        var missing = CollectSetDifference(expected, actual);
        var sb = new StringBuilder();
        _ = sb.AppendLine("Golden public API snapshot mismatch. Update ApiSnapshots/SquirixServerPublicTypes.golden.txt if the change is intentional.");
        for (var i = 0; i < unexpected.Count; i++)
            _ = sb.Append("  + ").AppendLine(unexpected[i]);

        for (var i = 0; i < missing.Count; i++)
            _ = sb.Append("  - ").AppendLine(missing[i]);

        return sb.ToString();
    }

    private static async Task<HashSet<string>> LoadIdentityLinesAsync(string path)
    {
        var expected = new HashSet<string>(StringComparer.Ordinal);
        var lines = await File.ReadAllLinesAsync(path, TestContext.Current.CancellationToken);
        for (var i = 0; i < lines.Length; i++)
        {
            var line = lines[i];
            if (line.Length == 0)
                continue;

            _ = expected.Add(line);
        }

        return expected;
    }
}
