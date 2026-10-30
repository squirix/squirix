using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.UnitTests.Support;
using Squirix.Testing;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
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
        _ = await Assert.That(File.Exists(path)).IsTrue();

        var expected = await LoadIdentityLinesAsync(path, cancellationToken);
        if (actual.SetEquals(expected))
            return;

        Assert.Fail(FormatGoldenMismatch(actual, expected));
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

    /// <summary>Loads the expected API identity lines from the golden snapshot file.</summary>
    /// <param name="path">The golden snapshot file path.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    private static async Task<HashSet<string>> LoadIdentityLinesAsync(string path, CancellationToken cancellationToken)
    {
        var expected = new HashSet<string>(StringComparer.Ordinal);
        var lines = await File.ReadAllLinesAsync(path, cancellationToken);
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
