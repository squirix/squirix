using System;
using System.Collections.Generic;
using System.IO;
using System.Runtime.InteropServices;
using System.Text;
using Squirix.TestKit.IO;
using Squirix.TestKit.Testing;
using Xunit;

namespace Squirix.UnitTests.Architecture;

/// <summary>
/// v0.1: golden snapshot of exported public API identities for the main <c>Squirix</c> assembly.
/// When the public surface changes intentionally, update <c>ApiSnapshots/SquirixPublicTypes.golden.txt</c>.
/// </summary>
public sealed class PublicApiGoldenSnapshotTests
{
    /// <summary>Ensures the on-disk golden snapshot matches the assembly; fails on unexpected additions or removals.</summary>
    [Fact]
    public void GoldenSnapshotMatchesMainAssemblyExports()
    {
        var assemblyPath = PathKit.Combine(AppContext.BaseDirectory, "Squirix.dll");
        var actual = ExportedApiMetadata.GetExportedApiIdentitySet(assemblyPath);
        var path = PathKit.Combine(AppContext.BaseDirectory, "ApiSnapshots", "SquirixPublicTypes.golden.txt");
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
        _ = sb.AppendLine("Golden public API snapshot mismatch. Update ApiSnapshots/SquirixPublicTypes.golden.txt if the change is intentional.");
        if (unexpected.Count > 0)
        {
            _ = sb.AppendLine("Unexpected (new) exports:");
            var unexpectedSpan = CollectionsMarshal.AsSpan(unexpected);
            for (var i = 0; i < unexpectedSpan.Length; i++)
                _ = sb.Append("  + ").AppendLine(unexpectedSpan[i]);
        }

        if (missing.Count > 0)
        {
            _ = sb.AppendLine("Missing (removed) exports:");
            var missingSpan = CollectionsMarshal.AsSpan(missing);
            for (var i = 0; i < missingSpan.Length; i++)
                _ = sb.Append("  - ").AppendLine(missingSpan[i]);
        }

        Assert.Fail(sb.ToString());
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
