using System;
using System.Collections.Generic;
using System.IO;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading.Tasks;
using Squirix.TestKit;
using Xunit;

namespace Squirix.UnitTests;

/// <summary>
/// v0.1: golden snapshot of exported public API identities for the main <c>Squirix</c> assembly.
/// When the public surface changes intentionally, update <c>ApiSnapshots/SquirixPublicTypes.golden.txt</c>.
/// </summary>
public sealed class PublicApiGoldenSnapshotTests
{
    /// <summary>Ensures the on-disk golden snapshot matches the assembly; fails on unexpected additions or removals.</summary>
    [Fact]
    public async Task GoldenSnapshotMatchesMainAssemblyExports()
    {
        // Compare the live exported-type identity set against the committed golden file.
        var assemblyPath = PathKit.Combine(AppContext.BaseDirectory, "Squirix.dll");
        var actual = ExportedApiMetadata.GetExportedApiIdentitySet(assemblyPath);
        var path = PathKit.Combine(AppContext.BaseDirectory, "ApiSnapshots", "SquirixPublicTypes.golden.txt");
        Assert.True(File.Exists(path));

        var expected = await LoadIdentityLinesAsync(path);
        if (actual.SetEquals(expected))
            return;

        Assert.Fail(FormatGoldenMismatch(actual, expected));
    }

    private static void AppendDiffSection(StringBuilder sb, string heading, string marker, List<string> items)
    {
        if (items.Count is 0)
            return;

        _ = sb.AppendLine(heading);
        var span = CollectionsMarshal.AsSpan(items);
        for (var i = 0; i < span.Length; i++)
            _ = sb.Append("  ").Append(marker).Append(' ').AppendLine(span[i]);
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
        // Build a deterministic +/− diff so reviewers can update the golden intentionally.
        var unexpected = CollectSetDifference(actual, expected);
        var missing = CollectSetDifference(expected, actual);
        var sb = new StringBuilder();
        _ = sb.AppendLine("Golden public API snapshot mismatch. Update ApiSnapshots/SquirixPublicTypes.golden.txt if the change is intentional.");
        AppendDiffSection(sb, "Unexpected (new) exports:", "+", unexpected);
        AppendDiffSection(sb, "Missing (removed) exports:", "-", missing);
        return sb.ToString();
    }

    private static async Task<HashSet<string>> LoadIdentityLinesAsync(string path)
    {
        var expected = new HashSet<string>(StringComparer.Ordinal);
        var lines = await File.ReadAllLinesAsync(path, TestContext.Current.CancellationToken);
        for (var i = 0; i < lines.Length; i++)
        {
            var line = lines[i];
            if (line.Length is 0)
                continue;

            _ = expected.Add(line);
        }

        return expected;
    }
}
