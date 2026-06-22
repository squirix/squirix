using System;
using System.Collections.Generic;
using System.IO;
using System.Reflection;
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
    private static readonly Assembly SquirixMainAssembly = typeof(ICache<>).Assembly;

    /// <summary>Ensures the on-disk golden snapshot matches the assembly; fails on unexpected additions or removals.</summary>
    [Fact]
    public void GoldenSnapshotMatchesMainAssemblyExports()
    {
        var actual = ExportedTypeReflection.GetExportedApiIdentitySet(SquirixMainAssembly);
        var path = PathKit.Combine(AppContext.BaseDirectory, "ApiSnapshots", "SquirixPublicTypes.golden.txt");
        Assert.True(File.Exists(path), $"Golden file missing: {path}");

        var expected = new HashSet<string>(StringComparer.Ordinal);
        foreach (var line in File.ReadAllLines(path))
        {
            var trimmed = line.Trim();
            if (trimmed.Length > 0)
                expected.Add(trimmed);
        }

        var unexpected = CollectSetDifference(actual, expected, StringComparer.OrdinalIgnoreCase);
        var missing = CollectSetDifference(expected, actual, StringComparer.OrdinalIgnoreCase);
        if (unexpected.Count is 0 && missing.Count is 0)
            return;

        var sb = new StringBuilder();
        _ = sb.AppendLine("Golden public API snapshot mismatch. Update ApiSnapshots/SquirixPublicTypes.golden.txt if the change is intentional.");
        if (unexpected.Count > 0)
        {
            _ = sb.AppendLine("Unexpected (new) exports:");
            foreach (var u in unexpected)
                _ = sb.Append("  + ").AppendLine(u);
        }

        if (missing.Count > 0)
        {
            _ = sb.AppendLine("Missing (removed) exports:");
            foreach (var m in missing)
                _ = sb.Append("  - ").AppendLine(m);
        }

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
