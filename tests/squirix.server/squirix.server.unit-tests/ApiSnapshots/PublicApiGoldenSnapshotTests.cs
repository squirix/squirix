using System;
using System.Collections.Generic;
using System.IO;
using System.Reflection;
using System.Text;
using Squirix.Server.TestKit.IO;
using Squirix.Server.TestKit.Testing;
using Xunit;

namespace Squirix.Server.UnitTests.ApiSnapshots;

/// <summary>
/// Golden snapshot and method allowlist for the intentionally minimal <c>Squirix.Server</c> CLR API.
/// </summary>
public sealed class PublicApiGoldenSnapshotTests
{
    private static readonly Assembly ServerAssembly = typeof(SquirixServer).Assembly;

    /// <summary>Ensures the on-disk golden snapshot matches the server assembly; fails on unexpected additions or removals.</summary>
    [Fact]
    public void GoldenSnapshotMatchesServerAssemblyExports()
    {
        var actual = ExportedTypeReflection.GetExportedApiIdentitySet(ServerAssembly);
        var path = PathKit.Combine(AppContext.BaseDirectory, "ApiSnapshots", "SquirixServerPublicTypes.golden.txt");
        Assert.True(File.Exists(path), $"Golden file missing: {path}");

        var expected = new HashSet<string>(StringComparer.Ordinal);
        foreach (var line in File.ReadAllLines(path))
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
        _ = sb.AppendLine("Golden public API snapshot mismatch. Update ApiSnapshots/SquirixServerPublicTypes.golden.txt if the change is intentional.");
        for (var i = 0; i < unexpected.Count; i++)
            _ = sb.Append("  + ").AppendLine(unexpected[i]);

        for (var i = 0; i < missing.Count; i++)
            _ = sb.Append("  - ").AppendLine(missing[i]);

        Assert.Fail(sb.ToString());
    }

    /// <summary>Ensures the server package exposes only the canonical lifetime methods.</summary>
    [Fact]
    public void ServerShouldExposeOnlyCanonicalLifetimeMethods()
    {
        var methodNames = new HashSet<string>(StringComparer.Ordinal);
        foreach (var method in typeof(SquirixServer).GetMethods(BindingFlags.Public | BindingFlags.Static | BindingFlags.Instance | BindingFlags.DeclaredOnly))
        {
            if (method.IsSpecialName)
                continue;

            _ = methodNames.Add(method.Name);
        }

        var methods = new List<string>(methodNames);
        methods.Sort(StringComparer.Ordinal);

        Assert.Equal(["DisposeAsync", "StartAsync"], methods);
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
