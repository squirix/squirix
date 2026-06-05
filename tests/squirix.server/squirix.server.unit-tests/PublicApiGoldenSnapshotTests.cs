using System;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text;
using Squirix.TestKit;
using Xunit;

namespace Squirix.Server.UnitTests;

/// <summary>
/// Golden snapshot and method allowlist for the intentionally minimal <c>Squirix.Server</c> CLR API.
/// </summary>
public sealed class PublicApiGoldenSnapshotTests
{
    private static readonly Assembly ServerAssembly = typeof(SquirixServer).Assembly;

    /// <summary>
    /// Ensures the on-disk golden snapshot matches the server assembly; fails on unexpected additions or removals.
    /// </summary>
    [Fact]
    public void GoldenSnapshotMatchesServerAssemblyExports()
    {
        var actual = ExportedTypeReflection.GetExportedApiIdentitySet(ServerAssembly);
        var path = Path.Combine(AppContext.BaseDirectory, "ApiSnapshots", "SquirixServerPublicTypes.golden.txt");
        Assert.True(File.Exists(path), $"Golden file missing: {path}");
        var expected = File.ReadAllLines(path).Select(static l => l.Trim()).Where(static l => l.Length > 0).ToHashSet(StringComparer.Ordinal);

        var unexpected = actual.Except(expected).OrderBy(static s => s, StringComparer.Ordinal).ToArray();
        var missing = expected.Except(actual).OrderBy(static s => s, StringComparer.Ordinal).ToArray();
        if (unexpected.Length == 0 && missing.Length == 0)
            return;

        var sb = new StringBuilder();
        _ = sb.AppendLine("Golden public API snapshot mismatch. Update ApiSnapshots/SquirixServerPublicTypes.golden.txt if the change is intentional.");
        foreach (var export in unexpected)
            _ = sb.Append("  + ").AppendLine(export);
        foreach (var export in missing)
            _ = sb.Append("  - ").AppendLine(export);

        Assert.Fail(sb.ToString());
    }

    /// <summary>
    /// Ensures the server package exposes only the canonical lifetime methods.
    /// </summary>
    [Fact]
    public void ServerShouldExposeOnlyCanonicalLifetimeMethods()
    {
        var methods = typeof(SquirixServer).GetMethods(BindingFlags.Public | BindingFlags.Static | BindingFlags.Instance | BindingFlags.DeclaredOnly)
                                           .Where(static method => !method.IsSpecialName).Select(static method => method.Name).Distinct(StringComparer.Ordinal)
                                           .OrderBy(static name => name, StringComparer.Ordinal).ToArray();

        Assert.Equal(["DisposeAsync", "StartAsync"], methods);
    }
}
