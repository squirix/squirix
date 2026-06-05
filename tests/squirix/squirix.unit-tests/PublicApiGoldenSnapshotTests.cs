using System;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text;
using Squirix.TestKit;
using Squirix.TestKit.IO;
using Xunit;

namespace Squirix.UnitTests;

/// <summary>
/// v0.1: golden snapshot of exported public API identities for the main <c>Squirix</c> assembly.
/// When the public surface changes intentionally, update <c>ApiSnapshots/SquirixPublicTypes.golden.txt</c>.
/// </summary>
public sealed class PublicApiGoldenSnapshotTests
{
    private static readonly Assembly SquirixMainAssembly = typeof(ICache<>).Assembly;

    /// <summary>
    /// Ensures the on-disk golden snapshot matches the assembly; fails on unexpected additions or removals.
    /// </summary>
    [Fact]
    public void GoldenSnapshotMatchesMainAssemblyExports()
    {
        var actual = ExportedTypeReflection.GetExportedApiIdentitySet(SquirixMainAssembly);
        var path = PathKit.Combine(AppContext.BaseDirectory, "ApiSnapshots", "SquirixPublicTypes.golden.txt");
        Assert.True(File.Exists(path), $"Golden file missing: {path}");
        var expected = File.ReadAllLines(path).Select(static l => l.Trim()).Where(static l => l.Length > 0).ToHashSet(StringComparer.Ordinal);

        var unexpected = actual.Except(expected).OrderBy(static s => s, StringComparer.Ordinal).ToArray();
        var missing = expected.Except(actual).OrderBy(static s => s, StringComparer.Ordinal).ToArray();
        if (unexpected.Length == 0 && missing.Length == 0)
            return;

        var sb = new StringBuilder();
        _ = sb.AppendLine("Golden public API snapshot mismatch. Update ApiSnapshots/SquirixPublicTypes.golden.txt if the change is intentional.");
        if (unexpected.Length > 0)
        {
            _ = sb.AppendLine("Unexpected (new) exports:");
            foreach (var u in unexpected)
                _ = sb.Append("  + ").AppendLine(u);
        }

        if (missing.Length > 0)
        {
            _ = sb.AppendLine("Missing (removed) exports:");
            foreach (var m in missing)
                _ = sb.Append("  - ").AppendLine(m);
        }

        Assert.Fail(sb.ToString());
    }
}
