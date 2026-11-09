using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;

namespace Squirix.Testing;

/// <summary>Compares observed identities to a committed golden snapshot.</summary>
internal static class GoldenSnapshotAssert
{
    /// <summary>Checks the snapshot using the comparer of the observed identity set.</summary>
    /// <param name="actual">Observed identities.</param>
    /// <param name="path">Path to the golden snapshot.</param>
    /// <param name="cancellationToken">Cancellation token for reading the file.</param>
    /// <returns>The assertion task.</returns>
    internal static async Task MatchesAsync(HashSet<string> actual, string path, CancellationToken cancellationToken)
    {
        _ = await Assert.That(File.Exists(path)).IsTrue().Because($"the golden snapshot must exist at {path}");
        var lines = await File.ReadAllLinesAsync(path, cancellationToken);
        var expected = new HashSet<string>(actual.Comparer);
        foreach (var line in lines)
        {
            if (line.Length != 0)
                _ = expected.Add(line);
        }

        if (actual.SetEquals(expected))
            return;

        var diff = new StringBuilder();
        _ = diff.Append("Golden snapshot mismatch. Update ").Append(path).AppendLine(" if the change is intentional.");
        AppendDifference(diff, actual, expected, '+');
        AppendDifference(diff, expected, actual, '-');
        Assert.Fail(diff.ToString());
    }

    private static void AppendDifference(StringBuilder diff, HashSet<string> left, HashSet<string> right, char marker)
    {
        var items = new List<string>();
        foreach (var item in left)
        {
            if (!right.Contains(item))
                items.Add(item);
        }

        items.Sort(StringComparer.Ordinal);
        for (var i = 0; i < items.Count; i++)
            _ = diff.Append("  ").Append(marker).Append(' ').AppendLine(items[i]);
    }
}
