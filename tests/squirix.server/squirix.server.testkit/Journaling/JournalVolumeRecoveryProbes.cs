using System.Collections.Generic;
using System.Runtime.InteropServices;

namespace Squirix.Server.TestKit.Journaling;

/// <summary>Deterministic recovery probe indices for journal volume stress (not tied to one CI failure key).</summary>
public static class JournalVolumeRecoveryProbes
{
    /// <summary>Same coprime stride as <c>JournalVolumeStressTests</c> sampling.</summary>
    /// <param name="sampleOrdinal">Zero-based sample loop index.</param>
    /// <param name="keyCount">Total keys written during fill.</param>
    /// <returns>Key index for the sample ordinal.</returns>
    public static int SampleIndex(int sampleOrdinal, int keyCount)
    {
        var ordinal = sampleOrdinal * 9973;
        return keyCount > 0 ? ordinal % keyCount : 0;
    }

    /// <summary>
    /// Returns deduplicated indices spanning head, quartiles, tail, and the first two stride samples.
    /// Anchors depend only on <paramref name="keyCount" />, not on a fixed magic index from one run.
    /// </summary>
    /// <param name="keyCount">Total keys written during fill.</param>
    /// <returns>Probe indices to verify after recovery.</returns>
    public static IReadOnlyList<int> AnchorIndices(int keyCount)
    {
        if (keyCount <= 0)
            return [];

        var count = keyCount * 3 / 4;
        var ordered = new List<int>(8)
        {
            0,
            1,
            keyCount / 4,
            keyCount / 2,
            count,
            keyCount - 1,
            SampleIndex(0, keyCount),
            SampleIndex(1, keyCount),
        };

        var seen = new HashSet<int>();
        var result = new List<int>(ordered.Count);
        foreach (var index in CollectionsMarshal.AsSpan(ordered))
        {
            if (index < 0 || index >= keyCount)
                continue;

            if (seen.Add(index))
                result.Add(index);
        }

        return result;
    }
}
