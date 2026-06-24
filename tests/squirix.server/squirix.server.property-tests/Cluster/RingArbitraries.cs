using System;
using System.Collections.Generic;
using System.Globalization;
using System.Runtime.InteropServices;
using FsCheck;
using FsCheck.Fluent;
using JetBrains.Annotations;

namespace Squirix.Server.PropertyTests.Cluster;

/// <summary>
/// Provides FsCheck arbitraries/generators used by the property tests.
/// Generates distinct ASCII-safe node ids and sane vnode counts.
/// </summary>
internal static class RingArbitraries
{
    /// <summary>Distinct node identifiers like "node-123"; array length in [1, 12].</summary>
    /// <returns>
    /// An <see cref="Arbitrary{T}" /> that produces arrays of distinct ASCII-safe node IDs.
    /// </returns>
    [UsedImplicitly]
    public static Arbitrary<string[]> Nodes() => Arb.From(GenNodes(1, 12));

    /// <summary>Virtual nodes per physical node. Prefer values that empirically smooth the distribution.</summary>
    /// <returns>
    /// An <see cref="Arbitrary{T}" /> that produces virtual-node counts chosen from a curated pool
    /// (e.g., 128, 160, 192, …, 512).
    /// </returns>
    [UsedImplicitly]
    public static Arbitrary<int> VirtualNodes()
    {
        var pool = new[] { 128, 160, 192, 224, 256, 320, 384, 448, 512 };
        return Arb.From(Gen.Elements(pool));
    }

    /// <summary>Builds a generator of distinct ASCII-safe node IDs of size within the inclusive range [min, max].</summary>
    /// <param name="min">Inclusive lower bound for the number of nodes (&gt;= 1).</param>
    /// <param name="max">Inclusive upper bound for the number of nodes (&gt;= <paramref name="min" />).</param>
    /// <returns>
    /// A <see cref="Gen{T}" /> producing arrays of distinct node IDs with length in [<paramref name="min" />, <paramref name="max" />].
    /// </returns>
    private static Gen<string[]> GenNodes(int min, int max)
    {
        return Gen.Choose(min, max).SelectMany<int, List<int>, string[]>(
            static size => Gen.Choose(0, int.MaxValue).ListOf(size),
            static (_, ints) =>
            {
                var nodes = new List<string>(ints.Count);
                var seen = new HashSet<string>(StringComparer.Ordinal);
                var intsSpan = CollectionsMarshal.AsSpan(ints);
                for (var i = 0; i < intsSpan.Length; i++)
                {
                    var value = intsSpan[i];
                    var node = $"node-{value.ToString(CultureInfo.InvariantCulture)}";
                    if (seen.Add(node))
                        nodes.Add(node);
                }

                return [.. nodes];
            });
    }
}
