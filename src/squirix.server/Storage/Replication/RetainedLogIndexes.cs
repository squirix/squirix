using System.Collections.Generic;

namespace Squirix.Server.Storage.Replication;

/// <summary>Retained journal indexes represented by an exact set or a verified contiguous suffix.</summary>
/// <param name="Indexes">The exact set, or null to use the interval bounds.</param>
/// <param name="Boundary">The exclusive lower bound of the interval.</param>
/// <param name="LastIndex">The inclusive upper bound of the interval.</param>
internal readonly record struct RetainedLogIndexes(HashSet<ulong>? Indexes, ulong Boundary, ulong LastIndex)
{
    /// <summary>Determines whether the journal retains the index.</summary>
    /// <param name="index">The index to test.</param>
    /// <returns>Whether the index belongs to the retained set or interval.</returns>
    internal bool Contains(ulong index) => Indexes?.Contains(index) ?? (index > Boundary && index <= LastIndex);
}
