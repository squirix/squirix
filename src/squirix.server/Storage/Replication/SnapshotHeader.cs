using System.Runtime.InteropServices;
using Squirix.Server.Attributes;

namespace Squirix.Server.Storage.Replication;

/// <summary>Fixed header fields decoded from the start of a replica-group snapshot payload.</summary>
[Immutable]
[StructLayout(LayoutKind.Auto)]
internal readonly record struct SnapshotHeader
{
    /// <summary>Initializes a new instance of the <see cref="SnapshotHeader" /> struct.</summary>
    /// <param name="generation">The configuration generation of the group.</param>
    /// <param name="lastIncludedTerm">The term of the entry at <paramref name="lastIncludedIndex" />.</param>
    /// <param name="lastIncludedIndex">The highest committed journal index covered by the snapshot.</param>
    /// <param name="commitIndex">The durable commit index carried by the snapshot.</param>
    /// <param name="offset">The payload read offset directly after the fixed header fields.</param>
    internal SnapshotHeader(ulong generation, ulong lastIncludedTerm, ulong lastIncludedIndex, ulong commitIndex, int offset)
    {
        Generation = generation;
        LastIncludedTerm = lastIncludedTerm;
        LastIncludedIndex = lastIncludedIndex;
        CommitIndex = commitIndex;
        Offset = offset;
    }

    /// <summary>Gets the durable commit index carried by the snapshot.</summary>
    internal ulong CommitIndex { get; }

    /// <summary>Gets the configuration generation of the group.</summary>
    internal ulong Generation { get; }

    /// <summary>Gets the highest committed journal index covered by the snapshot.</summary>
    internal ulong LastIncludedIndex { get; }

    /// <summary>Gets the term of the entry at <see cref="LastIncludedIndex" />.</summary>
    internal ulong LastIncludedTerm { get; }

    /// <summary>Gets the payload read offset directly after the fixed header fields.</summary>
    internal int Offset { get; }
}
