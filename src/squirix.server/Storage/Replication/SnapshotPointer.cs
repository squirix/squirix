using System;

namespace Squirix.Server.Storage.Replication;

/// <summary>A validated pointer slot, or sequence zero for an absent or torn slot.</summary>
/// <param name="Sequence">The publication order.</param>
/// <param name="Generation">The immutable snapshot file identity.</param>
/// <param name="Slot">The pointer slot index.</param>
[System.Runtime.InteropServices.StructLayout(System.Runtime.InteropServices.LayoutKind.Auto)]
internal readonly record struct SnapshotPointer(ulong Sequence, Guid Generation, int Slot)
{
    internal (ulong Generation, long Offset, long RewrittenOffset, long RewrittenBoundary) WalPosition { get; init; }
}
