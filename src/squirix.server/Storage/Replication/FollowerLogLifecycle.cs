using System.Threading;

namespace Squirix.Server.Storage.Replication;

/// <summary>The lifecycle bits of a follower log that are read without the log gate: disposed, and complete.</summary>
/// <remarks>
/// The complete bit is a hint for a fast path: a reader that finds it set may be one step behind a state transfer that just cleared it, so the
/// log checks the member state again under its gate before it serves a call.
/// </remarks>
internal static class FollowerLogLifecycle
{
    /// <summary>The log has been disposed of.</summary>
    internal const int Disposed = 1;

    /// <summary>The copy is complete; a state transfer clears it when it marks the copy incomplete, and its completion sets it again.</summary>
    internal const int Complete = 2;

    /// <summary>Makes metadata the metadata of the log, and publishes whether the copy is complete to readers outside the gate.</summary>
    /// <param name="slot">The metadata of the log.</param>
    /// <param name="lifecycle">The lifecycle bits of the log.</param>
    /// <param name="value">The new metadata.</param>
    internal static void Assign(ref GroupLogMetadata slot, ref int lifecycle, in GroupLogMetadata value)
    {
        slot = value;
        _ = value.MemberState == GroupMemberState.Complete ? Interlocked.Or(ref lifecycle, Complete) : Interlocked.And(ref lifecycle, ~Complete);
    }
}
