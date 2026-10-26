using System;
using System.Threading;
using Squirix.Server.Attributes;

namespace Squirix.Server.Storage.Journaling;

/// <summary>
/// Hands the segments that retention deleted to the journal thread, so its capacity counters can fall again.
/// Recorded by any thread, taken only by the journal thread, which is the only writer of the counters.
/// </summary>
/// <remarks>
/// The journal subtracts exactly what was deleted instead of re-reading the directory: the length of a segment that is still being
/// appended to can be stale on disk, so a re-read could count less than the bytes written. A segment whose length could not be read is
/// recorded with zero bytes, which keeps the counters above the on-disk total.
/// </remarks>
[Mutable]
internal sealed class JournalCapacityResyncSignal
{
    private long _deletedBytes;
    private int _deletedSegments;
    private Action? _wake;

    /// <summary>Sets the action that wakes an idle journal thread; at a full quota producers are refused before the ring, so nothing else would wake it.</summary>
    /// <param name="wake">Wakes the journal thread; must be safe to call from any thread at any time.</param>
    internal void AttachWake(Action wake)
    {
        ArgumentNullException.ThrowIfNull(wake);
        Volatile.Write(ref _wake, wake);
    }

    /// <summary>Records deleted segments and wakes the journal thread. Call after the segments were deleted.</summary>
    /// <param name="segments">Number of segments deleted; must be positive.</param>
    /// <param name="bytes">Total length of the deleted segments; zero when unknown.</param>
    internal void RecordDeleted(int segments, long bytes)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(segments);
        ArgumentOutOfRangeException.ThrowIfNegative(bytes);
        _ = Interlocked.Add(ref _deletedBytes, bytes);
        _ = Interlocked.Add(ref _deletedSegments, segments);
        Volatile.Read(ref _wake)?.Invoke();
    }

    /// <summary>Takes everything recorded since the last take.</summary>
    /// <param name="segments">Number of deleted segments.</param>
    /// <param name="bytes">Total length of the deleted segments.</param>
    /// <returns><see langword="true" /> when anything was recorded.</returns>
    internal bool TryTake(out int segments, out long bytes)
    {
        // Segments first: a record adds its bytes before its segments, so a take never leaves bytes behind that belong to segments it took.
        segments = Interlocked.Exchange(ref _deletedSegments, 0);
        bytes = Interlocked.Exchange(ref _deletedBytes, 0L);
        return segments != 0 || bytes != 0L;
    }
}
