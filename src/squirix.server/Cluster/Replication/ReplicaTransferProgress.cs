using System;
using System.Threading;
using Squirix.Server.Attributes;

namespace Squirix.Server.Cluster.Replication;

/// <summary>Remembers when a state transfer of this leader to a follower last made progress, so its authority deadline does not fire meanwhile.</summary>
/// <remarks>
/// Posted by the readiness loop of the leader on every accepted transfer reply, read by the election driver in its verdict. It only reports
/// recency: the window of the deadline is not reset by it.
/// </remarks>
[ThreadSafe]
internal sealed class ReplicaTransferProgress
{
    private readonly TimeProvider _clock;
    private long _last = long.MinValue;

    /// <summary>Initializes a new instance of the <see cref="ReplicaTransferProgress" /> class.</summary>
    /// <param name="clock">The time source; only its monotonic timestamp is read.</param>
    internal ReplicaTransferProgress(TimeProvider clock)
    {
        ArgumentNullException.ThrowIfNull(clock);
        _clock = clock;
    }

    /// <summary>Tells whether a transfer made progress within a window.</summary>
    /// <param name="window">How recent the progress must be.</param>
    /// <returns><see langword="true" /> when an accepted transfer reply was recorded within <paramref name="window" />.</returns>
    internal bool IsRecent(TimeSpan window)
    {
        var last = Volatile.Read(ref _last);
        return last != long.MinValue && _clock.GetElapsedTime(last) < window;
    }

    /// <summary>Records an accepted transfer reply.</summary>
    internal void Record() => Volatile.Write(ref _last, _clock.GetTimestamp());
}
