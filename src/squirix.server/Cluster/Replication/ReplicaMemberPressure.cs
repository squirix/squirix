using System;
using System.Threading;
using Microsoft.Extensions.Logging;
using Squirix.Server.Attributes;

namespace Squirix.Server.Cluster.Replication;

/// <summary>Tracks which members of a led group reported critical memory pressure in a reply, so the leader can stop the group from growing.</summary>
/// <remarks>
/// <para>
/// Every copy of a group holds the same entries, so the group holds only what its smallest member can hold. A member reports the flag in
/// every append and heartbeat reply while its node is at critical pressure. The leader keeps the time of the last flagged reply per slot,
/// and a reply without the flag clears the slot. A flag lapses after its lifetime, so a member that stopped replying does not block the
/// group for good: the state fails open.
/// </para>
/// <para>
/// One instance lives as long as its committer and outlives the pipelines the committer replaces; it starts empty, so a leader learns of
/// a flagged member only from the first replies it receives. Reads and writes are lock-free, one slot at a time.
/// </para>
/// </remarks>
[ThreadSafe]
internal sealed class ReplicaMemberPressure
{
    private readonly TimeProvider _clock;
    private readonly long[] _flaggedAt = new long[PolicyOptions.MaxReplicaCount];
    private readonly string _groupId;
    private readonly ILogger _log;

    /// <summary>Initializes a new instance of the <see cref="ReplicaMemberPressure" /> class.</summary>
    /// <param name="groupId">The identifier of the group, for logs.</param>
    /// <param name="lifetime">How long a flag counts after the reply that carried it.</param>
    /// <param name="clock">The time source of the lifetime; only its monotonic timestamp is read.</param>
    /// <param name="log">Logger of the flag changes.</param>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="lifetime" /> is not positive.</exception>
    internal ReplicaMemberPressure(string groupId, TimeSpan lifetime, TimeProvider clock, ILogger log)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(groupId);
        ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(lifetime, TimeSpan.Zero);
        ArgumentNullException.ThrowIfNull(clock);
        ArgumentNullException.ThrowIfNull(log);
        _groupId = groupId;
        Lifetime = lifetime;
        _clock = clock;
        _log = log;
    }

    /// <summary>Gets how long a flag counts after the reply that carried it.</summary>
    internal TimeSpan Lifetime { get; }

    /// <summary>Records the flag of one reply of a member.</summary>
    /// <param name="slot">Zero-based slot of the member in its group.</param>
    /// <param name="critical">Whether the reply reported critical memory pressure.</param>
    /// <remarks>Runs on the follower reply paths; it never waits and never throws. A slot outside the group is ignored.</remarks>
    internal void Observe(int slot, bool critical)
    {
        if (slot < 0 || slot >= _flaggedAt.Length)
            return;

        if (!critical)
        {
            if (Volatile.Read(ref _flaggedAt[slot]) != 0 && Interlocked.Exchange(ref _flaggedAt[slot], 0) != 0)
                ReplicaMemberPressureLog.Cleared(_log, _groupId, slot);

            return;
        }

        var now = _clock.GetTimestamp();
        if (Interlocked.Exchange(ref _flaggedAt[slot], now == 0 ? 1 : now) == 0)
            ReplicaMemberPressureLog.Flagged(_log, _groupId, slot);
    }

    /// <summary>Tells whether any member's flag is still within its lifetime.</summary>
    /// <returns><see langword="true" /> when a member reported critical pressure less than the lifetime ago and no later reply cleared it.</returns>
    internal bool IsAnyFresh() => AnyFlagYoungerThan(Lifetime);

    /// <summary>Tells whether the freshest flag is older than half its lifetime, so a new reply should be asked for before it lapses.</summary>
    /// <returns><see langword="true" /> when every fresh flag is at least half the lifetime old; <see langword="false" /> when none is fresh or one is younger.</returns>
    internal bool NeedsRefresh() => IsAnyFresh() && !AnyFlagYoungerThan(Lifetime / 2);

    private bool AnyFlagYoungerThan(TimeSpan age)
    {
        var now = _clock.GetTimestamp();
        for (var i = 0; i < _flaggedAt.Length; i++)
        {
            var flaggedAt = Volatile.Read(ref _flaggedAt[i]);
            if (flaggedAt != 0 && _clock.GetElapsedTime(flaggedAt, now) < age)
                return true;
        }

        return false;
    }
}
