using System;
using System.Threading;
using Squirix.Server.Attributes;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.Storage.Replication;

namespace Squirix.Server.Node.Services;

/// <summary>Queues a follower that is out of the write quorum for repair as soon as it answers the leader from its log.</summary>
/// <remarks>
/// A follower that was unreachable is verified again only by the readiness service, whose retries back off to several seconds. Its first
/// answer to a heartbeat or an append (accepted, or a log mismatch the leader repairs) proves it reachable, so it is queued at once. A
/// follower that keeps answering without becoming ready is queued at most once per interval, so its repair is retried without spinning.
/// A quarantined follower waits for an explicit recovery, and a log that is not ready cannot be verified, so neither is queued. Runs on
/// the follower reply paths; it never waits and never throws.
/// </remarks>
[ThreadSafe]
internal sealed class ReplicaAnsweringFollowers
{
    private readonly TimeProvider _clock;
    private readonly ReplicaEligibility _eligibility;
    private readonly TimeSpan _interval;
    private readonly long[] _queuedAt;
    private readonly ReplicaRepairQueue _repairs;

    /// <summary>Initializes a new instance of the <see cref="ReplicaAnsweringFollowers" /> class.</summary>
    /// <param name="eligibility">Participation gates of the led group.</param>
    /// <param name="repairs">The queue the readiness service repairs followers from.</param>
    /// <param name="clock">The time source of the interval; only its monotonic timestamp is read.</param>
    /// <param name="interval">The shortest time between two queueings of one follower.</param>
    internal ReplicaAnsweringFollowers(ReplicaEligibility eligibility, ReplicaRepairQueue repairs, TimeProvider clock, TimeSpan interval)
    {
        ArgumentNullException.ThrowIfNull(eligibility);
        ArgumentNullException.ThrowIfNull(repairs);
        ArgumentNullException.ThrowIfNull(clock);
        _eligibility = eligibility;
        _repairs = repairs;
        _clock = clock;
        _interval = interval;
        _queuedAt = new long[eligibility.ReplicaCount];
    }

    /// <summary>Observes one follower reply and queues the follower for repair when it answered from its log while out of the write quorum.</summary>
    /// <param name="replicaIndex">Zero-based follower slot.</param>
    /// <param name="reply">The reply.</param>
    internal void Observe(int replicaIndex, in FollowerLogAppendResult reply)
    {
        if (replicaIndex < 0 || replicaIndex >= _queuedAt.Length || !AnsweredFromLog(in reply))
            return;

        var state = _eligibility.StateFor(replicaIndex);
        if (state is ReplicaParticipantState.Ready or ReplicaParticipantState.Quarantined)
            return;

        var now = _clock.GetTimestamp();
        var last = Volatile.Read(ref _queuedAt[replicaIndex]);
        if (last != 0 && _clock.GetElapsedTime(last, now) < _interval)
            return;

        // One of the concurrent replies of a slot queues it; the queue holds each slot at most once anyway.
        if (Interlocked.CompareExchange(ref _queuedAt[replicaIndex], now == 0 ? 1 : now, last) == last)
            _repairs.Enqueue(replicaIndex);
    }

    private static bool AnsweredFromLog(in FollowerLogAppendResult reply) =>
        reply.Success || string.Equals(reply.RefusalCode, FollowerLogRefusal.LogMismatch, StringComparison.Ordinal);
}
