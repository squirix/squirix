using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.Storage.Replication;

namespace Squirix.Server.Node.Services;

/// <summary>Rebuilds the idempotency outcomes of the committed group log entries that no snapshot carries.</summary>
/// <remarks>
/// A group snapshot carries the outcomes of the entries it covers; the outcomes of the committed entries above it lived only in memory.
/// Every committed record carries the outcome the leader decided, so after a restart the owner restores them from its log and a retry of
/// an operation committed before the restart replays its outcome instead of running again.
/// </remarks>
internal static class ReplicaOutcomeRecovery
{
    /// <summary>Restores the outcomes of the newest committed entries retained in <paramref name="log" />.</summary>
    /// <param name="log">The owned group log, opened.</param>
    /// <param name="clock">The clock the decision times are read against.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The number of outcomes restored.</returns>
    /// <exception cref="InvalidDataException">A retained committed record cannot be read back.</exception>
    /// <remarks>
    ///     <para>
    ///     The entries are read back from disk one at a time, applied or not, newest first and no more than the idempotency store holds:
    ///     the pinned recovered tail keeps its places, and the newest log outcomes take what is left: an outcome a snapshot restored is
    ///     displaced by a newer log outcome when the store is full, and replaced by a newer outcome of its identity. The read stops at the
    ///     first outcome that finds no older one to displace.
    ///     </para>
    ///     <para>
    ///     The age of an outcome is measured from the leader time of its decision on the wall clock, the only clock that spans a restart;
    ///     outcomes past retention are skipped. The decision precedes the commit, by at most the commit budget unless a late majority
    ///     committed the entry, so a rebuilt outcome may leave its window that much sooner than it would have without the restart.
    ///     </para>
    /// </remarks>
    internal static async Task<int> RestoreAsync(IFollowerLog log, TimeProvider clock, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(log);
        ArgumentNullException.ThrowIfNull(clock);
        var idempotency = log.Idempotency;
        idempotency.Expire();
        idempotency.BeginOutcomeRebuild();
        var now = clock.GetUtcNow().UtcDateTime;
        var restored = 0;
        _ = await log.ReadRecentCommittedAsync(
                idempotency.Capacity,
                entry =>
                {
                    var record = Rebuild(in entry, out var decidedUtc);
                    var result = idempotency.RestoreOutcome(in record, now - decidedUtc);
                    if (result == GroupOutcomeRestoreResult.Restored)
                        restored++;

                    // Full at this index: no resolved outcome older than it remains, so the older frames would only be read to be refused.
                    return result != GroupOutcomeRestoreResult.Full;
                },
                cancellationToken)
            .ConfigureAwait(false);

        idempotency.MarkOutcomesRebuilt();
        return restored;
    }

    private static GroupIdempotencyRecord Rebuild(in FollowerLogEntry entry, out DateTime decidedUtc)
    {
        if (ReplicaLogCodec.Decode(entry.Payload) is not { } decoded || decoded.LogIndex != entry.LogIndex || decoded.Term != entry.Term ||
            decoded.DecidedUtcTicks < 0 || decoded.DecidedUtcTicks > DateTime.MaxValue.Ticks)
            throw new InvalidDataException($"Committed group log entry {entry.LogIndex} does not carry a readable record of its position.");

        decidedUtc = new DateTime(decoded.DecidedUtcTicks, DateTimeKind.Utc);
        var kind = string.Equals(decoded.OperationScope, ReplicaExpirationOperationId.OperationScope, StringComparison.Ordinal) ? GroupRecordKind.Expiration
            : GroupRecordKind.UserMutation;

        // The decoder hands out owned buffers, so the record keeps them as they are.
        return new GroupIdempotencyRecord(
            decoded.OperationScope,
            decoded.OperationId,
            decoded.OperationFingerprint,
            decoded.OutcomePayload,
            kind,
            decidedUtc,
            decidedUtc,
            decoded.LogIndex,
            decoded.Term);
    }
}
