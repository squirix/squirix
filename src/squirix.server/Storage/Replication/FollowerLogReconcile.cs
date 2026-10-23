using System.Threading;
using System.Threading.Tasks;

namespace Squirix.Server.Storage.Replication;

/// <summary>Tail repair of a follower log: truncates the tail from an index a leader instructs.</summary>
/// <remarks>The caller holds the log gate and has already refused a log that takes no mutating call.</remarks>
internal static class FollowerLogReconcile
{
    internal static async Task<FollowerLogReconcileResult> ReconcileTailAsync(
        FollowerLogJournal journal,
        IFollowerLogContext owner,
        ulong fromIndex,
        ulong prevLogTerm,
        ulong leaderTerm,
        CancellationToken cancellationToken)
    {
        // A stale leader term authorizes nothing: unlike the appending path, repair never advances the
        // current term, it only refuses instructions from deposed leaders.
        if (leaderTerm < owner.Meta.CurrentTerm)
            return new FollowerLogReconcileResult(false, FollowerLogRefusal.StaleTerm, 0, false);

        // A repair instruction is lifecycle-owned and trusted, but it still fails closed if it crosses the
        // durable commit boundary. No leader response may destructively revise a committed prefix.
        // A zero index is caller-bug input rather than storage corruption, so it is refused without quarantine.
        if (fromIndex == 0UL)
            return new FollowerLogReconcileResult(false, FollowerLogRefusal.LogMismatch, 0, false);

        if (fromIndex <= owner.Meta.CommitIndex)
        {
            owner.Readiness = FollowerLogReadiness.Failed;
            return new FollowerLogReconcileResult(false, FollowerLogRefusal.LogMismatch, 0, true);
        }

        // Previous-log consistency, mirroring the appending path: a stale leader must not truncate a tail
        // written by the current term. Verified before the no-op fast path, so a wrong term fails here
        // instead of costing another repair round-trip through the append consistency check.
        // An unverifiable predecessor (compacted below the snapshot baseline) is refused without
        // quarantine; a term conflict at or below the commit boundary fails readiness.
        if (!FollowerLogAppend.PrevTermMatches(journal, fromIndex - 1UL, prevLogTerm))
        {
            if (fromIndex - 1UL > owner.Meta.CommitIndex)
                return new FollowerLogReconcileResult(false, FollowerLogRefusal.LogMismatch, 0, false);
            owner.Readiness = FollowerLogReadiness.Failed;
            return new FollowerLogReconcileResult(false, FollowerLogRefusal.LogMismatch, 0, true);
        }

        if (fromIndex == owner.LastLogIndex + 1UL)
            return new FollowerLogReconcileResult(true, string.Empty, 0, false);

        if (fromIndex > owner.LastLogIndex || !journal.EntryOffsets.ContainsKey(fromIndex))
            return new FollowerLogReconcileResult(false, FollowerLogRefusal.LogMismatch, 0, false);

        try
        {
            var released = await FollowerLogDurable.TruncateFromAsync(journal, owner, fromIndex, cancellationToken).ConfigureAwait(false);
            return new FollowerLogReconcileResult(true, string.Empty, released, false);
        }
        catch
        {
            // The low-level operation reconciles its indexes with any possible SetLength outcome. The explicit
            // repair path additionally quarantines storage because the caller cannot prove which durable boundary
            // survived an I/O fault until restart recovery scans the file again.
            owner.Readiness = FollowerLogReadiness.Failed;
            throw;
        }
    }
}
