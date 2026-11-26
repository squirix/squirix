using System;
using System.Threading;
using Squirix.Server.Attributes;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.Logging;
using Squirix.Server.Utils;

namespace Squirix.Server.Node.Services;

/// <summary>Records the appends a leader makes to its own group log, so the election driver can read them without the commit gate or the log gate.</summary>
/// <remarks>
/// The committer owns one recorder for its lifetime, because the pipelines and coordinators that append are recreated after every failed
/// append. Every counter is updated with interlocked operations; <see cref="Snapshot" /> takes no gate, so a hung append that holds both gates
/// cannot keep the driver from reading it. An append ends its record after its counters moved, so a reader never sees it neither in flight nor counted.
/// </remarks>
[ThreadSafe]
internal sealed class ReplicaLocalAppends
{
    private ulong _appended;
    private ulong _failed;
    private int _inFlight;
    private int _refused;
    private (Type Type, ulong Appended)? _reportedFault;
    private ulong _started;

    /// <summary>How an append ended.</summary>
    internal enum Outcome
    {
        /// <summary>Not counted: the append was superseded by a higher term, or the log threw <see cref="ObjectDisposedException" />.</summary>
        Ignored = 0,

        /// <summary>The entry is durable in the log.</summary>
        Appended = 1,

        /// <summary>The append threw or was refused for a reason other than a not-ready log.</summary>
        Failed = 2,

        /// <summary>
        /// The log refused the append because it is not ready, which stays so until an append succeeds. A log disposed after its drain answers the
        /// same way, so it counts as refused too; that happens only while the leadership ends.
        /// </summary>
        Refused = 3,
    }

    /// <summary>Records the start of an append.</summary>
    internal void Begin()
    {
        _ = Interlocked.Increment(ref _started);
        _ = Interlocked.Increment(ref _inFlight);
    }

    /// <summary>Records a write that was refused before any append because the log of the group is not ready.</summary>
    /// <remarks>It counts as a refused append, so a log that latched its failure stays visible to the election driver while no write reaches it.</remarks>
    internal void Refuse()
    {
        Begin();
        End(Outcome.Refused);
    }

    /// <summary>Records the end of an append.</summary>
    /// <param name="outcome">How the append ended.</param>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="outcome" /> is not a named outcome.</exception>
    internal void End(Outcome outcome)
    {
        switch (outcome)
        {
            case Outcome.Appended:
                _ = Interlocked.Increment(ref _appended);
                Volatile.Write(ref _refused, 0);
                break;
            case Outcome.Refused:
                Volatile.Write(ref _refused, 1);
                _ = Interlocked.Increment(ref _failed);
                break;
            case Outcome.Failed:
                _ = Interlocked.Increment(ref _failed);
                break;
            case Outcome.Ignored:
                break;
            default:
                throw new ArgumentOutOfRangeException(nameof(outcome), outcome, "Unsupported append outcome.");
        }

        _ = Interlocked.Decrement(ref _inFlight);
    }

    /// <summary>Clears fault deduplication when the committer ends its run, under the commit gate.</summary>
    internal void ResetFaultReport() => _reportedFault = null;

    /// <summary>Reports a newly failed append under the commit gate, once per fault type and successful append count.</summary>
    /// <param name="error">The error returned by the commit.</param>
    /// <param name="failedBefore">The failed append count before the commit.</param>
    /// <param name="log">The logger for the group committer.</param>
    /// <param name="groupId">The group whose log received the append.</param>
    internal void ReportFault(Exception error, ulong failedBefore, IServerLogger log, string groupId)
    {
        var progress = Snapshot();
        if (progress.Failed == failedBefore)
            return;

        var fault = (error.GetType(), progress.Appended);
        if (_reportedFault != fault)
            ReplicaCommitLog.ReplicaLocalAppendFailed(log, error, groupId);

        _reportedFault = fault;
    }

    /// <summary>Reads the progress without any gate.</summary>
    /// <returns>The counters, and whether an append is in flight or the log refused the last one as not ready.</returns>
    internal ReplicaAppendProgress Snapshot()
    {
        // The flags are read before the counters, and Begin counts the start first: an append that began between the reads shows in the
        // counters but is never missing from them.
        var inFlight = Volatile.Read(ref _inFlight) > 0;
        var refused = Volatile.Read(ref _refused) != 0;
        var appended = Interlocked.Read(ref _appended);
        var failed = Interlocked.Read(ref _failed);
        return new ReplicaAppendProgress(appended, failed, Interlocked.Read(ref _started), inFlight, refused);
    }
}
