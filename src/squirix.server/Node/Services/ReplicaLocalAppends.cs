using System;
using System.Threading;
using Squirix.Server.Attributes;
using Squirix.Server.Cluster.Replication;

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
