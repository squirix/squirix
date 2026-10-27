using System.Threading;
using Squirix.Server.Attributes;

namespace Squirix.Server.Cluster.Replication;

/// <summary>Whether this node cannot apply the committed entries of a group it follows, so a win of that group would fail its start.</summary>
/// <remarks>
/// Set by the apply loop of the group when a pass fails with a fault that clears by itself (a full node journal, a failed storage, memory
/// that refuses the write) while committed entries are pending or while the outcomes of the log are rebuilt, and when a committed record
/// stopped the group. Cleared by the first pass that ends with nothing left to apply, also after a fault with nothing pending, and while a
/// committer or a state transfer holds the group, unless a stopped group still waits for its transfer. A pass that finds the log not
/// ready or the copy not complete, or that cannot read the log status, leaves it as it is. A stopped group recovers only when a state
/// transfer reseeds it. It is a plain flag read without any gate, so the election driver, which must never wait for the commit gate, can
/// read it on every tick. The loop passes at least once per fallback interval, so a node whose fault cleared is held back that long at
/// most.
/// </remarks>
[ThreadSafe]
internal sealed class ReplicaApplyBlock
{
    private int _assessed;
    private int _blocked;
    private long _episodes;

    /// <summary>Gets how many times the group went from not blocked to blocked, which tells one block from the next.</summary>
    internal long Episodes => Volatile.Read(ref _episodes);

    /// <summary>Gets a value indicating whether the apply loop of the group made a decision for it yet: a pass over a ready, complete copy that blocked or cleared, or one the loop skipped.</summary>
    /// <remarks>
    /// False from the start of the process until the first decision, because the loop waits for local recovery and runs its first pass
    /// after the election driver may have armed, so a group that is not blocked yet may simply not be assessed. It never turns false again,
    /// so a reader that sees it true and then reads <see cref="IsBlocked" /> sees the decision or a later one. A copy that is not
    /// complete or whose log is not ready stays unassessed until a pass over it decides, and so does a group whose log status cannot be
    /// read; the node does not campaign for either meanwhile.
    /// </remarks>
    internal bool IsAssessed => Volatile.Read(ref _assessed) != 0;

    /// <summary>Gets a value indicating whether the last apply pass of the group failed in a way that keeps the node from leading it.</summary>
    internal bool IsBlocked => Volatile.Read(ref _blocked) != 0;

    /// <summary>Gets why the apply keeps this node from leading the group, read without any gate.</summary>
    /// <returns><see cref="FailoverDenial.ApplyUnassessed" /> before the apply loop decided, <see cref="FailoverDenial.ApplyBlocked" /> while it is blocked, otherwise <see cref="FailoverDenial.None" />.</returns>
    internal FailoverDenial Denial() => true switch
    {
        _ when !IsAssessed => FailoverDenial.ApplyUnassessed,
        _ when IsBlocked => FailoverDenial.ApplyBlocked,
        _ => FailoverDenial.None,
    };

    /// <summary>Records that the apply loop looked at the group and found no reason to block it, and leaves the mark as it is.</summary>
    /// <remarks>For a group that runs no apply loop because this node leads it from the start.</remarks>
    internal void Assess() => Volatile.Write(ref _assessed, 1);

    /// <summary>Records that the group cannot be applied; a transition from not blocked starts a new episode.</summary>
    /// <remarks>
    /// The apply loop of the group is the only writer. The episode is counted before the flag is raised, so a reader that sees the group
    /// blocked also sees the episode of that block.
    /// </remarks>
    internal void Block()
    {
        if (Volatile.Read(ref _blocked) == 0)
        {
            _ = Interlocked.Increment(ref _episodes);
            Volatile.Write(ref _blocked, 1);
        }

        Assess();
    }

    /// <summary>Records that nothing committed is left to apply, or that the loop does not apply the group.</summary>
    internal void Clear()
    {
        Volatile.Write(ref _blocked, 0);
        Assess();
    }
}
