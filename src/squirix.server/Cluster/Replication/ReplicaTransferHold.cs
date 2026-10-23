using System.Threading;
using Squirix.Server.Attributes;

namespace Squirix.Server.Cluster.Replication;

/// <summary>Tells whether a state transfer holds a replica group, which excludes the first start of the group from running meanwhile.</summary>
/// <remarks>The transfer receiver sets it for the lifetime of a transfer ticket; the follower reads it before it takes a bootstrap offer.</remarks>
[ThreadSafe]
internal sealed class ReplicaTransferHold
{
    private int _held;

    /// <summary>Gets a value indicating whether a state transfer holds the group.</summary>
    internal bool IsHeld => Volatile.Read(ref _held) != 0;

    /// <summary>Sets or clears the hold.</summary>
    /// <param name="held"><see langword="true" /> while a transfer ticket exists.</param>
    internal void Set(bool held) => Volatile.Write(ref _held, held ? 1 : 0);
}
