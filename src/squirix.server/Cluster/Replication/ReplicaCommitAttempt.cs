using Squirix.Server.Attributes;
using Squirix.Server.Threading;

namespace Squirix.Server.Cluster.Replication;

/// <summary>Tracks whether the entry of a commit may be in the durable log.</summary>
[ThreadSafe]
internal sealed class ReplicaCommitAttempt
{
    private readonly VolatileBool _appendUnconfirmed = new();
    private readonly VolatileBool _locallyAppended = new();

    /// <summary>Gets a value indicating whether the entry may be in the durable log: it was appended, or its append failed unconfirmed.</summary>
    internal bool MayBeDurable => _locallyAppended.Read() || _appendUnconfirmed.Read();

    internal void MarkAppendUnconfirmed() => _appendUnconfirmed.Write(true);

    internal void MarkLocallyAppended() => _locallyAppended.Write(true);
}
