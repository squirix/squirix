namespace Squirix.Server.Cluster.Replication;

/// <summary>Why a state transfer ticket was dropped.</summary>
internal enum ReplicaTransferEnd
{
    /// <summary>The transfer finished and the copy is complete at the transferred index.</summary>
    Completed = 0,

    /// <summary>The sender aborted it.</summary>
    Aborted = 1,

    /// <summary>The ticket was idle longer than the idle timeout.</summary>
    Expired = 2,

    /// <summary>A transfer of a higher term took the group over.</summary>
    Superseded = 3,

    /// <summary>A message was malformed, a count or a checksum disagreed, a higher term appeared, or a step failed.</summary>
    Failed = 4,
}
