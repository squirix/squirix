namespace Squirix.Server.Storage.Replication;

/// <summary>Durable storage surface for the follower log.</summary>
internal interface IFollowerLogDurability
{
    /// <summary>Gets the acknowledgments of the durable operations scheduled on pool threads.</summary>
    FollowerLogAckRegistry Acks { get; }

    /// <summary>Gets the fault hooks invoked on durability failures.</summary>
    IFollowerLogFaultHooks Faults { get; }

    /// <summary>Gets the durability policy for the group log.</summary>
    GroupLogDurability Durability { get; }

    /// <summary>Gets the durable log length in bytes.</summary>
    long LogLength { get; }
}
