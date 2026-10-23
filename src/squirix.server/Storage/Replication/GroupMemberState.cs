namespace Squirix.Server.Storage.Replication;

/// <summary>Whether a replica group copy on this node holds the data of the group.</summary>
internal enum GroupMemberState : byte
{
    /// <summary>A copy on a fresh disk that the group has not started or repaired yet; the fail-safe value.</summary>
    Unseeded = 0,

    /// <summary>A copy that holds the data of the group.</summary>
    Complete = 1,

    /// <summary>A copy in the middle of receiving the data of the group.</summary>
    Incomplete = 2,
}
