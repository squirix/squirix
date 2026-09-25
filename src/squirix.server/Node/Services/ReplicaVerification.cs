namespace Squirix.Server.Node.Services;

/// <summary>State of the leader-side verification of the owned replica group slots.</summary>
internal enum ReplicaVerification
{
    /// <summary>Every slot, including the leader's own, is verified and counts toward the write quorum.</summary>
    AllReady = 0,

    /// <summary>Some follower is not yet verified, or an uncommitted leader tail is not yet committed; verification should be retried.</summary>
    Pending = 1,

    /// <summary>The leader log is not ready, or its uncommitted tail holds no entry of the current term; slots cannot be verified now.</summary>
    Blocked = 2,
}
