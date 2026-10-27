using Squirix.Server.Storage.Replication;

namespace Squirix.Server.UnitTests.Persistence.Replication;

/// <summary>Builds the leader of a state transfer for the group log tests.</summary>
internal static class StateInstallLeaders
{
    /// <summary>The node of the leader that sends the transfers of these tests.</summary>
    internal const string LeaderNodeId = "leader-1";

    /// <summary>Builds a leader that did not check the term ceiling, as every transfer into a copy that did not lose its disk.</summary>
    /// <param name="term">The term of the leader.</param>
    /// <returns>The leader.</returns>
    internal static StateInstallLeader Leader(ulong term) => new(term, LeaderNodeId, false);

    /// <summary>Builds a leader that checked the term ceiling, which may repair a copy that lost its disk.</summary>
    /// <param name="term">The term of the leader.</param>
    /// <returns>The leader.</returns>
    internal static StateInstallLeader CeilingLeader(ulong term) => new(term, LeaderNodeId, true);
}
