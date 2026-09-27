using Squirix.Server.Cluster.Replication;

namespace Squirix.Server.UnitTests.Support;

/// <summary>Replica mutation fixtures shared by commit-coordinator tests across areas.</summary>
internal static class ReplicaMutationTestKit
{
    /// <summary>Creates the canonical single-entry mutation of group "group-a" at term 1, log index 1.</summary>
    /// <returns>The prepared mutation.</returns>
    internal static PreparedReplicaMutation CreateMutation()
    {
        var identity = new ReplicaOperationIdentity("group-a", "client", "fedcba9876543210fedcba9876543210", new byte[] { 1 });
        return new PreparedReplicaMutation(identity, 1, 1, new ReplicaMutationPayload(new byte[] { 2 }, new byte[] { 3 }, 4));
    }
}
