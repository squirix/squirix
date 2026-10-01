using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.IntegrationTests.Support;
using Squirix.Server.TestKit.Hosting;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.IntegrationTests.Cluster.Replication;

/// <summary>The host gives its replica group logs the idempotency window of the replication policy, not the follower log defaults.</summary>
public sealed class GroupIdempotencyCapacityTests : NodeIntegrationTestBase
{
    /// <summary>The owned group log of a started RF=3 node holds as many outcomes as the policy hashed into the topology fingerprint.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task GroupLogUsesPolicyCapacity(CancellationToken cancellationToken)
    {
        var options = new IntegrationStartOptions { ReplicaCount = 3, UsePersistence = true, CleanTestDir = true, ExtraScope = "group-idempotency-capacity" };
        await using var cluster = await StartClusterAsync("node-a", "node-b", "node-c", options, cancellationToken);

        var served = cluster["node-a"].GetRequiredService<ReplicaGroupRegistry>().TryGetLog("node-a", out var log);

        _ = await Assert.That(served).IsTrue();
        _ = await Assert.That(log!.Idempotency.Capacity).IsEqualTo(PolicyOptions.RfIdempotencyMaxInFlightRecords);
    }
}
