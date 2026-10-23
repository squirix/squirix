using System.Threading.Tasks;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Cluster.Replication;

/// <summary>Repair probe planning.</summary>
public sealed class RepairPlannerTests : ServerUnitTestBase
{
    /// <summary>The planner backs up to the follower's known boundary.</summary>
    [Test]
    public async Task PlannerBacksUpToFollowerBoundary() => _ = await Assert.That(ReplicaRepairPlanner.BackUpNextIndex(5UL, 2UL)).IsEqualTo(3UL);
}
