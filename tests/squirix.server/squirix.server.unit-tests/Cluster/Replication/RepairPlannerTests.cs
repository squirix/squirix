using System.Text;
using System.Threading.Tasks;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.Storage.Replication;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Cluster.Replication;

/// <summary>Bounded repair planning.</summary>
public sealed class RepairPlannerTests : ServerUnitTestBase
{
    /// <summary>An empty leader log at the genesis index selects a valid empty entries batch.</summary>
    [Test]
    public async Task EmptyLeaderLogAtGenesisSelectsEmptyBatch()
    {
        var planner = new ReplicaRepairPlanner(2);

        var selection = planner.SelectRepair([], 1UL, null);

        _ = await Assert.That(selection.Kind).IsEqualTo(ReplicaRepairSelectionKind.Entries);
        _ = await Assert.That(selection.Batch.Entries.IsEmpty).IsTrue();
    }

    /// <summary>The planner selects a bounded run and backs up to the follower's known boundary.</summary>
    [Test]
    public async Task PlannerBoundsSequentialRepair()
    {
        var entries = new[]
        {
            Entry(1UL, 1UL, "one"),
            Entry(2UL, 1UL, "two"),
            Entry(3UL, 1UL, "three"),
            Entry(4UL, 2UL, "four"),
        };
        var planner = new ReplicaRepairPlanner(2);

        var batch = planner.SelectBatch(entries, 2UL);

        _ = await Assert.That(batch.PrevLogIndex).IsEqualTo(1UL);
        _ = await Assert.That(batch.PrevLogTerm).IsEqualTo(1UL);
        _ = await Assert.That(batch.Entries.Length).IsEqualTo(2);
        _ = await Assert.That(batch.Entries.Span[0].LogIndex).IsEqualTo(2UL);
        _ = await Assert.That(batch.Entries.Span[1].LogIndex).IsEqualTo(3UL);
        _ = await Assert.That(ReplicaRepairPlanner.BackUpNextIndex(5UL, 2UL)).IsEqualTo(3UL);
    }

    private static FollowerLogEntry Entry(ulong index, ulong term, string payload) => new(index, term, Encoding.UTF8.GetBytes(payload));
}
