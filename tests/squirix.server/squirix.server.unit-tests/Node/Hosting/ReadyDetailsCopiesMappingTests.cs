using System.Text.Json;
using System.Threading.Tasks;
using Squirix.Server.Adapters.Rest;
using Squirix.Server.Attributes;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.Node.Hosting;
using Squirix.Server.Node.Observability;
using Squirix.Server.Runtime.Diagnostics;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Node.Hosting;

/// <summary>Verifies the copies a leader lists in the ready details reach the response with their state names and lag.</summary>
[Immutable]
public sealed class ReadyDetailsCopiesMappingTests : ServerUnitTestBase
{
    /// <summary>Verifies each copy keeps its node, its participation state name and its lag, and a group without copies lists none.</summary>
    [Test]
    public async Task CopiesReachTheResponse()
    {
        var snapshot = new HealthReadyDetailsSnapshot
        {
            ClientPool = new HealthClientPoolSnapshot(true, 2),
            Coordination = new HealthCoordinationSnapshot(new HealthLeaseSnapshot(false, 0, 0, 0), new HealthWatchSnapshot(false, 0, 0, 0)),
            JournalBacklogOps = 0,
            JournalDisk = new HealthJournalDiskSnapshot("normal", 1, 0, 0, false),
            MemoryPressure = new HealthMemoryPressureSnapshot("normal", 1, 0, 0, 0, false),
            RetentionCleanup = new HealthRetentionCleanupSnapshot(false, 0, 0, null),
            SnapshotAgeSeconds = null,
            SnapshotInFlight = false,
            ReplicaGroups =
            [
                new HealthReplicaGroupSnapshot(
                    "node-a",
                    1,
                    0,
                    0,
                    [
                        new ReplicaPeerStatus("node-b", ReplicaParticipantState.Ready, 0),
                        new ReplicaPeerStatus("node-c", ReplicaParticipantState.CatchingUp, 4),
                        new ReplicaPeerStatus("node-d", ReplicaParticipantState.Recovering, 9),
                    ]),
                new HealthReplicaGroupSnapshot("node-b", 1, 0, 0, []),
            ],
        };

        var response = SquirixEndpointMapping.BuildReadyDetailsResponse(snapshot);
        var element = JsonSerializer.SerializeToElement(response, RestJsonSerializerContext.Default.HealthReadyDetailsResponse);

        var groups = element.GetProperty("replicaGroups");
        var copies = groups[0].GetProperty("copies");
        _ = await Assert.That(copies.GetArrayLength()).IsEqualTo(3);
        _ = await Assert.That((copies[0].GetProperty("nodeId").GetString(), copies[0].GetProperty("state").GetString(), copies[0].GetProperty("lagEntries").GetUInt64()))
                        .IsEqualTo(("node-b", "ready", 0UL));
        _ = await Assert.That((copies[1].GetProperty("state").GetString(), copies[1].GetProperty("lagEntries").GetUInt64())).IsEqualTo(("catching-up", 4UL));
        _ = await Assert.That((copies[2].GetProperty("state").GetString(), copies[2].GetProperty("lagEntries").GetUInt64())).IsEqualTo(("recovering", 9UL));
        _ = await Assert.That(groups[1].GetProperty("copies").GetArrayLength()).IsEqualTo(0);
    }
}
