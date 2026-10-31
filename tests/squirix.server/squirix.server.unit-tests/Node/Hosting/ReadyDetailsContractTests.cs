using System;
using System.IO;
using System.Text.Json;
using System.Text.Json.Nodes;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Adapters.Rest;
using Squirix.Server.Attributes;
using Squirix.Server.Node.Hosting;
using Squirix.Server.Node.Observability;
using Squirix.Server.Runtime.Diagnostics;
using Squirix.Server.UnitTests.Support;
using Squirix.Testing;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Node.Hosting;

/// <summary>Pins the complete JSON contract written by the ready-details endpoint.</summary>
[Immutable]
public sealed class ReadyDetailsContractTests : ServerUnitTestBase
{
    /// <summary>Every section retains its field names, nesting, JSON types and mapped values, including nullable history.</summary>
    /// <param name="hasHistory">Whether snapshot age and the last retention failure have recorded values.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task ResponseMatchesGoldenJson(bool hasHistory, CancellationToken cancellationToken)
    {
        var snapshot = new HealthReadyDetailsSnapshot
        {
            ClientPool = new HealthClientPoolSnapshot(true, 3),
            Coordination = new HealthCoordinationSnapshot(new HealthLeaseSnapshot(true, 5, 7, 11), new HealthWatchSnapshot(false, 13, 17, 19)),
            JournalBacklogOps = 23,
            JournalDisk = new HealthJournalDiskSnapshot("high", 1000, 800, 750, false),
            MemoryPressure = new HealthMemoryPressureSnapshot("critical", 2000, 1900, 29, 31, true),
            RetentionCleanup = new HealthRetentionCleanupSnapshot(true, 37, 41, hasHistory ? new DateTime(2026, 1, 2, 3, 4, 5, DateTimeKind.Utc) : null),
            SnapshotAgeSeconds = hasHistory ? 12.5 : null,
            SnapshotInFlight = true,
            ReplicaGroups =
            [
                new HealthReplicaGroupSnapshot("node-a", 4096, 43, 47, [new ReplicaPeerStatus("node-b", "catching-up", 53)]),
                new HealthReplicaGroupSnapshot("node-b", 8192, 59, 61, []),
            ],
        };

        var response = SquirixEndpointMapping.BuildReadyDetailsResponse(snapshot);
        var actual = JsonSerializer.SerializeToNode(response, RestJsonSerializerContext.Default.HealthReadyDetailsResponse);
        var path = PathKit.Combine(AppContext.BaseDirectory, "ApiSnapshots", "HealthReadyDetails.golden.json");
        var expected = JsonNode.Parse(await File.ReadAllTextAsync(path, cancellationToken))!;
        if (!hasHistory)
        {
            expected["snapshotAgeSeconds"] = null;
            expected["retentionCleanup"]!["lastFailureUtc"] = null;
        }

        _ = await Assert.That(JsonNode.DeepEquals(expected, actual)).IsTrue()
                        .Because($"The complete ready-details JSON must match the contract. Actual: {actual}");
    }
}
