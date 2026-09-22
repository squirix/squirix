using System;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Core;
using Squirix.Server.IntegrationTests.Support;
using Squirix.Server.Storage;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.IntegrationTests;

/// <summary>
/// Integration tests for the <c language="csharp">/health/ready/details</c> endpoint.
/// Verifies that readiness reporting includes journal backlog, snapshot status,
/// compaction state, and client pool configuration.
/// </summary>
public sealed class HealthReadinessTests : NodeIntegrationTestBase
{
    /// <summary>
    /// Ensures the <c language="csharp">/health/ready/details</c> endpoint reports all core signals:
    /// <list type="bullet">
    ///     <item>
    ///         <description>journal backlog size is non-zero after writes.</description>
    ///     </item>
    ///     <item>
    ///         <description>Snapshot in-flight flag is present and boolean.</description>
    ///     </item>
    ///     <item>
    ///         <description>Snapshot age is reported as either <see langword="null" /> or numeric.</description>
    ///     </item>
    ///     <item>
    ///         <description>Compaction object includes state and in-flight flag.</description>
    ///     </item>
    ///     <item>
    ///         <description>Client pool reports configured status and peer count.</description>
    ///     </item>
    /// </list>
    /// </summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ReadyDetailsReportsReadinessSignals(CancellationToken cancellationToken)
    {
        var uri = GetNextHttpUri();

        await using var node = await StartNodeAsync(uri, "node_health_A", new NodeStartOptions { UsePersistence = true }, cancellationToken);
        var cache = GetCache(node);

        await cache.SetEntryAsync(IntegrationMutationOpIds.Default, ServerCacheNames.DefaultNamespace, "health:k1", BuildEntry("v", version: 1), cancellationToken);

        var json = await FetchReadyDetailsAsync(node.Uri, cancellationToken);

        await AssertJournalReadinessAsync(json);
        await AssertSnapshotReadinessAsync(json);
        await AssertCompactionReadinessAsync(json);
        await AssertClientPoolReadinessAsync(json);
        await AssertCoordinationReadinessAsync(json);
        await AssertMemoryPressureReadinessAsync(json);
        await AssertJournalDiskReadinessAsync(json);
        await AssertRetentionCleanupReadinessAsync(json);
    }

    private static async Task AssertClientPoolReadinessAsync(JsonElement json)
    {
        _ = await Assert.That(json.TryGetProperty("clientPool", out var pool)).IsTrue();
        _ = await Assert.That(pool.ValueKind).IsEqualTo(JsonValueKind.Object);
        _ = await Assert.That(pool.TryGetProperty("configured", out var configured)).IsTrue();
        _ = await Assert.That(configured.ValueKind == JsonValueKind.True || configured.ValueKind == JsonValueKind.False).IsTrue();
        _ = await Assert.That(pool.TryGetProperty("peers", out var peersCount)).IsTrue();
        _ = await Assert.That(peersCount.GetInt32() >= 1).IsTrue();
    }

    private static async Task AssertCompactionReadinessAsync(JsonElement json)
    {
        _ = await Assert.That(json.TryGetProperty("compaction", out var compaction)).IsTrue();
        _ = await Assert.That(compaction.ValueKind).IsEqualTo(JsonValueKind.Object);
        _ = await Assert.That(compaction.TryGetProperty("state", out var stateProp)).IsTrue();
        _ = await Assert.That(stateProp.ValueKind).IsEqualTo(JsonValueKind.String);
        _ = await Assert.That(compaction.TryGetProperty("inFlight", out var compInFlight)).IsTrue();
        _ = await Assert.That(compInFlight.ValueKind == JsonValueKind.True || compInFlight.ValueKind == JsonValueKind.False).IsTrue();
    }

    private static async Task AssertCoordinationReadinessAsync(JsonElement json)
    {
        _ = await Assert.That(json.TryGetProperty("coordination", out var coordination)).IsTrue();
        _ = await Assert.That(coordination.ValueKind).IsEqualTo(JsonValueKind.Object);
        _ = await Assert.That(coordination.TryGetProperty("leases", out var leases)).IsTrue();
        _ = await Assert.That(leases.GetProperty("configured").GetBoolean()).IsFalse();
        _ = await Assert.That(leases.GetProperty("active").GetInt32()).IsEqualTo(0);
        _ = await Assert.That(coordination.TryGetProperty("watches", out var watches)).IsTrue();
        _ = await Assert.That(watches.GetProperty("configured").GetBoolean()).IsFalse();
        _ = await Assert.That(watches.GetProperty("active").GetInt32()).IsEqualTo(0);
    }

    private static async Task AssertJournalDiskReadinessAsync(JsonElement json)
    {
        _ = await Assert.That(json.TryGetProperty("journalDisk", out var journalDisk)).IsTrue();
        _ = await Assert.That(journalDisk.ValueKind).IsEqualTo(JsonValueKind.Object);
        _ = await Assert.That(journalDisk.TryGetProperty("state", out var state)).IsTrue();
        _ = await Assert.That(state.ValueKind).IsEqualTo(JsonValueKind.String);
        var stateValue = state.GetString();
        _ = await Assert.That(
            string.Equals(stateValue, "normal", StringComparison.Ordinal) || string.Equals(stateValue, "high", StringComparison.Ordinal) ||
            string.Equals(stateValue, "critical", StringComparison.Ordinal)).IsTrue();
        _ = await Assert.That(journalDisk.TryGetProperty("maxBytes", out var maxBytes)).IsTrue();
        _ = await Assert.That(maxBytes.ValueKind).IsEqualTo(JsonValueKind.Number);
        _ = await Assert.That(maxBytes.GetInt64() > 0).IsTrue();
        _ = await Assert.That(journalDisk.TryGetProperty("usedBytes", out var usedBytes)).IsTrue();
        _ = await Assert.That(usedBytes.ValueKind).IsEqualTo(JsonValueKind.Number);
        _ = await Assert.That(journalDisk.TryGetProperty("highWaterBytes", out var highWater)).IsTrue();
        _ = await Assert.That(highWater.ValueKind).IsEqualTo(JsonValueKind.Number);
        _ = await Assert.That(highWater.GetInt64()).IsEqualTo(maxBytes.GetInt64() * JournalSegmentLimits.HighWaterPercent / 100L);
        _ = await Assert.That(journalDisk.TryGetProperty("writeRejectionActive", out var rejection)).IsTrue();
        _ = await Assert.That(rejection.ValueKind == JsonValueKind.True || rejection.ValueKind == JsonValueKind.False).IsTrue();
    }

    private static async Task AssertJournalReadinessAsync(JsonElement json)
    {
        _ = await Assert.That(json.TryGetProperty("journalBacklogOps", out var journalBacklogProp)).IsTrue();
        _ = await Assert.That(journalBacklogProp.ValueKind is JsonValueKind.Number).IsTrue();
        _ = await Assert.That(journalBacklogProp.GetUInt64() >= 1).IsTrue();
    }

    private static async Task AssertMemoryPressureReadinessAsync(JsonElement json)
    {
        _ = await Assert.That(json.TryGetProperty("memoryPressure", out var memoryPressure)).IsTrue();
        _ = await Assert.That(memoryPressure.ValueKind).IsEqualTo(JsonValueKind.Object);
        _ = await Assert.That(memoryPressure.TryGetProperty("state", out var memState)).IsTrue();
        _ = await Assert.That(memState.ValueKind).IsEqualTo(JsonValueKind.String);
        _ = await Assert.That(memoryPressure.TryGetProperty("maxEstimatedCacheBytes", out var memMax)).IsTrue();
        _ = await Assert.That(memMax.ValueKind).IsEqualTo(JsonValueKind.Number);
        _ = await Assert.That(memMax.GetInt64() > 0).IsTrue();
        _ = await Assert.That(memoryPressure.TryGetProperty("estimatedCacheBytes", out var memEst)).IsTrue();
        _ = await Assert.That(memEst.ValueKind).IsEqualTo(JsonValueKind.Number);
        _ = await Assert.That(memoryPressure.TryGetProperty("entryCount", out var memEntries)).IsTrue();
        _ = await Assert.That(memEntries.ValueKind).IsEqualTo(JsonValueKind.Number);
        _ = await Assert.That(memoryPressure.TryGetProperty("rejectedWriteCount", out var memRej)).IsTrue();
        _ = await Assert.That(memRej.ValueKind).IsEqualTo(JsonValueKind.Number);
        _ = await Assert.That(memoryPressure.TryGetProperty("writeRejectionActive", out var memWra)).IsTrue();
        _ = await Assert.That(memWra.GetBoolean()).IsTrue();
    }

    private static async Task AssertRetentionCleanupReadinessAsync(JsonElement json)
    {
        _ = await Assert.That(json.TryGetProperty("retentionCleanup", out var retentionCleanup)).IsTrue();
        _ = await Assert.That(retentionCleanup.ValueKind).IsEqualTo(JsonValueKind.Object);
        _ = await Assert.That(retentionCleanup.TryGetProperty("degraded", out var degraded)).IsTrue();
        _ = await Assert.That(degraded.ValueKind == JsonValueKind.True || degraded.ValueKind == JsonValueKind.False).IsTrue();
        _ = await Assert.That(retentionCleanup.TryGetProperty("consecutiveWriteFailures", out var consecutive)).IsTrue();
        _ = await Assert.That(consecutive.ValueKind).IsEqualTo(JsonValueKind.Number);
        _ = await Assert.That(retentionCleanup.TryGetProperty("recentFailureCount", out var recent)).IsTrue();
        _ = await Assert.That(recent.ValueKind).IsEqualTo(JsonValueKind.Number);
    }

    private static async Task AssertSnapshotReadinessAsync(JsonElement json)
    {
        _ = await Assert.That(json.TryGetProperty("snapshotInFlight", out var snpFlight)).IsTrue();
        _ = await Assert.That(snpFlight.ValueKind == JsonValueKind.True || snpFlight.ValueKind == JsonValueKind.False).IsTrue();

        _ = await Assert.That(json.TryGetProperty("snapshotAgeSeconds", out var snpAge)).IsTrue();
        _ = await Assert.That(snpAge.ValueKind == JsonValueKind.Null || snpAge.ValueKind == JsonValueKind.Number).IsTrue();
    }

    private async Task<JsonElement> FetchReadyDetailsAsync(Uri uri, CancellationToken cancellationToken)
    {
        var resp = await HttpClient.GetAsync(new Uri(uri, "/health/ready/details"), cancellationToken);
        _ = resp.EnsureSuccessStatusCode();
        var text = await resp.Content.ReadAsStringAsync(cancellationToken);
        using var document = JsonDocument.Parse(text);
        return document.RootElement.Clone();
    }
}
