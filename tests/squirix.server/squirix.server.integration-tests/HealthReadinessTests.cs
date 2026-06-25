using System;
using System.Net.Http.Json;
using System.Text.Json;
using System.Threading.Tasks;
using Squirix.Server.Core;
using Squirix.Server.IntegrationTests.Support;
using Squirix.Server.TestKit;
using Xunit;

namespace Squirix.Server.IntegrationTests;

/// <summary>
/// Integration tests for the <c>/health/ready/details</c> endpoint.
/// Verifies that readiness reporting includes journal backlog, snapshot status,
/// compaction state, and client pool configuration.
/// </summary>
public sealed class HealthReadinessTests : NodeIntegrationTestBase
{
    /// <summary>
    /// Ensures the <c>/health/ready/details</c> endpoint reports all core signals:
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
    [Fact]
    public async Task ReadyDetailsEndpointReportsReadinessSignals()
    {
        var url = GetNextHttpUri();

        await using var node = await StartNodeAsync(url, "node_health_A", usePersistence: true);
        var cache = GetCache(node);

        await cache.SetEntryAsync(TestOperationIds.Default, CacheNames.DefaultNamespace, "health:k1", BuildEntry("v", version: 1), DefaultCancellationToken);

        var json = await FetchReadyDetailsAsync(node.Uri);

        AssertJournalReadiness(json);
        AssertSnapshotReadiness(json);
        AssertCompactionReadiness(json);
        AssertClientPoolReadiness(json);
        AssertCoordinationReadiness(json);
        AssertMemoryPressureReadiness(json);
        AssertJournalDiskReadiness(json);
        AssertRetentionCleanupReadiness(json);
    }

    private static void AssertClientPoolReadiness(JsonElement json)
    {
        Assert.True(json.TryGetProperty("clientPool", out var pool));
        Assert.Equal(JsonValueKind.Object, pool.ValueKind);
        Assert.True(pool.TryGetProperty("configured", out var configured));
        Assert.True(configured.ValueKind is JsonValueKind.True or JsonValueKind.False);
        Assert.True(pool.TryGetProperty("peers", out var peersCount));
        Assert.True(peersCount.GetInt32() >= 1);
    }

    private static void AssertCompactionReadiness(JsonElement json)
    {
        Assert.True(json.TryGetProperty("compaction", out var compaction));
        Assert.Equal(JsonValueKind.Object, compaction.ValueKind);
        Assert.True(compaction.TryGetProperty("state", out var stateProp));
        Assert.Equal(JsonValueKind.String, stateProp.ValueKind);
        Assert.True(compaction.TryGetProperty("inFlight", out var compInFlight));
        Assert.True(compInFlight.ValueKind is JsonValueKind.True or JsonValueKind.False);
    }

    private static void AssertCoordinationReadiness(JsonElement json)
    {
        Assert.True(json.TryGetProperty("coordination", out var coordination));
        Assert.Equal(JsonValueKind.Object, coordination.ValueKind);
        Assert.True(coordination.TryGetProperty("leases", out var leases));
        Assert.False(leases.GetProperty("configured").GetBoolean());
        Assert.Equal(0, leases.GetProperty("active").GetInt32());
        Assert.True(coordination.TryGetProperty("watches", out var watches));
        Assert.False(watches.GetProperty("configured").GetBoolean());
        Assert.Equal(0, watches.GetProperty("active").GetInt32());
    }

    private static void AssertJournalDiskReadiness(JsonElement json)
    {
        Assert.True(json.TryGetProperty("journalDisk", out var journalDisk));
        Assert.Equal(JsonValueKind.Object, journalDisk.ValueKind);
        Assert.True(journalDisk.TryGetProperty("state", out var state));
        Assert.Equal(JsonValueKind.String, state.ValueKind);
        var stateValue = state.GetString();
        Assert.True(
            string.Equals(stateValue, "normal", StringComparison.Ordinal) || string.Equals(stateValue, "high", StringComparison.Ordinal) ||
            string.Equals(stateValue, "critical", StringComparison.Ordinal));
        Assert.True(journalDisk.TryGetProperty("maxBytes", out var maxBytes));
        Assert.Equal(JsonValueKind.Number, maxBytes.ValueKind);
        Assert.True(maxBytes.GetInt64() > 0);
        Assert.True(journalDisk.TryGetProperty("usedBytes", out var usedBytes));
        Assert.Equal(JsonValueKind.Number, usedBytes.ValueKind);
        Assert.True(journalDisk.TryGetProperty("highWaterBytes", out var highWater));
        Assert.Equal(JsonValueKind.Number, highWater.ValueKind);
        Assert.Equal(maxBytes.GetInt64() * JournalSegmentLimits.HighWaterPercent / 100L, highWater.GetInt64());
        Assert.True(journalDisk.TryGetProperty("writeRejectionActive", out var rejection));
        Assert.True(rejection.ValueKind is JsonValueKind.True or JsonValueKind.False);
    }

    private static void AssertJournalReadiness(JsonElement json)
    {
        Assert.True(json.TryGetProperty("journalBacklogOps", out var journalBacklogProp));
        Assert.True(journalBacklogProp.ValueKind is JsonValueKind.Number);
        Assert.True(journalBacklogProp.GetUInt64() >= 1);
    }

    private static void AssertMemoryPressureReadiness(JsonElement json)
    {
        Assert.True(json.TryGetProperty("memoryPressure", out var memoryPressure));
        Assert.Equal(JsonValueKind.Object, memoryPressure.ValueKind);
        Assert.True(memoryPressure.TryGetProperty("state", out var memState));
        Assert.Equal(JsonValueKind.String, memState.ValueKind);
        Assert.True(memoryPressure.TryGetProperty("maxEstimatedCacheBytes", out var memMax));
        Assert.Equal(JsonValueKind.Number, memMax.ValueKind);
        Assert.True(memMax.GetInt64() > 0);
        Assert.True(memoryPressure.TryGetProperty("estimatedCacheBytes", out var memEst));
        Assert.Equal(JsonValueKind.Number, memEst.ValueKind);
        Assert.True(memoryPressure.TryGetProperty("entryCount", out var memEntries));
        Assert.Equal(JsonValueKind.Number, memEntries.ValueKind);
        Assert.True(memoryPressure.TryGetProperty("rejectedWriteCount", out var memRej));
        Assert.Equal(JsonValueKind.Number, memRej.ValueKind);
        Assert.True(memoryPressure.TryGetProperty("writeRejectionActive", out var memWra));
        Assert.True(memWra.GetBoolean());
    }

    private static void AssertRetentionCleanupReadiness(JsonElement json)
    {
        Assert.True(json.TryGetProperty("retentionCleanup", out var retentionCleanup));
        Assert.Equal(JsonValueKind.Object, retentionCleanup.ValueKind);
        Assert.True(retentionCleanup.TryGetProperty("degraded", out var degraded));
        Assert.True(degraded.ValueKind is JsonValueKind.True or JsonValueKind.False);
        Assert.True(retentionCleanup.TryGetProperty("consecutiveWriteFailures", out var consecutive));
        Assert.Equal(JsonValueKind.Number, consecutive.ValueKind);
        Assert.True(retentionCleanup.TryGetProperty("recentFailureCount", out var recent));
        Assert.Equal(JsonValueKind.Number, recent.ValueKind);
    }

    private static void AssertSnapshotReadiness(JsonElement json)
    {
        Assert.True(json.TryGetProperty("snapshotInFlight", out var snpFlight));
        Assert.True(snpFlight.ValueKind is JsonValueKind.True or JsonValueKind.False);

        Assert.True(json.TryGetProperty("snapshotAgeSeconds", out var snpAge));
        Assert.True(snpAge.ValueKind is JsonValueKind.Null or JsonValueKind.Number);
    }

    private async Task<JsonElement> FetchReadyDetailsAsync(Uri uri)
    {
        var resp = await HttpClient.GetAsync(new Uri(uri, "/health/ready/details"), DefaultCancellationToken);
        _ = resp.EnsureSuccessStatusCode();
        return await resp.Content.ReadFromJsonAsync<JsonElement>(DefaultCancellationToken);
    }
}
