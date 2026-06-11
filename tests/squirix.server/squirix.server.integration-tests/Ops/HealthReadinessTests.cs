using System.Net.Http.Json;
using System.Text.Json;
using System.Threading.Tasks;
using Squirix.Server.Cluster.Membership;
using Squirix.Server.Core;
using Xunit;

namespace Squirix.Server.IntegrationTests.Ops;

/// <summary>
/// Integration tests for the <c>/health/ready/details</c> endpoint.
/// Verifies that readiness reporting includes journal backlog, snapshot status,
/// compaction state, and client pool configuration.
/// </summary>
public sealed class HealthReadinessTests : IntegrationTestBase
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
    ///         <description>Snapshot age is reported as either <c>null</c> or numeric.</description>
    ///     </item>
    ///     <item>
    ///         <description>Compaction object includes state and in-flight flag.</description>
    ///     </item>
    ///     <item>
    ///         <description>Client pool reports configured status and peer count.</description>
    ///     </item>
    /// </list>
    /// </summary>
    /// <returns>A <see cref="Task" /> representing the asynchronous operation.</returns>
    [Fact]
    public async Task ReadyDetailsEndpointReportsReadinessSignals()
    {
        var url = GetNextHttpUrl();
        var peers = new[] { new Peer { NodeId = "node_health_A", Url = url } };

        await using var node = await StartNodeAsync(url, peers);
        var cache = GetCache(node);

        // Cause some journal activity
        await cache.SetAsync(CacheNames.DefaultNamespace, "health:k1", BuildEntry("v", version: 1), DefaultCancellationToken);

        var resp = await HttpClient.GetAsync(node.Address + "/health/ready/details", DefaultCancellationToken);
        _ = resp.EnsureSuccessStatusCode();

        var json = await resp.Content.ReadFromJsonAsync<JsonElement>(DefaultCancellationToken);

        Assert.True(json.TryGetProperty("journalBacklogOps", out var journalBacklogProp));
        Assert.True(journalBacklogProp.ValueKind is JsonValueKind.Number);
        Assert.True(journalBacklogProp.GetInt64() >= 1); // after one insert and no snapshot, backlog should be >= 1

        Assert.True(json.TryGetProperty("snapshotInFlight", out var snpFlight));
        Assert.True(snpFlight.ValueKind is JsonValueKind.True or JsonValueKind.False);

        Assert.True(json.TryGetProperty("snapshotAgeSeconds", out var snpAge));

        // May be null if no snapshot exists yet
        Assert.True(snpAge.ValueKind is JsonValueKind.Null or JsonValueKind.Number);

        Assert.True(json.TryGetProperty("compaction", out var compaction));
        Assert.Equal(JsonValueKind.Object, compaction.ValueKind);
        Assert.True(compaction.TryGetProperty("state", out var stateProp));
        Assert.Equal(JsonValueKind.String, stateProp.ValueKind);

        // lastRunUtc may be null (default DateTime) or a string depending on serializer; we only check presence
        Assert.True(compaction.TryGetProperty("inFlight", out var compInFlight));
        Assert.True(compInFlight.ValueKind is JsonValueKind.True or JsonValueKind.False);

        Assert.True(json.TryGetProperty("clientPool", out var pool));
        Assert.Equal(JsonValueKind.Object, pool.ValueKind);
        Assert.True(pool.TryGetProperty("configured", out var configured));
        Assert.True(configured.ValueKind is JsonValueKind.True or JsonValueKind.False);
        Assert.True(pool.TryGetProperty("peers", out var peersCount));
        Assert.True(peersCount.GetInt32() >= 1);

        Assert.True(json.TryGetProperty("coordination", out var coordination));
        Assert.Equal(JsonValueKind.Object, coordination.ValueKind);
        Assert.True(coordination.TryGetProperty("leases", out var leases));
        Assert.False(leases.GetProperty("configured").GetBoolean());
        Assert.Equal(0, leases.GetProperty("active").GetInt32());
        Assert.True(coordination.TryGetProperty("watches", out var watches));
        Assert.False(watches.GetProperty("configured").GetBoolean());
        Assert.Equal(0, watches.GetProperty("active").GetInt32());

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
}
