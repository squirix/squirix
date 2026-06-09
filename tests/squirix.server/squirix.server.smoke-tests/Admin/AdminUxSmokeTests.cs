using System;
using System.Linq;
using System.Net;
using System.Net.Http.Json;
using System.Text.Json;
using System.Threading.Tasks;
using Squirix.Server.Cluster.Membership;
using Xunit;

namespace Squirix.Server.SmokeTests.Admin;

/// <summary>
/// Smoke tests covering admin UX endpoints such as ring overview and audit feed.
/// </summary>
public sealed class AdminUxSmokeTests : SmokeTestBase
{
    /// <summary>
    /// Validates that admin interactions are captured by the lightweight audit feed.
    /// </summary>
    /// <returns>A task representing the asynchronous smoke test.</returns>
    [Fact]
    public async Task AuditEndpointReturnsRecordedEvents()
    {
        var url = GetNextHttpUrl();
        var peers = new[] { new Peer { NodeId = "nodeA", Url = url } };

        await using var node = await StartNodeAsync(url, peers, cancellationToken: DefaultCancellationToken);

        var whoami = await HttpClient.GetAsync($"{url}/admin/whoami", DefaultCancellationToken);
        _ = whoami.EnsureSuccessStatusCode();

        var auditResponse = await HttpClient.GetAsync($"{url}/admin/audit", DefaultCancellationToken);
        _ = auditResponse.EnsureSuccessStatusCode();

        var audit = await auditResponse.Content.ReadFromJsonAsync<JsonElement>(DefaultCancellationToken);
        var events = audit.GetProperty("events").EnumerateArray().ToArray();
        Assert.NotEmpty(events);

        Assert.Contains(
            events,
            static e => TryGetPropertyCaseInsensitive(e, "Action", out var actionNode) && string.Equals(actionNode.GetString(), "GET /admin/whoami", StringComparison.Ordinal));
    }

    /// <summary>
    /// Ensures the static core reports no rebalance history when runtime membership changes are rejected.
    /// </summary>
    /// <returns>A task representing the asynchronous smoke test.</returns>
    [Fact]
    public async Task RebalanceHistoryIsEmptyWhenMembershipChangesAreRejected()
    {
        var url = GetNextHttpUrl();
        var peers = new[] { new Peer { NodeId = "nodeA", Url = url } };

        await using var node = await StartNodeAsync(url, peers, cancellationToken: DefaultCancellationToken);

        var joinResponse = await HttpClient.PostAsJsonAsync($"{url}/admin/join", new { nodeId = "nodeB" }, DefaultCancellationToken);
        Assert.Equal(HttpStatusCode.Conflict, joinResponse.StatusCode);

        var vnodeResponse = await HttpClient.PostAsJsonAsync($"{url}/admin/vnodes", new { vnodes = 64 }, DefaultCancellationToken);
        Assert.Equal(HttpStatusCode.Conflict, vnodeResponse.StatusCode);

        var historyResponse = await HttpClient.GetAsync($"{url}/admin/rebalance/history", DefaultCancellationToken);
        _ = historyResponse.EnsureSuccessStatusCode();

        var json = await historyResponse.Content.ReadFromJsonAsync<JsonElement>(DefaultCancellationToken);
        Assert.Equal(0, json.GetProperty("retention").GetInt32());

        var events = json.GetProperty("events").EnumerateArray().ToArray();
        Assert.Empty(events);
    }

    /// <summary>
    /// Ensures the ring overview endpoint reports current membership and basic distribution details.
    /// </summary>
    /// <returns>A task representing the asynchronous smoke test.</returns>
    [Fact]
    public async Task RingOverviewReflectsCurrentMembership()
    {
        var url = GetNextHttpUrl();
        var peers = new[] { new Peer { NodeId = "nodeA", Url = url } };

        await using var node = await StartNodeAsync(url, peers, cancellationToken: DefaultCancellationToken);

        var response = await HttpClient.GetAsync($"{url}/admin/ring", DefaultCancellationToken);
        _ = response.EnsureSuccessStatusCode();

        var json = await response.Content.ReadFromJsonAsync<JsonElement>(DefaultCancellationToken);
        Assert.Equal(128, json.GetProperty("virtualNodes").GetInt32());

        var members = json.GetProperty("members").EnumerateArray().Select(static e => e.GetString()).ToArray();
        Assert.Contains("nodeA", members);

        var distribution = json.GetProperty("vnodeDistribution").EnumerateArray().ToArray();
        Assert.NotEmpty(distribution);
        Assert.Contains(distribution, static item => string.Equals(item.GetProperty("nodeId").GetString(), "nodeA", StringComparison.Ordinal));

        var preview = json.GetProperty("ownerLookupSamples").EnumerateArray().ToArray();
        Assert.NotEmpty(preview);
        Assert.All(preview, static item => Assert.True(item.TryGetProperty("owner", out _)));
    }

    /// <summary>
    /// Ensures the storage diagnostics endpoint reports manifest state and recent journal segment headers.
    /// </summary>
    /// <returns>A task representing the asynchronous smoke test.</returns>
    [Fact]
    public async Task StorageDiagnosticsReturnsManifestAndJournalSegments()
    {
        var url = GetNextHttpUrl();
        var peers = new[] { new Peer { NodeId = "nodeA", Url = url } };

        await using var node = await StartNodeAsync(url, peers, cancellationToken: DefaultCancellationToken);
        var cache = GetCacheApiClient(node);
        await cache.InsertAsync("diag:key", BuildEntry("value", version: 1), DefaultCancellationToken);

        var response = await HttpClient.GetAsync($"{url}/admin/diagnostics/storage", DefaultCancellationToken);
        _ = response.EnsureSuccessStatusCode();

        var json = await response.Content.ReadFromJsonAsync<JsonElement>(DefaultCancellationToken);
        Assert.True(json.TryGetProperty("manifest", out _));

        var writer = json.GetProperty("writer");
        Assert.True(writer.GetProperty("currentJournal").GetInt32() >= 1);
        Assert.True(writer.GetProperty("nextSequence").GetUInt64() >= 2);

        var segments = json.GetProperty("journal").GetProperty("segments").EnumerateArray().ToArray();
        Assert.NotEmpty(segments);
        Assert.Contains(segments, static segment => segment.GetProperty("headerValid").GetBoolean());
    }

    private static bool TryGetPropertyCaseInsensitive(JsonElement element, string name, out JsonElement value)
    {
        if (element.TryGetProperty(name, out value))
            return true;

        var lower = name.ToLowerInvariant();
        if (element.TryGetProperty(lower, out value))
            return true;

        var upper = name.ToUpperInvariant();
        if (!string.Equals(upper, name, StringComparison.Ordinal) && element.TryGetProperty(upper, out value))
            return true;

        value = default;
        return false;
    }
}
