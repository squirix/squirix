using System;
using System.Net;
using System.Net.Http.Json;
using System.Text.Json;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Http;
using Squirix.Server.Core;
using Squirix.Server.Node.Cluster.Membership;
using Xunit;

namespace Squirix.Server.SmokeTests.Rest;

/// <summary>
/// Smoke tests asserting REST cache endpoints enforce key and payload size limits.
/// </summary>
public sealed class CachePayloadLimitSmokeTests : SmokeTestBase
{
    private const string Route = "/api/v1/cache";

    /// <summary>
    /// Verifies that inserting with a key longer than the configured limit yields a 400 response and stable error code.
    /// </summary>
    /// <returns>A task representing the asynchronous smoke test.</returns>
    [Fact]
    public async Task PutRejectsKeysOverMaxLength()
    {
        var url = GetNextHttpUrl();
        var peers = new[] { new Peer { NodeId = Guid.NewGuid().ToString("N"), Url = url } };

        await using var node = await StartNodeAsync(url, peers, disableSecurity: true, cancellationToken: DefaultCancellationToken);

        var key = new string('k', CacheKeyValidator.MaxLength + 1);
        var entry = new CacheEntry<string> { Value = "small" };

        var response = await HttpClient.PutAsJsonAsync($"{url}{Route}/{key}", entry, DefaultCancellationToken);
        Assert.Equal(HttpStatusCode.BadRequest, response.StatusCode);

        var payload = await response.Content.ReadFromJsonAsync<JsonElement>(DefaultCancellationToken);
        Assert.True(payload.TryGetProperty("error", out var errorNode));
        Assert.Equal("InvalidCacheKey", errorNode.GetString());
        Assert.Equal("INVALID_CACHE_KEY", payload.GetProperty("code").GetString());
    }

    /// <summary>
    /// Ensures that cache inserts with payloads over one megabyte fail with HTTP 413.
    /// </summary>
    /// <returns>A task representing the asynchronous smoke test.</returns>
    [Fact]
    public async Task PutRejectsPayloadsOverOneMegabyte()
    {
        var url = GetNextHttpUrl();
        var peers = new[] { new Peer { NodeId = Guid.NewGuid().ToString("N"), Url = url } };

        await using var node = await StartNodeAsync(url, peers, disableSecurity: true, cancellationToken: DefaultCancellationToken);

        var largeValue = new string('x', 1_049_000);
        var response = await HttpClient.PutAsJsonAsync($"{url}{Route}/big", new CacheEntry<string> { Value = largeValue }, DefaultCancellationToken);

        Assert.Equal((HttpStatusCode)StatusCodes.Status413PayloadTooLarge, response.StatusCode);

        var payload = await response.Content.ReadFromJsonAsync<JsonElement>(DefaultCancellationToken);
        Assert.Equal("PayloadTooLarge", payload.GetProperty("error").GetString());
    }
}
