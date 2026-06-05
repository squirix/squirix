using System;
using System.Net;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Net.Http.Json;
using System.Text;
using System.Threading.Tasks;
using Squirix.Server.Node.Cluster.Membership;
using Squirix.Server.TestKit;
using Xunit;

namespace Squirix.Server.IntegrationTests.Security;

/// <summary>
/// Integration tests verifying API key authentication for REST cache endpoints.
/// Ensures endpoints are protected when keys are enabled and allow anonymous access when disabled.
/// </summary>
/// <remarks>
/// Initializes a new instance of the <see cref="ApiKeyAuthTests" /> class.
/// </remarks>
[Collection("AuthSensitive")]
public sealed class ApiKeyAuthTests : IntegrationTestBase
{
    /// <summary>
    /// Verifies that REST cache endpoints allow anonymous access
    /// when API key authentication is explicitly disabled.
    /// </summary>
    /// <returns>A <see cref="Task" /> representing the asynchronous unit test.</returns>
    [Fact]
    public async Task RestCacheEndpointsAllowAnonymousWhenDisabled()
    {
        using var env = new TempEnvironmentVariable("SQUIRIX_API_KEYS", null);
        var url = GetNextHttpUrl();
        var peers = new[] { new Peer { NodeId = Guid.NewGuid().ToString("N"), Url = url } };

        await using var node = await StartNodeAsync(url, peers);

        using var stringContent = new StringContent("{\"Value\":\"v\",\"Version\":1}", Encoding.UTF8, "application/json");
        var resp = await HttpClient.PutAsync($"{url}/api/v1/cache/ping", stringContent, DefaultCancellationToken);
        Assert.True(resp.IsSuccessStatusCode, $"Expected anonymous success when auth disabled, got {(int)resp.StatusCode} {resp.ReasonPhrase}");
    }

    /// <summary>
    /// Verifies that REST cache endpoints require a valid API key when authentication is enabled.
    /// Supports both <c>X-Api-Key</c> header and <c>Authorization: Bearer</c> header.
    /// </summary>
    /// <returns>A <see cref="Task" /> representing the asynchronous unit test.</returns>
    [Fact]
    public async Task RestCacheEndpointsRequireApiKeyWhenEnabled()
    {
        using var env = new TempEnvironmentVariable("SQUIRIX_API_KEYS", "secret1");
        var url = GetNextHttpUrl();
        var peers = new[] { new Peer { NodeId = Guid.NewGuid().ToString("N"), Url = url } };

        await using var node = await StartNodeAsync(url, peers);

        var respNoKey = await HttpClient.PutAsJsonAsync($"{url}/api/v1/cache/foo", new CacheEntry<string> { Value = "v", Version = 1L }, DefaultCancellationToken);
        Assert.Equal(HttpStatusCode.Unauthorized, respNoKey.StatusCode);

        using var reqWithHeader = new HttpRequestMessage(HttpMethod.Put, $"{url}/api/v1/cache/foo");
        reqWithHeader.Version = HttpVersion.Version20;
        reqWithHeader.VersionPolicy = HttpVersionPolicy.RequestVersionExact;
        reqWithHeader.Content = new StringContent("{\"Value\":\"v\",\"Version\":1}", Encoding.UTF8, "application/json");
        reqWithHeader.Headers.Add("X-Api-Key", "secret1");
        var respWithKey = await HttpClient.SendAsync(reqWithHeader, DefaultCancellationToken);
        Assert.True(respWithKey.IsSuccessStatusCode, $"Expected success with API key, got {(int)respWithKey.StatusCode} {respWithKey.ReasonPhrase}");

        using var reqGet = new HttpRequestMessage(HttpMethod.Get, $"{url}/api/v1/cache/foo");
        reqGet.Version = HttpVersion.Version20;
        reqGet.VersionPolicy = HttpVersionPolicy.RequestVersionExact;
        reqGet.Headers.Authorization = new AuthenticationHeaderValue("Bearer", "secret1");
        var respGet = await HttpClient.SendAsync(reqGet, DefaultCancellationToken);
        Assert.True(respGet.IsSuccessStatusCode, $"Expected success with Bearer token, got {(int)respGet.StatusCode} {respGet.ReasonPhrase}");
    }
}
