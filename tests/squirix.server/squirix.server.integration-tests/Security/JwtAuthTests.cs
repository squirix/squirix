using System;
using System.Net;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Net.Http.Json;
using System.Text;
using System.Threading.Tasks;
using Squirix.Server.Cluster.Membership;
using Squirix.Server.TestKit.AspNetCore;
using Squirix.Server.TestKit.Security;
using Xunit;

namespace Squirix.Server.IntegrationTests.Security;

/// <summary>
/// Integration tests verifying JWT authentication for REST cache endpoints.
/// </summary>
public sealed class JwtAuthTests : IntegrationTestBase
{
    /// <summary>
    /// Verifies that REST cache endpoints allow anonymous access when JWT auth is not configured.
    /// </summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Fact]
    public async Task RestCacheEndpointsAllowAnonymousWhenJwtDisabled()
    {
        var url = GetNextHttpUrl();
        var peers = new[] { new Peer { NodeId = Guid.NewGuid().ToString("N"), Url = url } };

        await using var node = await StartNodeAsync(url, peers, security: new TestNodeSecurityOptions());

        using var stringContent = new StringContent("{\"Value\":\"v\",\"Version\":1}", Encoding.UTF8, "application/json");
        var resp = await HttpClient.PutAsync($"{url}/api/v1/cache/ping", stringContent, DefaultCancellationToken);
        Assert.True(resp.IsSuccessStatusCode, $"Expected anonymous success when auth disabled, got {(int)resp.StatusCode} {resp.ReasonPhrase}");
    }

    /// <summary>
    /// Verifies that REST cache endpoints require a valid JWT bearer token when authentication is enabled.
    /// </summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Fact]
    public async Task RestCacheEndpointsRequireJwtWhenEnabled()
    {
        var credentials = TestJwtHelper.CreateRandomCredentials();
        var url = GetNextHttpUrl();
        var peers = new[] { new Peer { NodeId = Guid.NewGuid().ToString("N"), Url = url } };

        await using var node = await StartNodeAsync(url, peers, security: TestJwtHelper.ToSecurityOptions(credentials));

        var respNoToken = await HttpClient.PutAsJsonAsync($"{url}/api/v1/cache/foo", new CacheEntry<string> { Value = "v", Version = 1L }, DefaultCancellationToken);
        Assert.Equal(HttpStatusCode.Unauthorized, respNoToken.StatusCode);

        using var put = new HttpRequestMessage(HttpMethod.Put, $"{url}/api/v1/cache/foo");
        put.Version = HttpVersion.Version20;
        put.VersionPolicy = HttpVersionPolicy.RequestVersionExact;
        put.Content = new StringContent("{\"Value\":\"v\",\"Version\":1}", Encoding.UTF8, "application/json");
        put.Headers.Authorization = new AuthenticationHeaderValue("Bearer", TestJwtHelper.CreateBearerToken(credentials));
        var putResponse = await HttpClient.SendAsync(put, DefaultCancellationToken);
        Assert.True(putResponse.IsSuccessStatusCode, $"Expected success with JWT, got {(int)putResponse.StatusCode} {putResponse.ReasonPhrase}");

        using var get = new HttpRequestMessage(HttpMethod.Get, $"{url}/api/v1/cache/foo");
        get.Version = HttpVersion.Version20;
        get.VersionPolicy = HttpVersionPolicy.RequestVersionExact;
        get.Headers.Authorization = new AuthenticationHeaderValue("Bearer", TestJwtHelper.CreateBearerToken(credentials));
        var getResponse = await HttpClient.SendAsync(get, DefaultCancellationToken);
        Assert.True(getResponse.IsSuccessStatusCode, $"Expected success with JWT, got {(int)getResponse.StatusCode} {getResponse.ReasonPhrase}");
    }
}
