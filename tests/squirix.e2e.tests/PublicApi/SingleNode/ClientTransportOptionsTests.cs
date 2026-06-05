using System.Threading;
using System.Threading.Tasks;
using Grpc.Core;
using Squirix.E2ETests.Infrastructure;
using Xunit;

namespace Squirix.E2ETests.PublicApi.SingleNode;

/// <summary>
/// End-to-end coverage for <see cref="SquirixOptions" /> transport and auth extension points.
/// </summary>
public sealed class ClientTransportOptionsTests : E2ETestBase
{
    private const string ApiKeyValue = "e2e-api-key";

    /// <summary>
    /// Verifies <see cref="SquirixOptions.BearerTokenProvider" /> supplies JWT authentication for cache RPCs.
    /// </summary>
    /// <returns>A task that completes when assertions pass.</returns>
    [Fact]
    public async Task ClientAuthenticatesWithBearerTokenProvider()
    {
        var credentials = E2EJwtHelper.CreateSymmetricCredentials();
        var bearerToken = E2EJwtHelper.CreateBearerToken(credentials);
        using var apiKeys = new E2ETempEnvironment("SQUIRIX_API_KEYS", null);
        using var signingKey = new E2ETempEnvironment("SQUIRIX_JWT_SIGNING_KEY", credentials.Base64SigningKey);
        using var issuer = new E2ETempEnvironment("SQUIRIX_JWT_ISSUER", credentials.Issuer);
        using var audience = new E2ETempEnvironment("SQUIRIX_JWT_AUDIENCE", credentials.Audience);

        await using var cluster = await E2ECluster.StartSingleNodeAsync(nameof(ClientAuthenticatesWithBearerTokenProvider), DefaultCancellationToken);
        var url = cluster.GetAddress("nodeA");

        await using var client = await SquirixClient.ConnectAsync(
            options =>
            {
                options.Endpoints.Add(url);
                options.BearerTokenProvider = _ => new ValueTask<string>(bearerToken);
            },
            DefaultCancellationToken);

        var cache = await client.GetCacheAsync<string>("default", DefaultCancellationToken);
        await cache.SetAsync("jwt-e2e", "ok", cancellationToken: DefaultCancellationToken);
        Assert.Equal("ok", (await cache.GetValueAsync("jwt-e2e", DefaultCancellationToken)).Value);
    }

    /// <summary>
    /// Verifies <see cref="SquirixOptions.ApiKey" /> authenticates against a key-protected node.
    /// </summary>
    /// <returns>A task that completes when assertions pass.</returns>
    [Fact]
    public async Task ClientAuthenticatesWithConfiguredApiKey()
    {
        using var apiKeys = new E2ETempEnvironment("SQUIRIX_API_KEYS", ApiKeyValue);
        await using var cluster = await E2ECluster.StartSingleNodeAsync(nameof(ClientAuthenticatesWithConfiguredApiKey), DefaultCancellationToken);
        var url = cluster.GetAddress("nodeA");

        await using var client = await SquirixClient.ConnectAsync(
            options =>
            {
                options.Endpoints.Add(url);
                options.ApiKey = ApiKeyValue;
            },
            DefaultCancellationToken);

        var cache = await client.GetCacheAsync<string>("default", DefaultCancellationToken);
        await cache.SetAsync("api-key-e2e", "ok", cancellationToken: DefaultCancellationToken);
        Assert.Equal("ok", (await cache.GetValueAsync("api-key-e2e", DefaultCancellationToken)).Value);
    }

    /// <summary>
    /// Verifies cache RPCs fail when the server requires an API key but <see cref="SquirixOptions.ApiKey" /> is unset.
    /// </summary>
    /// <returns>A task that completes when assertions pass.</returns>
    [Fact]
    public async Task ClientFailsWhenApiKeyRequiredButNotConfigured()
    {
        using var apiKeys = new E2ETempEnvironment("SQUIRIX_API_KEYS", ApiKeyValue);
        await using var cluster = await E2ECluster.StartSingleNodeAsync(nameof(ClientFailsWhenApiKeyRequiredButNotConfigured), DefaultCancellationToken);
        var url = cluster.GetAddress("nodeA");

        await using var client = await SquirixClient.ConnectAsync(url, DefaultCancellationToken);
        var cache = await client.GetCacheAsync<string>("default", DefaultCancellationToken);

        var ex = await Assert.ThrowsAsync<RpcException>(async () => await cache.SetAsync("api-key-missing", "v", cancellationToken: DefaultCancellationToken));
        Assert.Equal(StatusCode.Unauthenticated, ex.StatusCode);
    }
}
