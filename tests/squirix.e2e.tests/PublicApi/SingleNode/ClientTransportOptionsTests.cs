using System.Threading.Tasks;
using Grpc.Core;
using Squirix.E2ETests.Infrastructure;
using Squirix.Server.TestKit.AspNetCore;
using Xunit;

namespace Squirix.E2ETests.PublicApi.SingleNode;

/// <summary>
/// End-to-end coverage for <see cref="SquirixOptions" /> transport and auth extension points.
/// </summary>
public sealed class ClientTransportOptionsTests : E2ETestBase
{
    /// <summary>
    /// Verifies <see cref="SquirixOptions.BearerTokenProvider" /> supplies JWT authentication for cache RPCs.
    /// </summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Fact]
    public async Task ClientAuthenticatesWithBearerTokenProvider()
    {
        var credentials = E2EJwtHelper.CreateSymmetricCredentials();
        var bearerToken = E2EJwtHelper.CreateBearerToken(credentials);
        var security = new TestNodeSecurityOptions
        {
            JwtSigningKey = credentials.Base64SigningKey,
            JwtIssuer = credentials.Issuer,
            JwtAudience = credentials.Audience,
        };

        await using var cluster = await E2ECluster.StartSingleNodeAsync(nameof(ClientAuthenticatesWithBearerTokenProvider), security, cancellationToken: DefaultCancellationToken);
        var url = cluster.GetAddress("nodeA");

        await using var client = await E2ETestConnect.ConnectAsync(
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
    /// Verifies cache RPCs fail when the server requires JWT but <see cref="SquirixOptions.BearerTokenProvider" /> is unset.
    /// </summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Fact]
    public async Task ClientFailsWhenJwtRequiredButNotConfigured()
    {
        var credentials = E2EJwtHelper.CreateSymmetricCredentials();
        var security = new TestNodeSecurityOptions
        {
            JwtSigningKey = credentials.Base64SigningKey,
            JwtIssuer = credentials.Issuer,
            JwtAudience = credentials.Audience,
        };
        await using var cluster = await E2ECluster.StartSingleNodeAsync(nameof(ClientFailsWhenJwtRequiredButNotConfigured), security, cancellationToken: DefaultCancellationToken);
        var url = cluster.GetAddress("nodeA");

        await using var client = await E2ETestConnect.ConnectAsync(url, DefaultCancellationToken);
        var cache = await client.GetCacheAsync<string>("default", DefaultCancellationToken);

        var ex = await Assert.ThrowsAsync<RpcException>(async () => await cache.SetAsync("jwt-missing", "v", cancellationToken: DefaultCancellationToken));
        Assert.Equal(StatusCode.Unauthenticated, ex.StatusCode);
    }
}
