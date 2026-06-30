using System.Threading.Tasks;
using Grpc.Core;
using Squirix.E2ETests.Support;
using Squirix.E2ETests.Support.Auth;
using Squirix.E2ETests.Support.Client;
using Squirix.E2ETests.Support.Cluster;
using Squirix.Server.TestKit.Hosting;
using Xunit;

namespace Squirix.E2ETests.Client;

/// <summary>
/// End-to-end coverage for <see cref="SquirixOptions" /> transport and auth extension points.
/// </summary>
public sealed class TransportOptionsTests : EndToEndTestBase
{
    /// <summary>
    /// Verifies <see cref="SquirixOptions.BearerTokenProvider" /> supplies JWT authentication for cache RPCs.
    /// </summary>
    [Fact]
    public async Task ClientAuthenticatesWithBearerTokenProvider()
    {
        var credentials = JwtHelper.CreateSymmetricCredentials();
        var bearerToken = JwtHelper.CreateBearerToken(credentials);
        var security = new TestNodeSecurityOptions
        {
            JwtSigningKey = credentials.Base64SigningKey,
            JwtIssuer = credentials.Issuer,
            JwtAudience = credentials.Audience,
        };

        await using var cluster = await HostedCluster.StartSingleNodeAsync(
            nameof(ClientAuthenticatesWithBearerTokenProvider),
            security,
            cancellationToken: DefaultCancellationToken);
        var uri = cluster.GetUri("nodeA");

        await using var client = await LoopbackConnect.ConnectAsync(
            options =>
            {
                options.Endpoints.Add(uri);
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
    [Fact]
    public async Task ClientFailsWhenJwtRequiredButNotConfigured()
    {
        var credentials = JwtHelper.CreateSymmetricCredentials();
        var security = new TestNodeSecurityOptions
        {
            JwtSigningKey = credentials.Base64SigningKey,
            JwtIssuer = credentials.Issuer,
            JwtAudience = credentials.Audience,
        };
        await using var cluster = await HostedCluster.StartSingleNodeAsync(
            nameof(ClientFailsWhenJwtRequiredButNotConfigured),
            security,
            cancellationToken: DefaultCancellationToken);
        var uri = cluster.GetUri("nodeA");

        await using var client = await LoopbackConnect.ConnectAsync(uri, DefaultCancellationToken);
        var cache = await client.GetCacheAsync<string>("default", DefaultCancellationToken);

        var ex = await Assert.ThrowsAsync<RpcException>(() => cache.SetAsync("jwt-missing", "v", cancellationToken: DefaultCancellationToken));
        Assert.Equal(StatusCode.Unauthenticated, ex.StatusCode);
    }
}
