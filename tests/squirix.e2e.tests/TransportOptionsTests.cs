using System;
using System.Threading;
using System.Threading.Tasks;
using Grpc.Core;
using Squirix.Attributes;
using Squirix.Client;
using Squirix.E2ETests.Cluster;
using Squirix.Server.TestKit;
using Squirix.Server.TestKit.Hosting;
using Xunit;

namespace Squirix.E2ETests;

/// <summary>
/// End-to-end coverage for <see cref="SquirixClientOptions" /> transport and auth extension points.
/// </summary>
[Immutable]
public sealed class TransportOptionsTests : EndToEndTestBase
{
    /// <summary>
    /// Verifies <see cref="SquirixClientOptions.BearerTokenProvider" /> supplies JWT authentication for cache RPCs.
    /// </summary>
    [Fact]
    public async Task ConnectsWithBearerTokenProvider()
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
            nameof(ConnectsWithBearerTokenProvider),
            security,
            cancellationToken: DefaultCancellationToken);
        var uri = cluster.GetUri("nodeA");
        var provider = CreateBearerTokenProvider(bearerToken);

        await using var client = await LoopbackConnect.ConnectAsync(uri, provider, DefaultCancellationToken);

        var cache = await client.GetCacheAsync<string>("default", DefaultCancellationToken);
        await cache.SetAsync("jwt-e2e", "ok", cancellationToken: DefaultCancellationToken);
        Assert.Equal("ok", (await cache.GetValueAsync("jwt-e2e", DefaultCancellationToken)).Value);
    }

    /// <summary>
    /// Verifies cache RPCs fail when the server requires JWT but <see cref="SquirixClientOptions.BearerTokenProvider" /> is unset.
    /// </summary>
    [Fact]
    public async Task FailsWhenJwtRequiredButUnconfigured()
    {
        var credentials = JwtHelper.CreateSymmetricCredentials();
        var security = new TestNodeSecurityOptions
        {
            JwtSigningKey = credentials.Base64SigningKey,
            JwtIssuer = credentials.Issuer,
            JwtAudience = credentials.Audience,
        };
        await using var cluster = await HostedCluster.StartSingleNodeAsync(
            nameof(FailsWhenJwtRequiredButUnconfigured),
            security,
            cancellationToken: DefaultCancellationToken);
        var uri = cluster.GetUri("nodeA");

        await using var client = await LoopbackConnect.ConnectAsync(uri, DefaultCancellationToken);
        var cache = await client.GetCacheAsync<string>("default", DefaultCancellationToken);

        var ex = await NodeAsyncAssert.ThrowsAsync<RpcException>(cache.SetAsync("jwt-missing", "v", cancellationToken: DefaultCancellationToken));
        Assert.Equal(StatusCode.Unauthenticated, ex.StatusCode);
    }

    private static Func<CancellationToken, ValueTask<string>> CreateBearerTokenProvider(string token) => new FixedBearerTokenProvider(token).ProvideAsync;
}
