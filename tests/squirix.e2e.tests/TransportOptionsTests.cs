using System;
using System.Threading;
using System.Threading.Tasks;
using Grpc.Core;
using Squirix.Attributes;
using Squirix.Client;
using Squirix.E2ETests.Cluster;
using Squirix.Server.TestKit;
using Squirix.Server.TestKit.Hosting;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.E2ETests;

/// <summary>End-to-end coverage for <see cref="SquirixClientOptions" /> transport and auth extension points.</summary>
[Immutable]
public sealed class TransportOptionsTests : EndToEndTestBase
{
    /// <summary>Verifies <see cref="SquirixClientOptions.BearerTokenProvider" /> supplies JWT authentication for cache RPCs.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ConnectsWithBearerTokenProvider(CancellationToken cancellationToken)
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
            timeProvider: TimeProvider.System,
            cancellationToken: cancellationToken);
        var uri = cluster.GetUri("nodeA");
        var provider = CreateBearerTokenProvider(bearerToken);

        await using var client = await LoopbackConnect.ConnectAsync(uri, provider, cancellationToken);

        var cache = await client.GetCacheAsync<string>("default", cancellationToken);
        await cache.SetAsync("jwt-e2e", "ok", cancellationToken: cancellationToken);
        _ = await Assert.That((await cache.GetValueAsync("jwt-e2e", cancellationToken)).Value).IsEqualTo("ok");
    }

    /// <summary>Verifies cache RPCs fail when the server requires JWT but <see cref="SquirixClientOptions.BearerTokenProvider" /> is unset.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task FailsWhenJwtRequiredButUnconfigured(CancellationToken cancellationToken)
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
            timeProvider: TimeProvider.System,
            cancellationToken: cancellationToken);
        var uri = cluster.GetUri("nodeA");

        await using var client = await LoopbackConnect.ConnectAsync(uri, cancellationToken);
        var cache = await client.GetCacheAsync<string>("default", cancellationToken);

        var ex = await NodeAsyncAssert.ThrowsAsync<RpcException>(cache.SetAsync("jwt-missing", "v", cancellationToken: cancellationToken));
        _ = await Assert.That(ex.StatusCode).IsEqualTo(StatusCode.Unauthenticated);
    }

    private static Func<CancellationToken, ValueTask<string>> CreateBearerTokenProvider(string token) => new FixedBearerTokenProvider(token).ProvideAsync;
}
