using System;
using System.Threading;
using System.Threading.Tasks;
using Grpc.Core;
using Squirix.Server.TestKit;
using Squirix.Server.TestKit.Hosting;
using Squirix.Transport.Grpc;
using Squirix.Transport.Grpc.Cache;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.IntegrationTests.Surfaces;

/// <summary>Endpoint integration tests verifying JWT auth on binary cache calls when credentials are configured.</summary>
public sealed class AuthTests : ServerSurfaceTestBase
{
    private const string InvalidBearerToken = "invalid.jwt.token";

    /// <summary>Ensures binary cache calls reject missing and invalid JWT credentials and accept a valid bearer token.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task CacheRpcValidatesJwtWhenConfigured(CancellationToken cancellationToken)
    {
        var credentials = TestJwtHelper.CreateRandomCredentials("https://smoke.squirix.test", "smoke-grpc");

        await using var cluster = await StartClusterAsync(
            "node-grpc-auth",
            _ => new BlackBoxStartOptions { Security = TestNodeSecurityOptions.FromJwtCredentials(credentials) },
            cancellationToken);
        var uri = cluster["node-grpc-auth"].Uri;

        using var channel = CreateBinaryChannel(uri);
        var client = new SquirixCacheService.SquirixCacheServiceClient(channel);
        var getRequest = new GetEntryAsyncRequest { CacheName = "default", Key = "grpc-auth-smoke" };

        var missingAuth = await NodeAsyncAssert.ThrowsAsync<RpcException>(client.GetEntryAsync(getRequest, cancellationToken: cancellationToken).ResponseAsync, TimeSpan.FromSeconds(30), TimeProvider.System, cancellationToken);
        _ = await Assert.That(missingAuth.StatusCode).IsEqualTo(StatusCode.Unauthenticated).Because(missingAuth.Message);

        var token = TestJwtHelper.CreateBearerToken(credentials);
        var rejectedStatus = byte.MaxValue;
        using var invalidChannel = CreateBinaryChannel(uri, InvalidBearerToken, status => rejectedStatus = status);
        var invalidClient = new SquirixCacheService.SquirixCacheServiceClient(invalidChannel);
        var invalidAuth = await NodeAsyncAssert.ThrowsAsync<RpcException>(invalidClient.GetEntryAsync(getRequest, cancellationToken: cancellationToken).ResponseAsync, TimeSpan.FromSeconds(30), TimeProvider.System, cancellationToken);
        _ = await Assert.That(invalidAuth.StatusCode).IsEqualTo(StatusCode.Unavailable);
        _ = await Assert.That(rejectedStatus).IsEqualTo(ConnectionAuthenticationProtocol.InvalidCredentials);

        var acceptedHandshakes = 0;
        using var authenticated = CreateBinaryChannel(uri, token, status =>
        {
            if (status == ConnectionAuthenticationProtocol.Accepted)
                _ = Interlocked.Increment(ref acceptedHandshakes);
        });
        var authenticatedClient = new SquirixCacheService.SquirixCacheServiceClient(authenticated);
        var response = await authenticatedClient.GetValueAsync(new GetValueAsyncRequest { CacheName = "default", Key = "grpc-auth-smoke" }, cancellationToken: cancellationToken);
        var repeated = await authenticatedClient.GetValueAsync(new GetValueAsyncRequest { CacheName = "default", Key = "grpc-auth-smoke" }, cancellationToken: cancellationToken);
        _ = await Assert.That(response.Found).IsFalse();
        _ = await Assert.That(repeated.Found).IsFalse();
        _ = await Assert.That(acceptedHandshakes).IsEqualTo(1);
    }
}
