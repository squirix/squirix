using System.Threading;
using System.Threading.Tasks;
using Grpc.Core;
using Squirix.Server.IntegrationTests.Support;
using Squirix.Server.TestKit;
using Squirix.Transport.Grpc.Cache;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.IntegrationTests.Security;

/// <summary>Verifies gRPC cache authentication when JWT is enabled.</summary>
public sealed class GrpcAuthParityTests : NodeIntegrationTestBase
{
    private const string NodeId = "node-grpc-parity";

    /// <summary>Verifies gRPC rejects requests authenticated with an invalid JWT bearer token.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task GrpcInvalidJwtIsRejected(CancellationToken cancellationToken)
    {
        var credentials = TestJwtHelper.CreateRandomCredentials("https://integration.squirix.test", "grpc-cache");
        var uri = GetNextHttpUri();
        await using var node = await StartNodeAsync(uri, NodeId, new NodeStartOptions { Security = TestJwtHelper.ToSecurityOptions(credentials) }, cancellationToken);

        using var channel = CreateGrpcChannel(uri);
        var client = new SquirixCacheService.SquirixCacheServiceClient(channel);

        var headers = new Metadata { { "authorization", "Bearer invalid.jwt.token" } };
        var ex = await NodeAsyncAssert.ThrowsAsync<RpcException>(
            client.GetEntryAsync(new GetEntryAsyncRequest { CacheName = "default", Key = "grpc-jwt-bad" }, new CallOptions(headers, cancellationToken: cancellationToken))
                  .ResponseAsync);
        _ = await Assert.That(ex.StatusCode).IsEqualTo(StatusCode.Unauthenticated);
    }

    /// <summary>Verifies gRPC rejects requests without credentials when JWT auth is enabled.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task GrpcMissingAuthIsRejectedWhenJwtEnabled(CancellationToken cancellationToken)
    {
        var credentials = TestJwtHelper.CreateRandomCredentials();
        var uri = GetNextHttpUri();
        await using var node = await StartNodeAsync(uri, NodeId, new NodeStartOptions { Security = TestJwtHelper.ToSecurityOptions(credentials) }, cancellationToken);

        using var channel = CreateGrpcChannel(uri);
        var client = new SquirixCacheService.SquirixCacheServiceClient(channel);

        var ex = await NodeAsyncAssert.ThrowsAsync<RpcException>(
            client.GetEntryAsync(new GetEntryAsyncRequest { CacheName = "default", Key = "grpc-auth-missing" }, cancellationToken: cancellationToken).ResponseAsync);
        _ = await Assert.That(ex.StatusCode).IsEqualTo(StatusCode.Unauthenticated);
    }

    /// <summary>Verifies gRPC accepts requests authenticated with a valid JWT bearer token.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task GrpcValidJwtSucceeds(CancellationToken cancellationToken)
    {
        var credentials = TestJwtHelper.CreateRandomCredentials("https://integration.squirix.test", "grpc-cache");
        var uri = GetNextHttpUri();
        await using var node = await StartNodeAsync(uri, NodeId, new NodeStartOptions { Security = TestJwtHelper.ToSecurityOptions(credentials) }, cancellationToken);

        using var channel = CreateGrpcChannel(uri);
        var client = new SquirixCacheService.SquirixCacheServiceClient(channel);

        var headers = new Metadata { { "authorization", $"Bearer {TestJwtHelper.CreateBearerToken(credentials)}" } };
        var response = await client.GetValueAsync(
            new GetValueAsyncRequest { Key = "grpc-jwt-ok", CacheName = "default" },
            new CallOptions(headers, cancellationToken: cancellationToken));
        _ = await Assert.That(response.Found).IsFalse();
    }
}
