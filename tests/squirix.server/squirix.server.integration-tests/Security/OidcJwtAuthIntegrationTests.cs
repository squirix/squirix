using System;
using System.Threading;
using System.Threading.Tasks;
using Grpc.Core;
using Squirix.Server.IntegrationTests.Support;
using Squirix.Server.TestKit;
using Squirix.Server.TestKit.Hosting;
using Squirix.Transport.Grpc.Cache;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.IntegrationTests.Security;

/// <summary>Verifies JWT authentication when the node is configured with an OIDC authority URL.</summary>
public sealed class OidcJwtAuthIntegrationTests : NodeIntegrationTestBase
{
    private const string Audience = "squirix-oidc-integration";
    private const string NodeId = "node-oidc-auth";

    /// <summary>Verifies startup fails when an OIDC authority is configured without an audience.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task AuthorityWithoutAudienceFailsStartup(CancellationToken cancellationToken)
    {
        await using var authority = await MockOidcAuthority.StartAsync(cancellationToken);
        var ex = await NodeAsyncAssert.ThrowsAsync<InvalidOperationException, TestCluster<IntegrationStartOptions>>(
            StartClusterAsync(NodeId, new IntegrationStartOptions { Security = authority.ToSecurityOptionsWithoutAudience() }, cancellationToken));
        _ = await Assert.That(ex.Message).Contains("SQUIRIX_JWT_AUTHORITY requires SQUIRIX_JWT_AUDIENCE", StringComparison.Ordinal);
    }

    /// <summary>Verifies gRPC rejects expired bearer tokens from the mock authority.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task GrpcExpiredOidcJwtIsRejected(CancellationToken cancellationToken)
    {
        await using var authority = await MockOidcAuthority.StartAsync(cancellationToken);
        await using var cluster = await StartClusterAsync(NodeId, new IntegrationStartOptions { Security = authority.ToSecurityOptions(Audience) }, cancellationToken);

        using var channel = CreateGrpcChannel(cluster[NodeId].Uri);
        var client = new SquirixCacheService.SquirixCacheServiceClient(channel);
        var token = authority.CreateBearerToken(Audience, TimeSpan.FromMinutes(-10));
        var headers = new Metadata { { "authorization", $"Bearer {token}" } };

        var req = new GetValueAsyncRequest { CacheName = "default", Key = "oidc-expired" };
        var ex = await NodeAsyncAssert.ThrowsAsync<RpcException>(client.GetValueAsync(req, new CallOptions(headers, cancellationToken: cancellationToken)).ResponseAsync);

        _ = await Assert.That(ex.StatusCode).IsEqualTo(StatusCode.Unauthenticated);
    }

    /// <summary>Verifies gRPC rejects malformed bearer tokens when OIDC JWT auth is enabled.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task GrpcInvalidOidcJwtIsRejected(CancellationToken cancellationToken)
    {
        await using var authority = await MockOidcAuthority.StartAsync(cancellationToken);
        await using var cluster = await StartClusterAsync(NodeId, new IntegrationStartOptions { Security = authority.ToSecurityOptions(Audience) }, cancellationToken);

        using var channel = CreateGrpcChannel(cluster[NodeId].Uri);
        var client = new SquirixCacheService.SquirixCacheServiceClient(channel);
        var headers = new Metadata { { "authorization", "Bearer invalid.jwt.token" } };

        var ex = await NodeAsyncAssert.ThrowsAsync<RpcException>(
            client.GetValueAsync(new GetValueAsyncRequest { CacheName = "default", Key = "oidc-invalid" }, new CallOptions(headers, cancellationToken: cancellationToken))
                  .ResponseAsync);

        _ = await Assert.That(ex.StatusCode).IsEqualTo(StatusCode.Unauthenticated);
    }

    /// <summary>Verifies gRPC rejects requests without credentials when OIDC JWT auth is enabled.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task GrpcMissingOidcJwtIsRejected(CancellationToken cancellationToken)
    {
        await using var authority = await MockOidcAuthority.StartAsync(cancellationToken);
        await using var cluster = await StartClusterAsync(NodeId, new IntegrationStartOptions { Security = authority.ToSecurityOptions(Audience) }, cancellationToken);

        using var channel = CreateGrpcChannel(cluster[NodeId].Uri);
        var client = new SquirixCacheService.SquirixCacheServiceClient(channel);

        var ex = await NodeAsyncAssert.ThrowsAsync<RpcException>(
            client.GetValueAsync(new GetValueAsyncRequest { CacheName = "default", Key = "oidc-missing" }, cancellationToken: cancellationToken).ResponseAsync);

        _ = await Assert.That(ex.StatusCode).IsEqualTo(StatusCode.Unauthenticated);
    }

    /// <summary>Verifies gRPC accepts a bearer token signed by the mock authority's JWKS.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task GrpcValidOidcJwtSucceeds(CancellationToken cancellationToken)
    {
        await using var authority = await MockOidcAuthority.StartAsync(cancellationToken);
        await using var cluster = await StartClusterAsync(NodeId, new IntegrationStartOptions { Security = authority.ToSecurityOptions(Audience) }, cancellationToken);

        using var channel = CreateGrpcChannel(cluster[NodeId].Uri);
        var client = new SquirixCacheService.SquirixCacheServiceClient(channel);
        var headers = new Metadata { { "authorization", $"Bearer {authority.CreateBearerToken(Audience)}" } };

        var response = await client.GetValueAsync(
            new GetValueAsyncRequest { CacheName = "default", Key = "oidc-jwt-ok" },
            new CallOptions(headers, cancellationToken: cancellationToken));

        _ = await Assert.That(response.Found).IsFalse();
    }

    /// <summary>Verifies gRPC rejects bearer tokens with an unexpected audience claim.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task GrpcWrongAudienceOidcJwtIsRejected(CancellationToken cancellationToken)
    {
        await using var authority = await MockOidcAuthority.StartAsync(cancellationToken);
        await using var cluster = await StartClusterAsync(NodeId, new IntegrationStartOptions { Security = authority.ToSecurityOptions(Audience) }, cancellationToken);

        using var channel = CreateGrpcChannel(cluster[NodeId].Uri);
        var client = new SquirixCacheService.SquirixCacheServiceClient(channel);
        var headers = new Metadata { { "authorization", $"Bearer {authority.CreateBearerToken("wrong-audience")}" } };

        var ex = await NodeAsyncAssert.ThrowsAsync<RpcException>(
            client.GetValueAsync(new GetValueAsyncRequest { CacheName = "default", Key = "oidc-audience" }, new CallOptions(headers, cancellationToken: cancellationToken))
                  .ResponseAsync);

        _ = await Assert.That(ex.StatusCode).IsEqualTo(StatusCode.Unauthenticated);
    }
}
