using System;
using System.Threading.Tasks;
using Grpc.Core;
using Squirix.Server.IntegrationTests.Support;
using Squirix.Server.TestKit.Auth;
using Squirix.Transport.Grpc.Cache;
using Xunit;

namespace Squirix.Server.IntegrationTests.Security;

/// <summary>Verifies JWT authentication when the node is configured with an OIDC authority URL.</summary>
public sealed class OidcJwtAuthIntegrationTests : IntegrationTestBase
{
    private const string Audience = "squirix-oidc-integration";
    private const string NodeId = "node-oidc-auth";

    /// <summary>Verifies startup fails when an OIDC authority is configured without an audience.</summary>
    [Fact]
    public async Task AuthorityWithoutAudienceFailsStartupOnLoopback()
    {
        await using var authority = await MockOidcAuthority.StartAsync(DefaultCancellationToken);
        var uri = GetNextHttpUri();
        var ex = await Assert.ThrowsAsync<InvalidOperationException>(() =>
            StartNodeAsync(uri, NodeId, security: authority.ToSecurityOptionsWithoutAudience()).AsTask());
        Assert.Contains("SQUIRIX_JWT_AUTHORITY requires SQUIRIX_JWT_AUDIENCE", ex.Message, StringComparison.Ordinal);
    }

    /// <summary>Verifies gRPC rejects expired bearer tokens from the mock authority.</summary>
    [Fact]
    public async Task GrpcExpiredOidcJwtIsRejected()
    {
        await using var authority = await MockOidcAuthority.StartAsync(DefaultCancellationToken);
        var uri = GetNextHttpUri();
        await using var node = await StartNodeAsync(uri, NodeId, security: authority.ToSecurityOptions(Audience));

        using var channel = CreateGrpcChannel(uri);
        var client = new SquirixCacheService.SquirixCacheServiceClient(channel);
        var token = authority.CreateBearerToken(Audience, TimeSpan.FromMinutes(-10));
        var headers = new Metadata { { "authorization", $"Bearer {token}" } };

        var ex = await Assert.ThrowsAsync<RpcException>(() =>
            client.GetValueAsync(
                new GetValueAsyncRequest { CacheName = "default", Key = "oidc-expired" },
                new CallOptions(headers, cancellationToken: DefaultCancellationToken)).ResponseAsync);

        Assert.Equal(StatusCode.Unauthenticated, ex.StatusCode);
    }

    /// <summary>Verifies gRPC rejects malformed bearer tokens when OIDC JWT auth is enabled.</summary>
    [Fact]
    public async Task GrpcInvalidOidcJwtIsRejected()
    {
        await using var authority = await MockOidcAuthority.StartAsync(DefaultCancellationToken);
        var uri = GetNextHttpUri();
        await using var node = await StartNodeAsync(uri, NodeId, security: authority.ToSecurityOptions(Audience));

        using var channel = CreateGrpcChannel(uri);
        var client = new SquirixCacheService.SquirixCacheServiceClient(channel);
        var headers = new Metadata { { "authorization", "Bearer invalid.jwt.token" } };

        var ex = await Assert.ThrowsAsync<RpcException>(() =>
            client.GetValueAsync(
                new GetValueAsyncRequest { CacheName = "default", Key = "oidc-invalid" },
                new CallOptions(headers, cancellationToken: DefaultCancellationToken)).ResponseAsync);

        Assert.Equal(StatusCode.Unauthenticated, ex.StatusCode);
    }

    /// <summary>Verifies gRPC rejects requests without credentials when OIDC JWT auth is enabled.</summary>
    [Fact]
    public async Task GrpcMissingOidcJwtIsRejected()
    {
        await using var authority = await MockOidcAuthority.StartAsync(DefaultCancellationToken);
        var uri = GetNextHttpUri();
        await using var node = await StartNodeAsync(uri, NodeId, security: authority.ToSecurityOptions(Audience));

        using var channel = CreateGrpcChannel(uri);
        var client = new SquirixCacheService.SquirixCacheServiceClient(channel);

        var ex = await Assert.ThrowsAsync<RpcException>(() =>
            client.GetValueAsync(new GetValueAsyncRequest { CacheName = "default", Key = "oidc-missing" }, cancellationToken: DefaultCancellationToken).ResponseAsync);

        Assert.Equal(StatusCode.Unauthenticated, ex.StatusCode);
    }

    /// <summary>Verifies gRPC accepts a bearer token signed by the mock authority's JWKS.</summary>
    [Fact]
    public async Task GrpcValidOidcJwtSucceeds()
    {
        await using var authority = await MockOidcAuthority.StartAsync(DefaultCancellationToken);
        var uri = GetNextHttpUri();
        await using var node = await StartNodeAsync(uri, NodeId, security: authority.ToSecurityOptions(Audience));

        using var channel = CreateGrpcChannel(uri);
        var client = new SquirixCacheService.SquirixCacheServiceClient(channel);
        var headers = new Metadata { { "authorization", $"Bearer {authority.CreateBearerToken(Audience)}" } };

        var response = await client.GetValueAsync(
            new GetValueAsyncRequest { CacheName = "default", Key = "oidc-jwt-ok" },
            new CallOptions(headers, cancellationToken: DefaultCancellationToken));

        Assert.False(response.Found);
    }

    /// <summary>Verifies gRPC rejects bearer tokens with an unexpected audience claim.</summary>
    [Fact]
    public async Task GrpcWrongAudienceOidcJwtIsRejected()
    {
        await using var authority = await MockOidcAuthority.StartAsync(DefaultCancellationToken);
        var uri = GetNextHttpUri();
        await using var node = await StartNodeAsync(uri, NodeId, security: authority.ToSecurityOptions(Audience));

        using var channel = CreateGrpcChannel(uri);
        var client = new SquirixCacheService.SquirixCacheServiceClient(channel);
        var headers = new Metadata { { "authorization", $"Bearer {authority.CreateBearerToken("wrong-audience")}" } };

        var ex = await Assert.ThrowsAsync<RpcException>(() =>
            client.GetValueAsync(
                new GetValueAsyncRequest { CacheName = "default", Key = "oidc-audience" },
                new CallOptions(headers, cancellationToken: DefaultCancellationToken)).ResponseAsync);

        Assert.Equal(StatusCode.Unauthenticated, ex.StatusCode);
    }
}
