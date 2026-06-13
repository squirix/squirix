using System;
using System.Threading.Tasks;
using Grpc.Core;
using Squirix.Server.Cluster.Membership;
using Squirix.Server.TestKit.Security;
using Squirix.Transport.Grpc.Cache;
using Xunit;

namespace Squirix.Server.IntegrationTests.Security;

/// <summary>
/// Verifies JWT authentication when the node is configured with an OIDC authority URL.
/// </summary>
public sealed class OidcJwtAuthIntegrationTests : IntegrationTestBase
{
    private const string Audience = "squirix-oidc-integration";

    /// <summary>Verifies startup fails when an OIDC authority is configured without an audience.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Fact]
    public async Task AuthorityWithoutAudienceFailsStartupOnLoopback()
    {
        await using var authority = await MockOidcAuthority.StartAsync(DefaultCancellationToken);
        var url = GetNextHttpAddress();
        var peers = new[] { new Peer { NodeId = Guid.NewGuid().ToString("N"), Url = url } };
        var ex = await Assert.ThrowsAsync<InvalidOperationException>(async () => await StartNodeAsync(url, peers, security: authority.ToSecurityOptionsWithoutAudience()));
        Assert.Contains("SQUIRIX_JWT_AUTHORITY requires SQUIRIX_JWT_AUDIENCE", ex.Message, StringComparison.Ordinal);
    }

    /// <summary>Verifies gRPC rejects expired bearer tokens from the mock authority.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Fact]
    public async Task GrpcExpiredOidcJwtIsRejected()
    {
        await using var authority = await MockOidcAuthority.StartAsync(DefaultCancellationToken);
        var url = GetNextHttpAddress();
        var peers = new[] { new Peer { NodeId = Guid.NewGuid().ToString("N"), Url = url } };
        await using var node = await StartNodeAsync(url, peers, security: authority.ToSecurityOptions(Audience));

        using var channel = CreateGrpcChannel(new Uri(url, UriKind.Absolute));
        var client = new SquirixCacheService.SquirixCacheServiceClient(channel);
        var token = authority.CreateBearerToken(Audience, TimeSpan.FromMinutes(-10));
        var headers = new Metadata { { "authorization", $"Bearer {token}" } };

        var ex = await Assert.ThrowsAsync<RpcException>(async () =>
        {
            _ = await client.GetValueAsync(
                new GetValueRequest { CacheName = "default", Key = "oidc-expired" },
                new CallOptions(headers, cancellationToken: DefaultCancellationToken));
        });

        Assert.Equal(StatusCode.Unauthenticated, ex.StatusCode);
    }

    /// <summary>Verifies gRPC rejects malformed bearer tokens when OIDC JWT auth is enabled.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Fact]
    public async Task GrpcInvalidOidcJwtIsRejected()
    {
        await using var authority = await MockOidcAuthority.StartAsync(DefaultCancellationToken);
        var url = GetNextHttpAddress();
        var peers = new[] { new Peer { NodeId = Guid.NewGuid().ToString("N"), Url = url } };
        await using var node = await StartNodeAsync(url, peers, security: authority.ToSecurityOptions(Audience));

        using var channel = CreateGrpcChannel(new Uri(url, UriKind.Absolute));
        var client = new SquirixCacheService.SquirixCacheServiceClient(channel);
        var headers = new Metadata { { "authorization", "Bearer invalid.jwt.token" } };

        var ex = await Assert.ThrowsAsync<RpcException>(async () =>
        {
            _ = await client.GetValueAsync(
                new GetValueRequest { CacheName = "default", Key = "oidc-invalid" },
                new CallOptions(headers, cancellationToken: DefaultCancellationToken));
        });

        Assert.Equal(StatusCode.Unauthenticated, ex.StatusCode);
    }

    /// <summary>Verifies gRPC rejects requests without credentials when OIDC JWT auth is enabled.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Fact]
    public async Task GrpcMissingOidcJwtIsRejected()
    {
        await using var authority = await MockOidcAuthority.StartAsync(DefaultCancellationToken);
        var url = GetNextHttpAddress();
        var peers = new[] { new Peer { NodeId = Guid.NewGuid().ToString("N"), Url = url } };
        await using var node = await StartNodeAsync(url, peers, security: authority.ToSecurityOptions(Audience));

        using var channel = CreateGrpcChannel(new Uri(url, UriKind.Absolute));
        var client = new SquirixCacheService.SquirixCacheServiceClient(channel);

        var ex = await Assert.ThrowsAsync<RpcException>(async () =>
        {
            _ = await client.GetValueAsync(new GetValueRequest { CacheName = "default", Key = "oidc-missing" }, cancellationToken: DefaultCancellationToken);
        });

        Assert.Equal(StatusCode.Unauthenticated, ex.StatusCode);
    }

    /// <summary>Verifies gRPC accepts a bearer token signed by the mock authority's JWKS.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Fact]
    public async Task GrpcValidOidcJwtSucceeds()
    {
        await using var authority = await MockOidcAuthority.StartAsync(DefaultCancellationToken);
        var url = GetNextHttpAddress();
        var peers = new[] { new Peer { NodeId = Guid.NewGuid().ToString("N"), Url = url } };
        await using var node = await StartNodeAsync(url, peers, security: authority.ToSecurityOptions(Audience));

        using var channel = CreateGrpcChannel(new Uri(url, UriKind.Absolute));
        var client = new SquirixCacheService.SquirixCacheServiceClient(channel);
        var headers = new Metadata { { "authorization", $"Bearer {authority.CreateBearerToken(Audience)}" } };

        var response = await client.GetValueAsync(
            new GetValueRequest { CacheName = "default", Key = "oidc-jwt-ok" },
            new CallOptions(headers, cancellationToken: DefaultCancellationToken));

        Assert.False(response.Found);
    }

    /// <summary>Verifies gRPC rejects bearer tokens with an unexpected audience claim.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Fact]
    public async Task GrpcWrongAudienceOidcJwtIsRejected()
    {
        await using var authority = await MockOidcAuthority.StartAsync(DefaultCancellationToken);
        var url = GetNextHttpAddress();
        var peers = new[] { new Peer { NodeId = Guid.NewGuid().ToString("N"), Url = url } };
        await using var node = await StartNodeAsync(url, peers, security: authority.ToSecurityOptions(Audience));

        using var channel = CreateGrpcChannel(new Uri(url, UriKind.Absolute));
        var client = new SquirixCacheService.SquirixCacheServiceClient(channel);
        var headers = new Metadata { { "authorization", $"Bearer {authority.CreateBearerToken("wrong-audience")}" } };

        var ex = await Assert.ThrowsAsync<RpcException>(async () =>
        {
            _ = await client.GetValueAsync(
                new GetValueRequest { CacheName = "default", Key = "oidc-audience" },
                new CallOptions(headers, cancellationToken: DefaultCancellationToken));
        });

        Assert.Equal(StatusCode.Unauthenticated, ex.StatusCode);
    }
}
