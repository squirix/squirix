using System;
using System.IdentityModel.Tokens.Jwt;
using System.Security.Cryptography;
using System.Threading.Tasks;
using Grpc.Core;
using Microsoft.IdentityModel.Tokens;
using Squirix.Server.Cluster.Membership;
using Squirix.Server.TestKit.AspNetCore;
using Squirix.Transport.Grpc.Cache;
using Xunit;

namespace Squirix.Server.IntegrationTests.Security;

/// <summary>
/// Verifies gRPC authentication parity with REST/admin when ApiOrJwt is enabled.
/// </summary>
public sealed class GrpcAuthParityTests : IntegrationTestBase
{
    /// <summary>
    /// Verifies gRPC rejects requests with an invalid API key.
    /// </summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Fact]
    public async Task GrpcInvalidApiKeyIsRejected()
    {
        var url = GetNextHttpUrl();
        var peers = new[] { new Peer { NodeId = Guid.NewGuid().ToString("N"), Url = url } };
        await using var node = await StartNodeAsync(url, peers, security: new TestNodeSecurityOptions { ApiKeys = ["grpc-secret"] });

        using var channel = CreateGrpcChannel(url);
        var client = new SquirixCacheService.SquirixCacheServiceClient(channel);

        var headers = new Metadata { { "x-api-key", "invalid" } };
        var ex = await Assert.ThrowsAsync<RpcException>(async () =>
        {
            _ = await client.GetAsync(
                new GetRequest { CacheName = "default", Key = "grpc-auth-bad-key" },
                new CallOptions(headers, cancellationToken: DefaultCancellationToken));
        });
        Assert.Equal(StatusCode.Unauthenticated, ex.StatusCode);
    }

    /// <summary>
    /// Verifies gRPC rejects requests authenticated with an invalid JWT bearer token.
    /// </summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Fact]
    public async Task GrpcInvalidJwtIsRejected()
    {
        var keyBytes = RandomNumberGenerator.GetBytes(32);
        var base64Key = Convert.ToBase64String(keyBytes);
        const string issuer = "https://integration.squirix.test";
        const string audience = "grpc-cache";

        var url = GetNextHttpUrl();
        var peers = new[] { new Peer { NodeId = Guid.NewGuid().ToString("N"), Url = url } };
        await using var node = await StartNodeAsync(
            url,
            peers,
            security: new TestNodeSecurityOptions
            {
                JwtSigningKey = base64Key,
                JwtIssuer = issuer,
                JwtAudience = audience,
            });

        using var channel = CreateGrpcChannel(url);
        var client = new SquirixCacheService.SquirixCacheServiceClient(channel);

        var headers = new Metadata { { "authorization", "Bearer invalid.jwt.token" } };
        var ex = await Assert.ThrowsAsync<RpcException>(async () =>
        {
            _ = await client.GetAsync(new GetRequest { CacheName = "default", Key = "grpc-jwt-bad" }, new CallOptions(headers, cancellationToken: DefaultCancellationToken));
        });
        Assert.Equal(StatusCode.Unauthenticated, ex.StatusCode);
    }

    /// <summary>
    /// Verifies gRPC rejects requests without credentials when API key auth is enabled.
    /// </summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Fact]
    public async Task GrpcMissingAuthIsRejectedWhenApiKeyEnabled()
    {
        var url = GetNextHttpUrl();
        var peers = new[] { new Peer { NodeId = Guid.NewGuid().ToString("N"), Url = url } };
        await using var node = await StartNodeAsync(url, peers, security: new TestNodeSecurityOptions { ApiKeys = ["grpc-secret"] });

        using var channel = CreateGrpcChannel(url);
        var client = new SquirixCacheService.SquirixCacheServiceClient(channel);

        var ex = await Assert.ThrowsAsync<RpcException>(async () =>
        {
            _ = await client.GetAsync(new GetRequest { CacheName = "default", Key = "grpc-auth-missing" }, cancellationToken: DefaultCancellationToken);
        });
        Assert.Equal(StatusCode.Unauthenticated, ex.StatusCode);
    }

    /// <summary>
    /// Verifies gRPC accepts requests authenticated with a valid API key.
    /// </summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Fact]
    public async Task GrpcValidApiKeySucceeds()
    {
        var url = GetNextHttpUrl();
        var peers = new[] { new Peer { NodeId = Guid.NewGuid().ToString("N"), Url = url } };
        await using var node = await StartNodeAsync(url, peers, security: new TestNodeSecurityOptions { ApiKeys = ["grpc-secret"] });

        using var channel = CreateGrpcChannel(url);
        var client = new SquirixCacheService.SquirixCacheServiceClient(channel);

        var headers = new Metadata { { "x-api-key", "grpc-secret" } };
        var response = await client.ContainsAsync(
            new ContainsRequest { Key = "grpc-auth-ok", CacheName = "default" },
            new CallOptions(headers, cancellationToken: DefaultCancellationToken));
        Assert.False(response.Exists);
    }

    /// <summary>
    /// Verifies gRPC accepts requests authenticated with a valid JWT bearer token.
    /// </summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Fact]
    public async Task GrpcValidJwtSucceeds()
    {
        var keyBytes = RandomNumberGenerator.GetBytes(32);
        var base64Key = Convert.ToBase64String(keyBytes);
        const string issuer = "https://integration.squirix.test";
        const string audience = "grpc-cache";

        var url = GetNextHttpUrl();
        var peers = new[] { new Peer { NodeId = Guid.NewGuid().ToString("N"), Url = url } };
        await using var node = await StartNodeAsync(
            url,
            peers,
            security: new TestNodeSecurityOptions
            {
                JwtSigningKey = base64Key,
                JwtIssuer = issuer,
                JwtAudience = audience,
            });

        using var channel = CreateGrpcChannel(url);
        var client = new SquirixCacheService.SquirixCacheServiceClient(channel);

        var headers = new Metadata { { "authorization", $"Bearer {CreateJwt(keyBytes, issuer, audience)}" } };
        var response = await client.ContainsAsync(
            new ContainsRequest { Key = "grpc-jwt-ok", CacheName = "default" },
            new CallOptions(headers, cancellationToken: DefaultCancellationToken));
        Assert.False(response.Exists);
    }

    private static string CreateJwt(byte[] signingKey, string issuer, string audience)
    {
        var credentials = new SigningCredentials(new SymmetricSecurityKey(signingKey), SecurityAlgorithms.HmacSha256);
        var now = DateTime.UtcNow;
        var token = new JwtSecurityToken(issuer, audience, notBefore: now.AddMinutes(-1), expires: now.AddMinutes(5), signingCredentials: credentials);
        return new JwtSecurityTokenHandler().WriteToken(token);
    }
}
