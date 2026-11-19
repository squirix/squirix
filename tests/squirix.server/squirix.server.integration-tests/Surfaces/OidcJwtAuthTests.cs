using System;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.TestKit;
using Squirix.Server.TestKit.Hosting;
using Squirix.Server.TestKit.Networking;
using Squirix.Transport.Binary;
using Squirix.Transport.Grpc;
using Squirix.Transport.Grpc.Cache;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.IntegrationTests.Surfaces;

/// <summary>Thin smoke coverage for OIDC authority JWT authentication on gRPC cache RPCs.</summary>
public sealed class OidcJwtAuthTests : ServerSurfaceTestBase
{
    private const string Audience = "squirix-oidc-smoke";

    /// <summary>Ensures gRPC cache RPCs accept a valid OIDC bearer token and reject missing credentials.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task CacheRpcEnforcesOidcJwtAuth(CancellationToken cancellationToken)
    {
        await using var authority = await MockOidcAuthority.StartAsync(cancellationToken);

        await using var cluster = await StartClusterAsync(
            "node-oidc-auth",
            _ => new BlackBoxStartOptions { Security = authority.ToSecurityOptions(Audience) },
            cancellationToken);
        var uri = cluster["node-oidc-auth"].Uri;

        using var channel = CreateBinaryChannel(uri);
        var client = new CacheWireClient(channel);
        var request = new GetValueAsyncRequest { CacheName = "default", Key = "oidc-smoke" };

        var missingAuth = await NodeAsyncAssert.ThrowsAsync<ServerCallException>(client.GetValueAsync(request, cancellationToken: cancellationToken), TimeSpan.FromSeconds(30), TimeProvider.System, cancellationToken);
        _ = await Assert.That(missingAuth.StatusCode).IsEqualTo(FrameStatus.Unauthenticated);

        using var authenticated = CreateBinaryChannel(uri, authority.CreateBearerToken(Audience));
        var authenticatedClient = new CacheWireClient(authenticated);
        var response = await authenticatedClient.GetValueAsync(request, cancellationToken: cancellationToken);
        _ = await Assert.That(response.Found).IsFalse();
    }

    /// <summary>Explicit issuer trust combines with authority keys while unrelated issuers and signatures remain denied.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task AuthorityKeysAcceptConfiguredIssuer(CancellationToken cancellationToken)
    {
        const string issuer = "https://explicit-issuer.test";
        await using var authority = await MockOidcAuthority.StartAsync(cancellationToken);
        await using var unrelated = await MockOidcAuthority.StartAsync(cancellationToken);
        await using var cluster = await StartClusterAsync(
            "node-oidc-explicit",
            _ => new BlackBoxStartOptions
            {
                Security = new TestNodeSecurityOptions
                {
                    JwtAuthority = authority.ToSecurityOptions(Audience).JwtAuthority,
                    JwtIssuer = issuer,
                    JwtAudience = Audience,
                    JwtAllowHttpMetadata = true,
                },
            },
            cancellationToken);
        var uri = cluster["node-oidc-explicit"].Uri;
        using var accepted = CreateBinaryChannel(uri, authority.CreateBearerToken(Audience, issuer: issuer));
        var client = new CacheWireClient(accepted);
        var response = await client.GetValueAsync(new GetValueAsyncRequest { CacheName = "default", Key = "explicit-issuer" }, cancellationToken: cancellationToken);
        _ = await Assert.That(response.Found).IsFalse();
        using var metadata = CreateBinaryChannel(uri, authority.CreateBearerToken(Audience));
        var metadataClient = new CacheWireClient(metadata);
        var metadataResponse = await metadataClient.GetValueAsync(new GetValueAsyncRequest { CacheName = "default", Key = "metadata-issuer" }, cancellationToken: cancellationToken);
        _ = await Assert.That(metadataResponse.Found).IsFalse();
        await AssertCredentialRejectedAsync(uri, authority.CreateBearerToken(Audience, issuer: "https://untrusted-issuer.test"), cancellationToken);
        await AssertCredentialRejectedAsync(uri, unrelated.CreateBearerToken(Audience, issuer: issuer), cancellationToken);
    }

    /// <summary>Static signing keys can authenticate the metadata issuer without an explicit issuer setting.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task StaticKeyAcceptsMetadataIssuer(CancellationToken cancellationToken)
    {
        await using var authority = await MockOidcAuthority.StartAsync(cancellationToken);
        var discovered = authority.ToSecurityOptions(Audience);
        var credentials = TestJwtHelper.CreateRandomCredentials(discovered.JwtIssuer!, Audience);
        await using var cluster = await StartClusterAsync(
            "node-oidc-static",
            _ => new BlackBoxStartOptions
            {
                Security = new TestNodeSecurityOptions
                {
                    JwtAuthority = discovered.JwtAuthority,
                    JwtSigningKey = credentials.Base64SigningKey,
                    JwtAudience = Audience,
                    JwtAllowHttpMetadata = true,
                },
            },
            cancellationToken);
        var uri = cluster["node-oidc-static"].Uri;
        using var accepted = CreateBinaryChannel(uri, TestJwtHelper.CreateBearerToken(credentials));
        var client = new CacheWireClient(accepted);
        var response = await client.GetValueAsync(new GetValueAsyncRequest { CacheName = "default", Key = "static-key-metadata" }, cancellationToken: cancellationToken);
        _ = await Assert.That(response.Found).IsFalse();
        var unknownIssuer = new TestJwtCredentials(credentials.GetSigningKey(), "https://untrusted-issuer.test", Audience);
        await AssertCredentialRejectedAsync(uri, TestJwtHelper.CreateBearerToken(unknownIssuer), cancellationToken);
        var wrongKey = TestJwtHelper.CreateRandomCredentials(credentials.Issuer, Audience);
        await AssertCredentialRejectedAsync(uri, TestJwtHelper.CreateBearerToken(wrongKey), cancellationToken);
    }

    private async Task AssertCredentialRejectedAsync(Uri uri, string token, CancellationToken cancellationToken)
    {
        var acknowledgement = byte.MaxValue;
        using var channel = CreateBinaryChannel(uri, token, status => acknowledgement = status);
        var client = new CacheWireClient(channel);
        var exception = await NodeAsyncAssert.ThrowsAsync<ServerCallException>(
            client.GetValueAsync(new GetValueAsyncRequest { CacheName = "default", Key = "denied-issuer" }, cancellationToken: cancellationToken),
            TimeSpan.FromSeconds(30),
            TimeProvider.System,
            cancellationToken);
        _ = await Assert.That(exception.StatusCode).IsEqualTo(FrameStatus.Unavailable);
        _ = await Assert.That(acknowledgement).IsEqualTo(ConnectionAuthenticationProtocol.InvalidCredentials);
    }
}
