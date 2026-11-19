using System;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.IntegrationTests.Support;
using Squirix.Server.TestKit;
using Squirix.Server.TestKit.Hosting;
using Squirix.Server.TestKit.Networking;
using Squirix.Transport.Binary;
using Squirix.Transport.Wire;
using Squirix.Transport.Wire.Cache;
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
        var options = new IntegrationStartOptions
        {
            Security = new TestNodeSecurityOptions { JwtAuthority = "https://authority.invalid" },
        };
        var ex = await NodeAsyncAssert.ThrowsAsync<InvalidOperationException, TestCluster<IntegrationStartOptions>>(StartClusterAsync(NodeId, options, cancellationToken), TimeSpan.FromSeconds(30), TimeProvider.System, cancellationToken);
        _ = await Assert.That(ex.Message).Contains("SQUIRIX_JWT_AUTHORITY requires SQUIRIX_JWT_AUDIENCE", StringComparison.Ordinal);
    }

    /// <summary>Verifies gRPC rejects expired, malformed, and wrong-audience OIDC tokens.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task GrpcRejectsInvalidOidcTokens(CancellationToken cancellationToken)
    {
        await using var authority = await MockOidcAuthority.StartAsync(cancellationToken);
        await AssertRejectsThenAcceptsAsync(authority, "oidc-expired", authority.CreateBearerToken(Audience, TimeSpan.FromMinutes(-10)), cancellationToken);
        await AssertRejectsThenAcceptsAsync(authority, "oidc-invalid", "invalid.jwt.token", cancellationToken);
        await AssertRejectsThenAcceptsAsync(authority, "oidc-audience", authority.CreateBearerToken("wrong-audience"), cancellationToken);
    }

    private async Task AssertRejectsThenAcceptsAsync(MockOidcAuthority authority, string key, string token, CancellationToken cancellationToken)
    {
        await using var cluster = await StartClusterAsync(NodeId, new IntegrationStartOptions { Security = authority.ToSecurityOptions(Audience) }, cancellationToken);

        var rejectedStatus = byte.MaxValue;
        using var channel = CreateBinaryChannel(cluster[NodeId].Uri, token, status => rejectedStatus = status);
        var client = new CacheWireClient(channel);
        var request = new GetValueAsyncRequest { CacheName = "default", Key = key };
        var exception = await NodeAsyncAssert.ThrowsAsync<ServerCallException>(
            client.GetValueAsync(request, cancellationToken: cancellationToken),
            TimeSpan.FromSeconds(30),
            TimeProvider.System,
            cancellationToken);
        _ = await Assert.That(exception.StatusCode).IsEqualTo(FrameStatus.Unavailable).Because(key);
        _ = await Assert.That(rejectedStatus).IsEqualTo(ConnectionAuthenticationProtocol.InvalidCredentials).Because(key);

        using var accepted = CreateBinaryChannel(cluster[NodeId].Uri, authority.CreateBearerToken(Audience));
        var acceptedClient = new CacheWireClient(accepted);
        var response = await acceptedClient.GetValueAsync(request, cancellationToken: cancellationToken);
        _ = await Assert.That(response.Found).IsFalse().Because(key);
    }
}
