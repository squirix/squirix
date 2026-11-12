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

        using var channel = CreateGrpcChannel(cluster[NodeId].Uri);
        var client = new SquirixCacheService.SquirixCacheServiceClient(channel);
        var headers = new Metadata { { "authorization", $"Bearer {token}" } };
        var request = new GetValueAsyncRequest { CacheName = "default", Key = key };
        var exception = await NodeAsyncAssert.ThrowsAsync<RpcException>(
            client.GetValueAsync(request, new CallOptions(headers, cancellationToken: cancellationToken)).ResponseAsync,
            TimeSpan.FromSeconds(30),
            TimeProvider.System,
            cancellationToken);
        _ = await Assert.That(exception.StatusCode).IsEqualTo(StatusCode.Unauthenticated).Because(key);

        var validHeaders = new Metadata { { "authorization", $"Bearer {authority.CreateBearerToken(Audience)}" } };
        var response = await client.GetValueAsync(request, new CallOptions(validHeaders, cancellationToken: cancellationToken));
        _ = await Assert.That(response.Found).IsFalse().Because(key);
    }
}
