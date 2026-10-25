using System;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.IntegrationTests.Support;
using Squirix.Server.TestKit;
using Squirix.Server.TestKit.Hosting;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.IntegrationTests.Security;

/// <summary>Verifies startup validation of symmetric JWT settings.</summary>
public sealed class SymmetricJwtConfigurationTests : NodeIntegrationTestBase
{
    private const string NodeId = "node-symmetric-jwt-config";
    private const int ShortKeyBytes = 16;

    /// <summary>Verifies startup fails when a signing key is configured without an audience.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task SigningKeyWithoutAudienceFailsStartup(CancellationToken cancellationToken)
    {
        var credentials = TestJwtHelper.CreateRandomCredentials();
        var security = new TestNodeSecurityOptions { JwtSigningKey = credentials.Base64SigningKey, JwtIssuer = credentials.Issuer };

        var ex = await NodeAsyncAssert.ThrowsAsync<InvalidOperationException, TestCluster<IntegrationStartOptions>>(
            StartClusterAsync(NodeId, new IntegrationStartOptions { Security = security }, cancellationToken));
        _ = await Assert.That(ex.Message).Contains("SQUIRIX_JWT_SIGNING_KEY requires SQUIRIX_JWT_AUDIENCE", StringComparison.Ordinal);
    }

    /// <summary>Verifies startup fails when the signing key is not base64.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task NonBase64SigningKeyFailsStartup(CancellationToken cancellationToken)
    {
        var security = CreateSecurity("not-base64-passphrase!!");

        var ex = await NodeAsyncAssert.ThrowsAsync<InvalidOperationException, TestCluster<IntegrationStartOptions>>(
            StartClusterAsync(NodeId, new IntegrationStartOptions { Security = security }, cancellationToken));
        _ = await Assert.That(ex.Message).Contains("SQUIRIX_JWT_SIGNING_KEY must be base64", StringComparison.Ordinal);
    }

    /// <summary>Verifies startup fails when the signing key decodes to fewer than 32 bytes.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ShortSigningKeyFailsStartup(CancellationToken cancellationToken)
    {
        var security = CreateSecurity(Convert.ToBase64String(new byte[ShortKeyBytes]));

        var ex = await NodeAsyncAssert.ThrowsAsync<InvalidOperationException, TestCluster<IntegrationStartOptions>>(
            StartClusterAsync(NodeId, new IntegrationStartOptions { Security = security }, cancellationToken));
        _ = await Assert.That(ex.Message).Contains("at least 32 bytes", StringComparison.Ordinal);
    }

    private static TestNodeSecurityOptions CreateSecurity(string signingKey)
    {
        var credentials = TestJwtHelper.CreateRandomCredentials();
        return new TestNodeSecurityOptions { JwtSigningKey = signingKey, JwtIssuer = credentials.Issuer, JwtAudience = credentials.Audience };
    }
}
