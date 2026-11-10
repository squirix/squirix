using System;
using System.Net;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.TestKit;
using Squirix.Server.TestKit.Hosting;
using Squirix.Server.TestKit.Networking;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.IntegrationTests.Surfaces;

/// <summary>Endpoint integration tests for JWT authentication on operational HTTP endpoints.</summary>
public sealed class OperationalEndpointAuthTests : ServerSurfaceTestBase
{
    private const string InvalidBearerToken = "invalid.jwt.token";
    private static readonly SocketsHttpHandler RemoteHandler = LoopbackHttp.CreateHandlerAllowingCertNameMismatch();
    private static readonly HttpClient RemoteClient = new(RemoteHandler, false);

    /// <summary>Verifies loopback-anonymous and remote-JWT rules when server auth is configured.</summary>
    /// <param name="path">The endpoint path.</param>
    /// <param name="nodeId">The identifier for the test node.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    [Arguments("/metrics", "node-metrics-auth")]
    [Arguments("/health/ready/details", "node-ready-details-auth")]
    public async Task EndpointValidatesJwt(string path, string nodeId, CancellationToken cancellationToken)
    {
        var localIp = LocalHostNetworking.GetLocalNonLoopbackIpv4();
        _ = await Assert.That(string.IsNullOrWhiteSpace(localIp)).IsFalse();

        var credentials = TestJwtHelper.CreateRandomCredentials();
        using var held = ListenPortPool.IntegrationTests.HoldPort();
        var uri = new Uri(NodeInvariantIndexStrings.FormatHttpsOrigin("0.0.0.0", held.Port), UriKind.Absolute);
        var remoteUrl = NodeInvariantIndexStrings.FormatHttpsAbsolute(localIp!, held.Port, path);
        var loopbackUrl = NodeInvariantIndexStrings.FormatHttpsAbsolute("127.0.0.1", held.Port, path);

        await using var cluster = await StartClusterAsync(
            new ClusterNode(nodeId, uri),
            _ => new BlackBoxStartOptions { Security = TestNodeSecurityOptions.FromJwtCredentials(credentials) },
            cancellationToken);

        var loopbackAnonymous = await HttpClient.GetAsync(new Uri(loopbackUrl), cancellationToken);
        _ = await Assert.That(loopbackAnonymous.StatusCode).IsEqualTo(HttpStatusCode.OK);

        using (var loopbackAuthorized = new HttpRequestMessage(HttpMethod.Get, loopbackUrl))
        {
            loopbackAuthorized.Version = HttpVersion.Version20;
            loopbackAuthorized.VersionPolicy = HttpVersionPolicy.RequestVersionExact;
            loopbackAuthorized.Headers.Authorization = new AuthenticationHeaderValue("Bearer", TestJwtHelper.CreateBearerToken(credentials));
            var loopbackWithJwt = await HttpClient.SendAsync(loopbackAuthorized, cancellationToken);
            _ = await Assert.That(loopbackWithJwt.StatusCode).IsEqualTo(HttpStatusCode.OK);
        }

        var remoteAnonymous = await RemoteClient.GetAsync(new Uri(remoteUrl), cancellationToken);
        _ = await Assert.That(remoteAnonymous.StatusCode).IsEqualTo(HttpStatusCode.Unauthorized);

        using (var remoteInvalid = new HttpRequestMessage(HttpMethod.Get, remoteUrl))
        {
            remoteInvalid.Headers.Authorization = new AuthenticationHeaderValue("Bearer", InvalidBearerToken);
            var remoteInvalidJwt = await RemoteClient.SendAsync(remoteInvalid, cancellationToken);
            _ = await Assert.That(remoteInvalidJwt.StatusCode).IsEqualTo(HttpStatusCode.Unauthorized);
        }

        using var remoteValid = new HttpRequestMessage(HttpMethod.Get, remoteUrl);
        remoteValid.Headers.Authorization = new AuthenticationHeaderValue("Bearer", TestJwtHelper.CreateBearerToken(credentials));
        var remoteWithJwt = await RemoteClient.SendAsync(remoteValid, cancellationToken);
        _ = await Assert.That(remoteWithJwt.StatusCode).IsEqualTo(HttpStatusCode.OK);
    }
}
