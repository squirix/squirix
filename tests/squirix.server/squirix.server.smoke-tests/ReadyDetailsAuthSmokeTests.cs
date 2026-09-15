using System;
using System.Net;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.TestKit;
using Squirix.Server.TestKit.Networking;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.SmokeTests;

/// <summary>Smoke tests verifying JWT auth rules on the <c language="csharp">/health/ready/details</c> endpoint.</summary>
public sealed class ReadyDetailsAuthSmokeTests : SmokeTestBase
{
    private const string InvalidBearerToken = "invalid.jwt.token";
    private static readonly SocketsHttpHandler RemoteHandler = LoopbackHttp.CreateHandlerAllowingCertNameMismatch();
    private static readonly HttpClient RemoteClient = new(RemoteHandler, false);

    /// <summary>Ensures <c language="csharp">/health/ready/details</c> follows loopback-anonymous and remote-JWT rules when server auth is configured.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ReadyDetailsValidatesJwtConfigured(CancellationToken cancellationToken)
    {
        var localIp = LocalHostNetworking.GetLocalNonLoopbackIpv4();
        _ = await Assert.That(string.IsNullOrWhiteSpace(localIp)).IsFalse();

        var credentials = TestJwtHelper.CreateRandomCredentials();
        var (bindUrl, loopbackUrl) = GetNextAnyInterfaceListenUrls();
        var port = new Uri(bindUrl).Port;
        var remoteDetailsUrl = NodeInvariantIndexStrings.FormatHttpsAbsolute(localIp!, port, "/health/ready/details");
        var loopbackDetailsUrl = $"{loopbackUrl}/health/ready/details";

        await using var node = await StartNodeAsync(
            bindUrl,
            "node-ready-details-auth",
            new SmokeNodeStartOptions { Security = TestJwtHelper.ToSecurityOptions(credentials) },
            cancellationToken);

        var loopbackAnonymous = await HttpClient.GetAsync(new Uri(loopbackDetailsUrl), cancellationToken);
        _ = await Assert.That(loopbackAnonymous.IsSuccessStatusCode).IsTrue();

        using (var loopbackAuthorized = new HttpRequestMessage(HttpMethod.Get, loopbackDetailsUrl))
        {
            loopbackAuthorized.Version = HttpVersion.Version20;
            loopbackAuthorized.VersionPolicy = HttpVersionPolicy.RequestVersionExact;
            loopbackAuthorized.Headers.Authorization = new AuthenticationHeaderValue("Bearer", TestJwtHelper.CreateBearerToken(credentials));
            var loopbackWithJwt = await HttpClient.SendAsync(loopbackAuthorized, cancellationToken);
            _ = await Assert.That(loopbackWithJwt.IsSuccessStatusCode).IsTrue();
        }

        var remoteAnonymous = await RemoteClient.GetAsync(new Uri(remoteDetailsUrl), cancellationToken);
        _ = await Assert.That(remoteAnonymous.StatusCode).IsEqualTo(HttpStatusCode.Unauthorized);

        using (var remoteInvalid = new HttpRequestMessage(HttpMethod.Get, remoteDetailsUrl))
        {
            remoteInvalid.Headers.Authorization = new AuthenticationHeaderValue("Bearer", InvalidBearerToken);
            var remoteInvalidJwt = await RemoteClient.SendAsync(remoteInvalid, cancellationToken);
            _ = await Assert.That(remoteInvalidJwt.StatusCode).IsEqualTo(HttpStatusCode.Unauthorized);
        }

        using var remoteValid = new HttpRequestMessage(HttpMethod.Get, remoteDetailsUrl);
        remoteValid.Headers.Authorization = new AuthenticationHeaderValue("Bearer", TestJwtHelper.CreateBearerToken(credentials));
        var remoteWithJwt = await RemoteClient.SendAsync(remoteValid, cancellationToken);
        _ = await Assert.That(remoteWithJwt.StatusCode).IsEqualTo(HttpStatusCode.OK);
    }
}
