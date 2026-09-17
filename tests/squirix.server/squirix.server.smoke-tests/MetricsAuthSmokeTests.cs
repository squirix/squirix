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

/// <summary>Smoke tests verifying JWT auth rules on the Prometheus-compatible <c language="csharp">/metrics</c> endpoint.</summary>
public sealed class MetricsAuthSmokeTests : SmokeTestBase
{
    private const string InvalidBearerToken = "invalid.jwt.token";
    private static readonly SocketsHttpHandler RemoteMetricsHandler = LoopbackHttp.CreateHandlerAllowingCertNameMismatch();
    private static readonly HttpClient RemoteMetricsClient = new(RemoteMetricsHandler, false);

    /// <summary>Ensures <c language="csharp">/metrics</c> follows loopback-anonymous and remote-JWT rules when server auth is configured.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task MetricsValidatesJwtWhenConfigured(CancellationToken cancellationToken)
    {
        var localIp = LocalHostNetworking.GetLocalNonLoopbackIpv4();
        _ = await Assert.That(string.IsNullOrWhiteSpace(localIp)).IsFalse();

        var credentials = TestJwtHelper.CreateRandomCredentials();
        using var held = ListenPortPool.SmokeTests.HoldPort();
        var bindUrl = NodeInvariantIndexStrings.FormatHttpsOrigin("0.0.0.0", held.Port);
        var loopbackUrl = NodeInvariantIndexStrings.FormatHttpsOrigin("127.0.0.1", held.Port);
        var remoteMetricsUrl = NodeInvariantIndexStrings.FormatHttpsAbsolute(localIp!, held.Port, "/metrics");
        var loopbackMetricsUrl = $"{loopbackUrl}/metrics";

        await using var node = await StartNodeAsync(
            bindUrl,
            "node-metrics-auth",
            new SmokeNodeStartOptions { Security = TestJwtHelper.ToSecurityOptions(credentials) },
            cancellationToken);

        var loopbackAnonymous = await HttpClient.GetAsync(new Uri(loopbackMetricsUrl), cancellationToken);
        _ = await Assert.That(loopbackAnonymous.IsSuccessStatusCode).IsTrue();

        using (var loopbackAuthorized = new HttpRequestMessage(HttpMethod.Get, loopbackMetricsUrl))
        {
            loopbackAuthorized.Version = HttpVersion.Version20;
            loopbackAuthorized.VersionPolicy = HttpVersionPolicy.RequestVersionExact;
            loopbackAuthorized.Headers.Authorization = new AuthenticationHeaderValue("Bearer", TestJwtHelper.CreateBearerToken(credentials));
            var loopbackWithJwt = await HttpClient.SendAsync(loopbackAuthorized, cancellationToken);
            _ = await Assert.That(loopbackWithJwt.IsSuccessStatusCode).IsTrue();
        }

        var remoteAnonymous = await RemoteMetricsClient.GetAsync(new Uri(remoteMetricsUrl), cancellationToken);
        _ = await Assert.That(remoteAnonymous.StatusCode).IsEqualTo(HttpStatusCode.Unauthorized);

        using (var remoteInvalid = new HttpRequestMessage(HttpMethod.Get, remoteMetricsUrl))
        {
            remoteInvalid.Headers.Authorization = new AuthenticationHeaderValue("Bearer", InvalidBearerToken);
            var remoteInvalidJwt = await RemoteMetricsClient.SendAsync(remoteInvalid, cancellationToken);
            _ = await Assert.That(remoteInvalidJwt.StatusCode).IsEqualTo(HttpStatusCode.Unauthorized);
        }

        using var remoteValid = new HttpRequestMessage(HttpMethod.Get, remoteMetricsUrl);
        remoteValid.Headers.Authorization = new AuthenticationHeaderValue("Bearer", TestJwtHelper.CreateBearerToken(credentials));
        var remoteWithJwt = await RemoteMetricsClient.SendAsync(remoteValid, cancellationToken);
        _ = await Assert.That(remoteWithJwt.StatusCode).IsEqualTo(HttpStatusCode.OK);
    }
}
