using System;
using System.Net;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Threading.Tasks;
using Squirix.Server.TestKit;
using Squirix.Server.TestKit.Networking;
using Xunit;

namespace Squirix.Server.SmokeTests;

/// <summary>
/// Smoke tests verifying JWT auth rules on the Prometheus-compatible <c>/metrics</c> endpoint.
/// </summary>
public sealed class MetricsAuthSmokeTests : SmokeTestBase
{
    private const string InvalidBearerToken = "invalid.jwt.token";
    private static readonly SocketsHttpHandler RemoteMetricsHandler = LoopbackHttp.CreateHandlerAllowingCertNameMismatch();
    private static readonly HttpClient RemoteMetricsClient = new(RemoteMetricsHandler, false);

    /// <summary>
    /// Ensures <c>/metrics</c> follows loopback-anonymous and remote-JWT rules when server auth is configured.
    /// </summary>
    [Fact]
    public async Task MetricsValidatesJwtWhenConfigured()
    {
        var localIp = LocalHostNetworking.GetLocalNonLoopbackIpv4();
        Assert.False(string.IsNullOrWhiteSpace(localIp));

        var credentials = TestJwtHelper.CreateRandomCredentials();
        var (bindUrl, loopbackUrl) = GetNextAnyInterfaceListenUrls();
        var port = new Uri(bindUrl).Port;
        var remoteMetricsUrl = NodeInvariantIndexStrings.FormatHttpsAbsolute(localIp, port, "/metrics");
        var loopbackMetricsUrl = $"{loopbackUrl}/metrics";

        await using var node = await StartNodeAsync(
            bindUrl,
            "node-metrics-auth",
            new SmokeNodeStartOptions { Security = TestJwtHelper.ToSecurityOptions(credentials) },
            DefaultCancellationToken);

        var loopbackAnonymous = await HttpClient.GetAsync(new Uri(loopbackMetricsUrl), DefaultCancellationToken);
        Assert.True(loopbackAnonymous.IsSuccessStatusCode);

        using (var loopbackAuthorized = new HttpRequestMessage(HttpMethod.Get, loopbackMetricsUrl))
        {
            loopbackAuthorized.Version = HttpVersion.Version20;
            loopbackAuthorized.VersionPolicy = HttpVersionPolicy.RequestVersionExact;
            loopbackAuthorized.Headers.Authorization = new AuthenticationHeaderValue("Bearer", TestJwtHelper.CreateBearerToken(credentials));
            var loopbackWithJwt = await HttpClient.SendAsync(loopbackAuthorized, DefaultCancellationToken);
            Assert.True(loopbackWithJwt.IsSuccessStatusCode);
        }

        var remoteAnonymous = await RemoteMetricsClient.GetAsync(new Uri(remoteMetricsUrl), DefaultCancellationToken);
        Assert.Equal(HttpStatusCode.Unauthorized, remoteAnonymous.StatusCode);

        using (var remoteInvalid = new HttpRequestMessage(HttpMethod.Get, remoteMetricsUrl))
        {
            remoteInvalid.Headers.Authorization = new AuthenticationHeaderValue("Bearer", InvalidBearerToken);
            var remoteInvalidJwt = await RemoteMetricsClient.SendAsync(remoteInvalid, DefaultCancellationToken);
            Assert.Equal(HttpStatusCode.Unauthorized, remoteInvalidJwt.StatusCode);
        }

        using var remoteValid = new HttpRequestMessage(HttpMethod.Get, remoteMetricsUrl);
        remoteValid.Headers.Authorization = new AuthenticationHeaderValue("Bearer", TestJwtHelper.CreateBearerToken(credentials));
        var remoteWithJwt = await RemoteMetricsClient.SendAsync(remoteValid, DefaultCancellationToken);
        Assert.Equal(HttpStatusCode.OK, remoteWithJwt.StatusCode);
    }
}
