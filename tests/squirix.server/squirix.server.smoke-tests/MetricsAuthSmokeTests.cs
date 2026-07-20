using System;
using System.Globalization;
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
    private static readonly SocketsHttpHandler RemoteMetricsHandler = LoopbackHttp.CreateHandlerAllowingCertificateNameMismatch();
    private static readonly HttpClient RemoteMetricsClient = new(RemoteMetricsHandler, false);

    /// <summary>
    /// Ensures <c>/metrics</c> follows loopback-anonymous and remote-JWT rules when server auth is configured.
    /// </summary>
    [Fact]
    public async Task MetricsRejectsMissingAndInvalidJwtForRemoteAndAcceptsValidJwtWhenConfigured()
    {
        var localIp = LocalHostNetworking.GetLocalNonLoopbackIpv4();
        Assert.False(string.IsNullOrWhiteSpace(localIp), "Test requires a non-loopback IPv4 address on the host.");

        var credentials = TestJwtHelper.CreateRandomCredentials();
        var (bindUrl, loopbackUrl) = GetNextAnyInterfaceListenUrls();

        await using var node = await StartNodeAsync(
            bindUrl,
            "node-metrics-auth",
            new SmokeNodeStartOptions { Security = TestJwtHelper.ToSecurityOptions(credentials) },
            cancellationToken: DefaultCancellationToken);

        var loopbackAnonymous = await HttpClient.GetAsync(new Uri($"{loopbackUrl}/metrics"), DefaultCancellationToken);
        Assert.True(loopbackAnonymous.IsSuccessStatusCode, $"Expected loopback scrape success, got {loopbackAnonymous.StatusCode:D} {loopbackAnonymous.ReasonPhrase}");

        using (var loopbackAuthorized = new HttpRequestMessage(HttpMethod.Get, $"{loopbackUrl}/metrics"))
        {
            loopbackAuthorized.Version = HttpVersion.Version20;
            loopbackAuthorized.VersionPolicy = HttpVersionPolicy.RequestVersionExact;
            loopbackAuthorized.Headers.Authorization = new AuthenticationHeaderValue("Bearer", TestJwtHelper.CreateBearerToken(credentials));
            var loopbackWithJwt = await HttpClient.SendAsync(loopbackAuthorized, DefaultCancellationToken);
            Assert.True(loopbackWithJwt.IsSuccessStatusCode, $"Expected loopback success with JWT, got {loopbackWithJwt.StatusCode:D} {loopbackWithJwt.ReasonPhrase}");
        }

        var remoteAnonymous = await RemoteMetricsClient.GetAsync(new Uri($"https://{localIp}:{new Uri(bindUrl).Port.ToString(CultureInfo.InvariantCulture)}/metrics"), DefaultCancellationToken);
        Assert.Equal(HttpStatusCode.Unauthorized, remoteAnonymous.StatusCode);

        using (var remoteInvalid = new HttpRequestMessage(HttpMethod.Get, $"https://{localIp}:{new Uri(bindUrl).Port.ToString(CultureInfo.InvariantCulture)}/metrics"))
        {
            remoteInvalid.Headers.Authorization = new AuthenticationHeaderValue("Bearer", InvalidBearerToken);
            var remoteInvalidJwt = await RemoteMetricsClient.SendAsync(remoteInvalid, DefaultCancellationToken);
            Assert.Equal(HttpStatusCode.Unauthorized, remoteInvalidJwt.StatusCode);
        }

        using var remoteValid = new HttpRequestMessage(HttpMethod.Get, $"https://{localIp}:{new Uri(bindUrl).Port.ToString(CultureInfo.InvariantCulture)}/metrics");
        remoteValid.Headers.Authorization = new AuthenticationHeaderValue("Bearer", TestJwtHelper.CreateBearerToken(credentials));
        var remoteWithJwt = await RemoteMetricsClient.SendAsync(remoteValid, DefaultCancellationToken);
        Assert.Equal(HttpStatusCode.OK, remoteWithJwt.StatusCode);
    }
}
