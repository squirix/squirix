using System;
using System.Net;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Net.NetworkInformation;
using System.Net.Sockets;
using System.Threading.Tasks;
using Squirix.Server.Cluster.Membership;
using Squirix.Server.TestKit.Http;
using Squirix.Server.TestKit.Security;
using Xunit;

namespace Squirix.Server.SmokeTests.Observability;

/// <summary>
/// Smoke tests verifying JWT auth rules on the Prometheus-compatible <c>/metrics</c> endpoint.
/// </summary>
/// <remarks>
/// See <see cref="AuthProtectedSurfaceInventory" /> for the full protected-surface map.
/// </remarks>
public sealed class MetricsAuthSmokeTests : SmokeTestBase
{
    private const string InvalidBearerToken = "invalid.jwt.token";

    private static readonly SocketsHttpHandler RemoteMetricsHandler = LoopbackHttp.CreateHandlerAllowingCertificateNameMismatch();
    private static readonly HttpClient RemoteMetricsClient = new(RemoteMetricsHandler, disposeHandler: false);

    /// <summary>
    /// Ensures <c>/metrics</c> follows loopback-anonymous and remote-JWT rules when server auth is configured.
    /// </summary>
    /// <returns>A task representing the asynchronous smoke test.</returns>
    [Fact]
    public async Task MetricsRejectsMissingAndInvalidJwtForRemoteAndAcceptsValidJwtWhenConfigured()
    {
        var localIp = TryGetLocalNonLoopbackIpv4();
        Assert.False(string.IsNullOrWhiteSpace(localIp), "Test requires a non-loopback IPv4 address on the host.");

        var credentials = TestJwtHelper.CreateRandomCredentials();
        var (bindUrl, loopbackUrl) = GetNextAnyInterfaceListenUrls();
        var peers = new[] { new Peer { NodeId = "node-metrics-auth", Url = bindUrl } };

        await using var node = await StartNodeAsync(
            bindUrl,
            peers,
            security: TestJwtHelper.ToSecurityOptions(credentials),
            extraScope: Guid.NewGuid().ToString("N"),
            cancellationToken: DefaultCancellationToken);

        var loopbackAnonymous = await HttpClient.GetAsync($"{loopbackUrl}/metrics", DefaultCancellationToken);
        Assert.True(loopbackAnonymous.IsSuccessStatusCode, $"Expected loopback scrape success, got {(int)loopbackAnonymous.StatusCode} {loopbackAnonymous.ReasonPhrase}");

        using (var loopbackAuthorized = new HttpRequestMessage(HttpMethod.Get, $"{loopbackUrl}/metrics"))
        {
            loopbackAuthorized.Version = HttpVersion.Version20;
            loopbackAuthorized.VersionPolicy = HttpVersionPolicy.RequestVersionExact;
            loopbackAuthorized.Headers.Authorization = new AuthenticationHeaderValue("Bearer", TestJwtHelper.CreateBearerToken(credentials));
            var loopbackWithJwt = await HttpClient.SendAsync(loopbackAuthorized, DefaultCancellationToken);
            Assert.True(loopbackWithJwt.IsSuccessStatusCode, $"Expected loopback success with JWT, got {(int)loopbackWithJwt.StatusCode} {loopbackWithJwt.ReasonPhrase}");
        }

        var remoteAnonymous = await RemoteMetricsClient.GetAsync($"https://{localIp}:{new Uri(bindUrl).Port}/metrics", DefaultCancellationToken);
        Assert.Equal(HttpStatusCode.Unauthorized, remoteAnonymous.StatusCode);

        using (var remoteInvalid = new HttpRequestMessage(HttpMethod.Get, $"https://{localIp}:{new Uri(bindUrl).Port}/metrics"))
        {
            remoteInvalid.Headers.Authorization = new AuthenticationHeaderValue("Bearer", InvalidBearerToken);
            var remoteInvalidJwt = await RemoteMetricsClient.SendAsync(remoteInvalid, DefaultCancellationToken);
            Assert.Equal(HttpStatusCode.Unauthorized, remoteInvalidJwt.StatusCode);
        }

        using (var remoteValid = new HttpRequestMessage(HttpMethod.Get, $"https://{localIp}:{new Uri(bindUrl).Port}/metrics"))
        {
            remoteValid.Headers.Authorization = new AuthenticationHeaderValue("Bearer", TestJwtHelper.CreateBearerToken(credentials));
            var remoteWithJwt = await RemoteMetricsClient.SendAsync(remoteValid, DefaultCancellationToken);
            Assert.Equal(HttpStatusCode.OK, remoteWithJwt.StatusCode);
        }
    }

    private static string? TryGetLocalNonLoopbackIpv4()
    {
        foreach (var nic in NetworkInterface.GetAllNetworkInterfaces())
        {
            if (nic.OperationalStatus != OperationalStatus.Up)
                continue;

            foreach (var address in nic.GetIPProperties().UnicastAddresses)
            {
                if (address.Address.AddressFamily != AddressFamily.InterNetwork)
                    continue;

                if (IPAddress.IsLoopback(address.Address))
                    continue;

                return address.Address.ToString();
            }
        }

        return null;
    }
}
