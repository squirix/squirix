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

namespace Squirix.Server.SmokeTests.Health;

/// <summary>
/// Smoke tests verifying JWT auth rules on the <c>/health/ready/details</c> endpoint.
/// </summary>
public sealed class ReadyDetailsAuthSmokeTests : SmokeTestBase
{
    private const string InvalidBearerToken = "invalid.jwt.token";
    private static readonly SocketsHttpHandler RemoteHandler = LoopbackHttp.CreateHandlerAllowingCertificateNameMismatch();
    private static readonly HttpClient RemoteClient = new(RemoteHandler, false);

    /// <summary>
    /// Ensures <c>/health/ready/details</c> follows loopback-anonymous and remote-JWT rules when server auth is configured.
    /// </summary>
    /// <returns>A task representing the asynchronous smoke test.</returns>
    [Fact]
    public async Task ReadyDetailsRejectsMissingAndInvalidJwtForRemoteAndAcceptsValidJwtWhenConfigured()
    {
        var localIp = TryGetLocalNonLoopbackIpv4();
        Assert.False(string.IsNullOrWhiteSpace(localIp), "Test requires a non-loopback IPv4 address on the host.");

        var credentials = TestJwtHelper.CreateRandomCredentials();
        var (bindUrl, loopbackUrl) = GetNextAnyInterfaceListenUrls();
        var peers = new[] { new Peer { NodeId = "node-ready-details-auth", Url = bindUrl } };

        await using var node = await StartNodeAsync(
            bindUrl,
            peers,
            security: TestJwtHelper.ToSecurityOptions(credentials),
            extraScope: Guid.NewGuid().ToString("N"),
            cancellationToken: DefaultCancellationToken);

        var loopbackAnonymous = await HttpClient.GetAsync(new Uri($"{loopbackUrl}/health/ready/details"), DefaultCancellationToken);
        Assert.True(loopbackAnonymous.IsSuccessStatusCode, $"Expected loopback success, got {(int)loopbackAnonymous.StatusCode} {loopbackAnonymous.ReasonPhrase}");

        using (var loopbackAuthorized = new HttpRequestMessage(HttpMethod.Get, $"{loopbackUrl}/health/ready/details"))
        {
            loopbackAuthorized.Version = HttpVersion.Version20;
            loopbackAuthorized.VersionPolicy = HttpVersionPolicy.RequestVersionExact;
            loopbackAuthorized.Headers.Authorization = new AuthenticationHeaderValue("Bearer", TestJwtHelper.CreateBearerToken(credentials));
            var loopbackWithJwt = await HttpClient.SendAsync(loopbackAuthorized, DefaultCancellationToken);
            Assert.True(loopbackWithJwt.IsSuccessStatusCode, $"Expected loopback success with JWT, got {(int)loopbackWithJwt.StatusCode} {loopbackWithJwt.ReasonPhrase}");
        }

        var remoteAnonymous = await RemoteClient.GetAsync(new Uri($"https://{localIp}:{new Uri(bindUrl).Port}/health/ready/details"), DefaultCancellationToken);
        Assert.Equal(HttpStatusCode.Unauthorized, remoteAnonymous.StatusCode);

        using (var remoteInvalid = new HttpRequestMessage(HttpMethod.Get, $"https://{localIp}:{new Uri(bindUrl).Port}/health/ready/details"))
        {
            remoteInvalid.Headers.Authorization = new AuthenticationHeaderValue("Bearer", InvalidBearerToken);
            var remoteInvalidJwt = await RemoteClient.SendAsync(remoteInvalid, DefaultCancellationToken);
            Assert.Equal(HttpStatusCode.Unauthorized, remoteInvalidJwt.StatusCode);
        }

        using var remoteValid = new HttpRequestMessage(HttpMethod.Get, $"https://{localIp}:{new Uri(bindUrl).Port}/health/ready/details");
        remoteValid.Headers.Authorization = new AuthenticationHeaderValue("Bearer", TestJwtHelper.CreateBearerToken(credentials));
        var remoteWithJwt = await RemoteClient.SendAsync(remoteValid, DefaultCancellationToken);
        Assert.Equal(HttpStatusCode.OK, remoteWithJwt.StatusCode);
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
