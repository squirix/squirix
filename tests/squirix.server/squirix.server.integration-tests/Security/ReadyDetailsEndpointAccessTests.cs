using System;
using System.Net;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Net.NetworkInformation;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Cluster.Membership;
using Squirix.Server.TestKit.Http;
using Squirix.Server.TestKit.Security;
using Xunit;

namespace Squirix.Server.IntegrationTests.Security;

/// <summary>
/// Verifies readiness details access rules for loopback and remote clients.
/// </summary>
public sealed class ReadyDetailsEndpointAccessTests : IntegrationTestBase
{
    private static readonly SocketsHttpHandler NonLoopbackIpHandler = LoopbackHttp.CreateHandlerAllowingCertificateNameMismatch();
    private static readonly HttpClient NonLoopbackIpHttpClient = new(NonLoopbackIpHandler, false);

    /// <summary>
    /// Verifies authenticated remote scrapes succeed when server auth is enabled.
    /// </summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Fact]
    public async Task AuthenticatedReadyDetailsScrapeSucceedsOnNonLoopbackListenerWhenAuthEnabled()
    {
        var credentials = TestJwtHelper.CreateRandomCredentials();
        var mainPort = AllocateDedicatedPort();
        var url = $"https://0.0.0.0:{mainPort}";
        var peers = new[] { new Peer { NodeId = Guid.NewGuid().ToString("N"), Url = url } };

        await using var node = await StartNodeAsync(url, peers, security: TestJwtHelper.ToSecurityOptions(credentials));

        using var req = new HttpRequestMessage(HttpMethod.Get, $"https://127.0.0.1:{mainPort}/health/ready/details");
        req.Version = HttpVersion.Version20;
        req.VersionPolicy = HttpVersionPolicy.RequestVersionExact;
        req.Headers.Authorization = new AuthenticationHeaderValue("Bearer", TestJwtHelper.CreateBearerToken(credentials));

        var response = await HttpClient.SendAsync(req, DefaultCancellationToken);
        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
    }

    /// <summary>
    /// Verifies loopback scrapes succeed without credentials when server auth is enabled.
    /// </summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Fact]
    public async Task LoopbackReadyDetailsScrapeSucceedsWithoutCredentialsWhenAuthEnabled()
    {
        var credentials = TestJwtHelper.CreateRandomCredentials();
        var mainPort = AllocateDedicatedPort();
        var url = $"https://0.0.0.0:{mainPort}";
        var peers = new[] { new Peer { NodeId = Guid.NewGuid().ToString("N"), Url = url } };

        await using var node = await StartNodeAsync(url, peers, security: TestJwtHelper.ToSecurityOptions(credentials));

        var response = await HttpClient.GetAsync(new Uri($"https://127.0.0.1:{mainPort}/health/ready/details"), DefaultCancellationToken);
        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
    }

    /// <summary>
    /// Verifies remote scrapes without credentials are rejected when server auth is enabled.
    /// </summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Fact]
    public async Task RemoteReadyDetailsScrapeReturns401WithoutCredentialsWhenAuthEnabled()
    {
        var localIp = TryGetLocalNonLoopbackIpv4();
        Assert.False(string.IsNullOrWhiteSpace(localIp), "Test requires a non-loopback IPv4 address on the host.");

        var credentials = TestJwtHelper.CreateRandomCredentials();
        var mainPort = AllocateDedicatedPort();
        var url = $"https://0.0.0.0:{mainPort}";
        var peers = new[] { new Peer { NodeId = Guid.NewGuid().ToString("N"), Url = url } };

        await using var node = await StartNodeAsync(url, peers, security: TestJwtHelper.ToSecurityOptions(credentials));

        var response = await GetReadyDetailsViaLocalIpAsync(localIp, mainPort, DefaultCancellationToken);
        Assert.Equal(HttpStatusCode.Unauthorized, response.StatusCode);
    }

    private static Task<HttpResponseMessage> GetReadyDetailsViaLocalIpAsync(string localIp, int port, CancellationToken cancellationToken) =>
        NonLoopbackIpHttpClient.GetAsync(new Uri($"https://{localIp}:{port}/health/ready/details"), cancellationToken);

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
