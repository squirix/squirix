using System;
using System.Net;
using System.Net.Http;
using System.Net.NetworkInformation;
using System.Net.Security;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Cluster.Membership;
using Squirix.Server.TestKit.AspNetCore;
using Squirix.Server.TestKit.Http;
using Xunit;

namespace Squirix.Server.IntegrationTests.Security;

/// <summary>
/// Verifies Prometheus metrics access rules for loopback and remote clients.
/// </summary>
public sealed class MetricsEndpointAccessTests : IntegrationTestBase
{
    /// <summary>
    /// Verifies loopback scrapes succeed without credentials when server auth is enabled.
    /// </summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Fact]
    public async Task LoopbackMetricsScrapeSucceedsWithoutCredentialsWhenAuthEnabled()
    {
        var mainPort = AllocateDedicatedPort();
        var url = $"https://0.0.0.0:{mainPort}";
        var peers = new[] { new Peer { NodeId = Guid.NewGuid().ToString("N"), Url = url } };

        await using var node = await StartNodeAsync(
            url,
            peers,
            security: new TestNodeSecurityOptions { ApiKeys = ["metrics-secret"] });

        var response = await HttpClient.GetAsync($"https://127.0.0.1:{mainPort}/metrics", DefaultCancellationToken);
        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
    }

    /// <summary>
    /// Verifies authenticated scrapes succeed against a non-loopback listener when server auth is enabled.
    /// </summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Fact]
    public async Task AuthenticatedMetricsScrapeSucceedsOnNonLoopbackListenerWhenAuthEnabled()
    {
        var mainPort = AllocateDedicatedPort();
        var url = $"https://0.0.0.0:{mainPort}";
        var peers = new[] { new Peer { NodeId = Guid.NewGuid().ToString("N"), Url = url } };

        await using var node = await StartNodeAsync(
            url,
            peers,
            security: new TestNodeSecurityOptions { ApiKeys = ["metrics-secret"] });

        using var req = new HttpRequestMessage(HttpMethod.Get, $"https://127.0.0.1:{mainPort}/metrics");
        req.Version = HttpVersion.Version20;
        req.VersionPolicy = HttpVersionPolicy.RequestVersionExact;
        req.Headers.Add("X-Api-Key", "metrics-secret");

        var response = await HttpClient.SendAsync(req, DefaultCancellationToken);
        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
    }

    /// <summary>
    /// Verifies remote scrapes without credentials are rejected when server auth is enabled.
    /// </summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Fact]
    public async Task RemoteMetricsScrapeReturns401WithoutCredentialsWhenAuthEnabled()
    {
        var localIp = TryGetLocalNonLoopbackIpv4();
        Assert.False(string.IsNullOrWhiteSpace(localIp), "Test requires a non-loopback IPv4 address on the host.");

        var mainPort = AllocateDedicatedPort();
        var url = $"https://0.0.0.0:{mainPort}";
        var peers = new[] { new Peer { NodeId = Guid.NewGuid().ToString("N"), Url = url } };

        await using var node = await StartNodeAsync(
            url,
            peers,
            security: new TestNodeSecurityOptions { ApiKeys = ["metrics-secret"] });

        var response = await GetMetricsViaLocalIpAsync(localIp!, mainPort, DefaultCancellationToken);
        Assert.Equal(HttpStatusCode.Unauthorized, response.StatusCode);
    }

    private static async Task<HttpResponseMessage> GetMetricsViaLocalIpAsync(string localIp, int port, CancellationToken cancellationToken)
    {
        using var handler = LoopbackHttp.CreateHandler();
        handler.SslOptions.RemoteCertificateValidationCallback = static (_, _, _, errors) =>
            errors is SslPolicyErrors.None or SslPolicyErrors.RemoteCertificateNameMismatch;

        using var client = new HttpClient(handler, disposeHandler: true)
        {
            DefaultRequestVersion = HttpVersion.Version20,
            DefaultVersionPolicy = HttpVersionPolicy.RequestVersionOrHigher,
        };

        return await client.GetAsync($"https://{localIp}:{port}/metrics", cancellationToken);
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
