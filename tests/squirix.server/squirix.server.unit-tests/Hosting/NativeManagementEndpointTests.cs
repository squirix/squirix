using System;
using System.IO;
using System.Net;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using Squirix.Server.Adapters.Endpoint;
using Squirix.Server.Attributes;
using Squirix.Server.Node.Observability;
using Squirix.Server.TestKit;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Hosting;

/// <summary>Verifies the native management HTTP contract.</summary>
[Immutable]
public sealed class NativeManagementEndpointTests
{
    /// <summary>GET and HEAD expose the same health metadata without a body for HEAD.</summary>
    [Test]
    public async Task HealthSupportsGetAndHead()
    {
        await using var services = new ServiceCollection().BuildServiceProvider();
        const string requestSuffix = " /health?probe=1 HTTP/1.1\r\nHost: localhost\r\n\r\n";
        var get = await ExchangeAsync(services, $"GET{requestSuffix}", IPAddress.Loopback);
        var head = await ExchangeAsync(services, $"HEAD{requestSuffix}", IPAddress.Loopback);

        _ = await Assert.That(get.StartsWith("HTTP/1.1 200 OK\r\n", StringComparison.Ordinal)).IsTrue();
        _ = await Assert.That(get).IsEqualTo(head + "\"OK\"");
        _ = await Assert.That(head).Contains("Content-Length: 4\r\n", StringComparison.Ordinal);
    }

    /// <summary>The readiness probe maps a failed check to 503 without affecting liveness.</summary>
    [Test]
    public async Task ReadyFailureKeepsLivenessHealthy()
    {
        var registrations = new ServiceCollection();
        _ = registrations.AddLogging();
        _ = registrations.AddSquirixFrameworkServices();
        _ = registrations.AddSingleton(new ReadinessCheck(
            "not-ready", static _ => new TestReadinessCheck(static () => ReadinessResult.Unhealthy("blocked"))));
        await using var services = registrations.BuildServiceProvider();
        var ready = await ExchangeAsync(services, "GET /health/ready HTTP/1.1\r\nHost: localhost\r\n\r\n", IPAddress.Loopback);
        var live = await ExchangeAsync(services, "GET /health/live HTTP/1.1\r\nHost: localhost\r\n\r\n", IPAddress.Loopback);

        _ = await Assert.That(ready.StartsWith("HTTP/1.1 503 Service Unavailable\r\n", StringComparison.Ordinal)).IsTrue();
        _ = await Assert.That(ready.EndsWith("\r\n\r\nUnhealthy", StringComparison.Ordinal)).IsTrue();
        _ = await Assert.That(ready).Contains("Cache-Control: no-store, no-cache\r\n", StringComparison.Ordinal);
        _ = await Assert.That(live.StartsWith("HTTP/1.1 200 OK\r\n", StringComparison.Ordinal)).IsTrue();
    }

    /// <summary>A degraded node still answers its readiness probe successfully.</summary>
    [Test]
    public async Task ReadyReturnsOkWithDegradedStatus()
    {
        var registrations = new ServiceCollection();
        _ = registrations.AddLogging();
        _ = registrations.AddSquirixFrameworkServices();
        _ = registrations.AddSingleton(new ReadinessCheck(
            "degraded", static _ => new TestReadinessCheck(static () => ReadinessResult.Degraded("slow"))));
        _ = registrations.AddSingleton(new ReadinessCheck(
            "healthy", static _ => new TestReadinessCheck(static () => ReadinessResult.Healthy("ready"))));
        await using var services = registrations.BuildServiceProvider();

        var ready = await ExchangeAsync(services, "GET /health/ready HTTP/1.1\r\nHost: localhost\r\n\r\n", IPAddress.Loopback);

        _ = await Assert.That(ready.StartsWith("HTTP/1.1 200 OK\r\n", StringComparison.Ordinal)).IsTrue();
        _ = await Assert.That(ready.EndsWith("\r\n\r\nDegraded", StringComparison.Ordinal)).IsTrue();
    }

    /// <summary>A check that throws makes the node unhealthy even when another check is only degraded.</summary>
    [Test]
    public async Task ReadyReturns503WhenCheckThrows()
    {
        var registrations = new ServiceCollection();
        _ = registrations.AddLogging();
        _ = registrations.AddSquirixFrameworkServices();
        _ = registrations.AddSingleton(new ReadinessCheck(
            "throwing", static _ => new TestReadinessCheck(static () => throw new InvalidOperationException("probe failed"))));
        _ = registrations.AddSingleton(new ReadinessCheck(
            "degraded", static _ => new TestReadinessCheck(static () => ReadinessResult.Degraded("slow"))));
        await using var services = registrations.BuildServiceProvider();

        var ready = await ExchangeAsync(services, "GET /health/ready HTTP/1.1\r\nHost: localhost\r\n\r\n", IPAddress.Loopback);

        _ = await Assert.That(ready.StartsWith("HTTP/1.1 503 Service Unavailable\r\n", StringComparison.Ordinal)).IsTrue();
        _ = await Assert.That(ready.EndsWith("\r\n\r\nUnhealthy", StringComparison.Ordinal)).IsTrue();
    }

    /// <summary>Only the authenticated socket identity and one bearer header can grant protected access.</summary>
    /// <param name="headers">Additional request headers.</param>
    /// <param name="status">Expected status line suffix.</param>
    [Test]
    [Arguments("X-Forwarded-For: 127.0.0.1\r\n", "401 Unauthorized")]
    [Arguments("Authorization: Bearer token\r\nAuthorization: Bearer other\r\n", "400 Bad Request")]
    [Arguments("Transfer-Encoding: chunked\r\n", "400 Bad Request")]
    public async Task RejectsInvalidProtectedRequests(string headers, string status)
    {
        await using var services = new ServiceCollection().BuildServiceProvider();
        var request = $"GET /health/ready/details HTTP/1.1\r\nHost: localhost\r\n{headers}\r\n";
        var response = await ExchangeAsync(services, request, IPAddress.Parse("203.0.113.10"));
        _ = await Assert.That(response.StartsWith($"HTTP/1.1 {status}\r\n", StringComparison.Ordinal)).IsTrue();
    }

    /// <summary>The server closes requests with headers exceeding its allocation bound.</summary>
    [Test]
    public async Task RejectsOversizedHeaders()
    {
        await using var services = new ServiceCollection().BuildServiceProvider();
        var request = $"GET /health HTTP/1.1\r\nHost: localhost\r\nTest: {new string('a', 16 * 1024)}\r\n\r\n";
        var response = await ExchangeAsync(services, request, IPAddress.Loopback);
        _ = await Assert.That(response.StartsWith("HTTP/1.1 431 Request Header Fields Too Large\r\n", StringComparison.Ordinal)).IsTrue();
    }

    /// <summary>Checks the authority field required for HTTP/1.1 requests.</summary>
    /// <param name="caseId">The authority test case.</param>
    [Test]
    [Arguments(0)]
    [Arguments(1)]
    [Arguments(2)]
    [Arguments(3)]
    [Arguments(4)]
    public async Task ValidatesManagementHost(int caseId)
    {
        await using var services = new ServiceCollection().BuildServiceProvider();
        var (headers, status) = caseId switch
        {
            0 => ("HTTP/1.1", "400 Bad Request"),
            1 => ("HTTP/1.1\r\nHost: localhost\r\nHOST: localhost", "400 Bad Request"),
            2 => ("HTTP/1.1\r\nHost: bad host", "400 Bad Request"),
            3 => ("HTTP/1.0", "200 OK"),
            _ => ("HTTP/1.1\r\nHost: [::1]:5002", "200 OK"),
        };

        var response = await ExchangeAsync(services, $"GET /health {headers}\r\n\r\n", IPAddress.Loopback, 1);
        _ = await Assert.That(response.StartsWith($"HTTP/1.1 {status}\r\n", StringComparison.Ordinal)).IsTrue();
    }

    /// <summary>Advertises the supported method or authentication challenge on the corresponding error.</summary>
    [Test]
    public async Task ManagementErrorsIncludeRequiredHeaders()
    {
        await using var services = new ServiceCollection().BuildServiceProvider();
        var method = await ExchangeAsync(services, "POST /health HTTP/1.1\r\nHost: localhost\r\nContent-Length: 1\r\n\r\nX", IPAddress.Loopback);
        var auth = await ExchangeAsync(services, "GET /health/ready/details HTTP/1.1\r\nHost: localhost\r\n\r\n", IPAddress.Parse("203.0.113.10"));

        _ = await Assert.That(method.StartsWith("HTTP/1.1 405 Method Not Allowed\r\n", StringComparison.Ordinal)).IsTrue();
        _ = await Assert.That(method).Contains("\r\nAllow: GET, HEAD\r\n", StringComparison.Ordinal);
        _ = await Assert.That(auth.StartsWith("HTTP/1.1 401 Unauthorized\r\n", StringComparison.Ordinal)).IsTrue();
        _ = await Assert.That(auth).Contains("\r\nWWW-Authenticate: Bearer realm=\"management\"\r\n", StringComparison.Ordinal);
    }

    private static async Task<string> ExchangeAsync(IServiceProvider services, string request, IPAddress address, int fragmentSize = int.MaxValue)
    {
        var endpoint = new NativeManagementEndpoint(services, static (_, _) => ValueTask.FromResult(false), Squirix.Server.TestKit.Logging.NullLogger<NativeManagementEndpoint>.Instance);
        var bytes = Encoding.ASCII.GetBytes(request);
        var stream = new FragmentedStream(fragmentSize);
        await using var lifetime = stream.ConfigureAwait(false);
        await stream.WriteAsync(bytes, CancellationToken.None).ConfigureAwait(false);
        stream.Position = 0;

        await endpoint.ServeAsync(stream, new IPEndPoint(address, 3000), CancellationToken.None, CancellationToken.None).ConfigureAwait(false);
        var responseOffset = Math.Min(bytes.Length, 16 * 1024);
        return Encoding.ASCII.GetString(stream.GetBuffer(), responseOffset, int.CreateChecked(stream.Position) - responseOffset);
    }

    private sealed class FragmentedStream : MemoryStream
    {
        private readonly int _fragmentSize;

        internal FragmentedStream(int fragmentSize)
        {
            _fragmentSize = fragmentSize;
        }

        public override ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = default) =>
            base.ReadAsync(buffer[..Math.Min(buffer.Length, _fragmentSize)], cancellationToken);
    }

    private sealed class TestReadinessCheck : IReadinessCheck
    {
        private readonly Func<ReadinessResult> _check;

        internal TestReadinessCheck(Func<ReadinessResult> check)
        {
            _check = check;
        }

        public Task<ReadinessResult> CheckHealthAsync(CancellationToken cancellationToken) => Task.FromResult(_check());
    }
}
