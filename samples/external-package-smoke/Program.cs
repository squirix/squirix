using System;
using System.Diagnostics.CodeAnalysis;
using System.IO;
using System.Net;
using System.Net.Http;
using System.Net.Security;
using System.Net.Sockets;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server;

namespace Squirix.ExternalPackageSmoke;

internal static class Program
{
    private const string IsolationSharedKey = "shared-key";

    private static async Task<int> Main()
    {
        // Isolated store so a third-party run does not pick up a developer's LocalApplicationData journal/snapshots.
        var testRoot = Path.Combine(Path.GetTempPath(), "squirix-external-smoke", Guid.NewGuid().ToString("N"));
        _ = Directory.CreateDirectory(testRoot);
        Environment.SetEnvironmentVariable("SQUIRIX_TEST_ROOT", testRoot);

        var endpoint = $"https://localhost:{NextFreePort()}";
        WriteSettings("external-smoke", endpoint);
        await using var host = await SquirixServer.StartAsync(CancellationToken.None);
#pragma warning disable CA2000 // Handler lifetime is owned by the connected SquirixClient session.
        var httpHandler = CreateLoopbackDevelopmentHandler();
#pragma warning restore CA2000
        await using var client = await SquirixClient.ConnectAsync(
            options =>
            {
                options.Endpoints.Add(endpoint);
                options.HttpMessageHandler = httpHandler;
            },
            CancellationToken.None);

        await RunIsolationAsync(client, CancellationToken.None);
        await RunExpirationAsync(client, CancellationToken.None);

        return 0;
    }

    [SuppressMessage("Security", "CA5359:Do not disable certificate validation", Justification = "Package smoke targets loopback HTTPS with the ASP.NET Core development certificate.")]
    [SuppressMessage("Reliability", "CA2000:Dispose objects before losing scope", Justification = "The connected SquirixClient session owns the supplied handler for its lifetime.")]
    private static SocketsHttpHandler CreateLoopbackDevelopmentHandler()
    {
        return new SocketsHttpHandler
        {
            UseProxy = false,
            EnableMultipleHttp2Connections = true,
            SslOptions = new SslClientAuthenticationOptions
            {
                RemoteCertificateValidationCallback = static (_, _, _, _) => true,
            },
        };
    }

    private static int NextFreePort()
    {
        using var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        return ((IPEndPoint)listener.LocalEndpoint).Port;
    }

    private static async Task RunIsolationAsync(ISquirixClient client, CancellationToken ct)
    {
        var a = await client.GetCacheAsync<string>("smoke-a", ct);
        var b = await client.GetCacheAsync<string>("smoke-b", ct);
        await a.SetAsync(IsolationSharedKey, "from-a", cancellationToken: ct);
        await b.SetAsync(IsolationSharedKey, "from-b", cancellationToken: ct);
        if (!string.Equals((await a.GetValueAsync(IsolationSharedKey, ct)).Value, "from-a", StringComparison.Ordinal) || !string.Equals(
                (await b.GetValueAsync(IsolationSharedKey, ct)).Value,
                "from-b",
                StringComparison.Ordinal))
        {
            throw new InvalidOperationException("Named cache isolation failed.");
        }
    }

    private static async Task RunExpirationAsync(ISquirixClient client, CancellationToken ct)
    {
        var cache = await client.GetCacheAsync<string>("smoke-expiration", ct);
        await cache.SetAsync("expiring", "x", new CacheEntryOptions { Expiration = TimeSpan.FromMilliseconds(80) }, ct);
        await Task.Delay(200, ct);
        var result = await cache.GetValueAsync("expiring", ct);
        if (result.Found)
        {
            throw new InvalidOperationException("Expected expiration key to be absent after wait.");
        }
    }

    private static void WriteSettings(string nodeId, string url)
    {
        var settings = new
        {
            Squirix = new
            {
                Cluster = new
                {
                    NodeId = nodeId,
                    Url = url,
                    VirtualNodes = 128,
                    Peers = new[]
                    {
                        new
                        {
                            NodeId = nodeId,
                            Url = url,
                        },
                    },
                },
            },
        };

        File.WriteAllText("Squirix.settings.json", JsonSerializer.Serialize(settings));
    }
}
