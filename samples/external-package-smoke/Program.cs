using System;
using System.IO;
using System.Net;
using System.Net.Sockets;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server;

namespace Squirix.ExternalPackageSmoke;

internal static class Program
{
    private const string IsolationSharedKey = "shared-key";

    public static async Task<int> Main()
    {
        // Isolated temp root for testkit-scoped paths when persistence is enabled in samples or tests.
        var testRoot = Path.Join(Path.GetTempPath(), "squirix-external-smoke", Guid.NewGuid().ToString("N"));
        _ = Directory.CreateDirectory(testRoot);
        Environment.SetEnvironmentVariable("SQUIRIX_TEST_ROOT", testRoot);

        var endpoint = $"https://localhost:{NextFreePort()}";
        WriteSettings("external-smoke", endpoint);
        await using var host = await SquirixServer.StartAsync(CancellationToken.None);
        await using var client = await SquirixClient.ConnectAsync(endpoint, CancellationToken.None);

        await RunIsolationAsync(client, CancellationToken.None);
        await RunExpirationAsync(client, CancellationToken.None);

        return 0;
    }

    private static int NextFreePort()
    {
        using var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        return ((IPEndPoint)listener.LocalEndpoint).Port;
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

    private static async Task RunIsolationAsync(ISquirixClient client, CancellationToken ct)
    {
        var a = await client.GetCacheAsync<string>("smoke-a", ct);
        var b = await client.GetCacheAsync<string>("smoke-b", ct);
        await a.SetAsync(IsolationSharedKey, "from-a", cancellationToken: ct);
        await b.SetAsync(IsolationSharedKey, "from-b", cancellationToken: ct);
        var v1 = (await a.GetValueAsync(IsolationSharedKey, ct)).Value;
        var v2 = (await b.GetValueAsync(IsolationSharedKey, ct)).Value;
        if (!string.Equals(v1, "from-a", StringComparison.Ordinal) || !string.Equals(v2, "from-b", StringComparison.Ordinal))
        {
            throw new InvalidOperationException("Named cache isolation failed.");
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
