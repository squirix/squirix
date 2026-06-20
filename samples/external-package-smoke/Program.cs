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
        var testRoot = Directory.CreateTempSubdirectory("squirix-external-smoke");
        try
        {
            Environment.SetEnvironmentVariable("SQUIRIX_TEST_ROOT", testRoot.FullName);

            var endpoint = new UriBuilder(Uri.UriSchemeHttps, "localhost", NextFreePort()).Uri.AbsoluteUri;
            await WriteSettingsAsync("external-smoke", endpoint, CancellationToken.None).ConfigureAwait(false);
            _ = await SquirixServer.StartAsync(CancellationToken.None).ConfigureAwait(false);
            var client = await SquirixClient.ConnectAsync(endpoint, CancellationToken.None).ConfigureAwait(false);

            await RunIsolationAsync(client, CancellationToken.None).ConfigureAwait(false);
            await RunExpirationAsync(client, CancellationToken.None).ConfigureAwait(false);

            return 0;
        }
        finally
        {
            testRoot.Delete(true);
        }
    }

    private static int NextFreePort()
    {
        using var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        if (listener.LocalEndpoint is not IPEndPoint endpoint)
            throw new InvalidOperationException("Failed to resolve local TCP listener endpoint.");

        return endpoint.Port;
    }

    private static async Task RunExpirationAsync(ISquirixClient client, CancellationToken ct)
    {
        var cache = await client.GetCacheAsync<string>("smoke-expiration", ct).ConfigureAwait(false);
        await cache.SetAsync("expiring", "x", new CacheEntryOptions { Expiration = TimeSpan.FromMilliseconds(80) }, ct).ConfigureAwait(false);
        await Task.Delay(200, ct).ConfigureAwait(false);
        var result = await cache.GetValueAsync("expiring", ct).ConfigureAwait(false);
        if (result.Found)
        {
            throw new InvalidOperationException("Expected expiration key to be absent after wait.");
        }
    }

    private static async Task RunIsolationAsync(ISquirixClient client, CancellationToken ct)
    {
        var a = await client.GetCacheAsync<string>("smoke-a", ct).ConfigureAwait(false);
        var b = await client.GetCacheAsync<string>("smoke-b", ct).ConfigureAwait(false);
        await a.SetAsync(IsolationSharedKey, "from-a", cancellationToken: ct).ConfigureAwait(false);
        await b.SetAsync(IsolationSharedKey, "from-b", cancellationToken: ct).ConfigureAwait(false);
        var v1 = (await a.GetValueAsync(IsolationSharedKey, ct).ConfigureAwait(false)).Value;
        var v2 = (await b.GetValueAsync(IsolationSharedKey, ct).ConfigureAwait(false)).Value;
        if (!string.Equals(v1, "from-a", StringComparison.Ordinal) || !string.Equals(v2, "from-b", StringComparison.Ordinal))
        {
            throw new InvalidOperationException("Named cache isolation failed.");
        }
    }

    private static async Task WriteSettingsAsync(string nodeId, string url, CancellationToken cancellationToken)
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

        await File.WriteAllTextAsync("Squirix.settings.json", JsonSerializer.Serialize(settings), cancellationToken).ConfigureAwait(false);
    }
}
