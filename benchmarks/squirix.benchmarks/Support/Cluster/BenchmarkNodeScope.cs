using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Attributes;
using Squirix.Benchmarks.Support.Client;
using Squirix.Server.TestKit.Hosting;
using Squirix.Server.TestKit.IO;
using Squirix.Server.TestKit.Networking;

namespace Squirix.Benchmarks.Support.Cluster;

/// <summary>Owns one in-process Squirix node used as the remote server for client SDK benchmarks.</summary>
[Immutable]
internal sealed class BenchmarkNodeScope : IAsyncDisposable
{
    private readonly TempDirectory? _dataDir;
    private int _disposed;

    private BenchmarkNodeScope(TestNodeHost host, Uri uri, TempDirectory? dataDir)
    {
        Host = host;
        Uri = uri;
        _dataDir = dataDir;
    }

    internal TestNodeHost Host { get; }

    internal Uri Uri { get; }

    public async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref _disposed, 1) is 1)
            return;

        await Host.DisposeAsync().ConfigureAwait(false);
        _dataDir?.Dispose();
    }

    internal static Task<BenchmarkNodeScope> StartAsync(CancellationToken cancellationToken, BenchmarkDurabilityMode durabilityMode = BenchmarkDurabilityMode.Ephemeral)
    {
        var nodeId = $"bench-{Guid.NewGuid():N}";
        var uri = ListenPortPool.ServerBenchmarks.NextHttpUri();
        return StartAsync(nodeId, uri, durabilityMode, cancellationToken, true);
    }

    internal Task<BenchmarkClientLease> OpenClientAsync(CancellationToken cancellationToken) => BenchmarkClientLease.ConnectAsync(Uri, cancellationToken);

    private static async Task<BenchmarkNodeScope> StartAsync(
        string nodeId,
        Uri uri,
        BenchmarkDurabilityMode durabilityMode,
        CancellationToken cancellationToken,
        bool warmUpClient = false)
    {
        TempDirectory? dataDir = null;

        TestNodeHost host;
        if (durabilityMode is BenchmarkDurabilityMode.Persistence)
        {
            dataDir = new TempDirectory("squirix-bench");
            host = await TestNodeHostFactory.StartNodeAsync(nodeId, uri, dataDir, cancellationToken).ConfigureAwait(false);
        }
        else
        {
            host = await TestNodeHostFactory.StartNodeAsync(nodeId, uri, cancellationToken).ConfigureAwait(false);
        }

        try
        {
            if (!warmUpClient)
                return new BenchmarkNodeScope(host, host.Uri, dataDir);

            var unused = await BenchmarkClientLease.ConnectAsync(host.Uri, cancellationToken).ConfigureAwait(false);
            await unused.DisposeAsync().ConfigureAwait(false);

            return new BenchmarkNodeScope(host, host.Uri, dataDir);
        }
        catch (InvalidOperationException)
        {
            await host.DisposeAsync().ConfigureAwait(false);
            dataDir?.Dispose();
            throw;
        }
        catch (IOException)
        {
            await host.DisposeAsync().ConfigureAwait(false);
            dataDir?.Dispose();
            throw;
        }
    }
}
