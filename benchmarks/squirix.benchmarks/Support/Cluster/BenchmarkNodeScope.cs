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
    private readonly TempDirectory? _dir;
    private int _disposed;

    private BenchmarkNodeScope(TestNodeHost host, Uri uri, TempDirectory? dir)
    {
        Host = host;
        Uri = uri;
        _dir = dir;
    }

    internal TestNodeHost Host { get; }

    internal Uri Uri { get; }

    public async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref _disposed, 1) == 1)
            return;

        await Host.DisposeAsync().ConfigureAwait(false);
        _dir?.Dispose();
    }

    internal static Task<BenchmarkNodeScope> StartAsync(CancellationToken cancellationToken, BenchmarkDurabilityMode durabilityMode = BenchmarkDurabilityMode.Ephemeral)
    {
        var nodeId = $"bench-{Guid.NewGuid():N}";
        var uri = ListenPortPool.ServerBenchmarks.HoldHttpUri();
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
        TempDirectory? dir = null;

        TestNodeHost host;
        if (durabilityMode is BenchmarkDurabilityMode.Persistence)
        {
            dir = new TempDirectory("squirix-bench");
            host = await TestNodeHostFactory.StartNodeAsync(nodeId, uri, dir, cancellationToken).ConfigureAwait(false);
        }
        else
        {
            host = await TestNodeHostFactory.StartNodeAsync(nodeId, uri, cancellationToken).ConfigureAwait(false);
        }

        try
        {
            if (!warmUpClient)
                return new BenchmarkNodeScope(host, host.Uri, dir);

            var unused = await BenchmarkClientLease.ConnectAsync(host.Uri, cancellationToken).ConfigureAwait(false);
            await unused.DisposeAsync().ConfigureAwait(false);

            return new BenchmarkNodeScope(host, host.Uri, dir);
        }
        catch (Exception ex) when (ex is InvalidOperationException or IOException)
        {
            await host.DisposeAsync().ConfigureAwait(false);
            dir?.Dispose();
            throw;
        }
    }
}
