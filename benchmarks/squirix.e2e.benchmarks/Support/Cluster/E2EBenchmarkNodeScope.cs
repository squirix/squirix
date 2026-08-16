using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Attributes;
using Squirix.E2EBenchmarks.Scenarios;
using Squirix.E2EBenchmarks.Support.Client;
using Squirix.Server.TestKit.Hosting;
using Squirix.Server.TestKit.IO;
using Squirix.Server.TestKit.Networking;

namespace Squirix.E2EBenchmarks.Support.Cluster;

/// <summary>Owns one in-process Squirix node used as the remote server for end-to-end benchmarks.</summary>
[Immutable]
internal sealed class E2EBenchmarkNodeScope : IAsyncDisposable
{
    private readonly TempDirectory? _dataDir;
    private readonly TestNodeHost _host;
    private int _disposed;

    private E2EBenchmarkNodeScope(TestNodeHost host, Uri uri, TempDirectory? dataDir)
    {
        _host = host;
        Uri = uri;
        _dataDir = dataDir;
    }

    private Uri Uri { get; }

    public async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref _disposed, 1) is 1)
            return;

        await _host.DisposeAsync().ConfigureAwait(false);
        _dataDir?.Dispose();
    }

    internal static Task<E2EBenchmarkNodeScope> StartAsync(CancellationToken cancellationToken, E2EBenchmarkDurabilityMode durabilityMode = E2EBenchmarkDurabilityMode.Ephemeral) =>
        StartAsync(Guid.NewGuid().ToString("N"), durabilityMode, cancellationToken);

    internal Task<E2EBenchmarkClientLease> OpenClientAsync(CancellationToken cancellationToken) => E2EBenchmarkClientLease.ConnectAsync(Uri, cancellationToken);

    private static Task<E2EBenchmarkNodeScope> StartAsync(string scopeId, E2EBenchmarkDurabilityMode durabilityMode, CancellationToken cancellationToken)
    {
        var nodeId = $"bench-{scopeId}";
        var uri = ListenPortPool.EndToEndBenchmarks.NextHttpUri();
        return StartAsync(nodeId, uri, durabilityMode, cancellationToken, true);
    }

    private static async Task<E2EBenchmarkNodeScope> StartAsync(
        string nodeId,
        Uri uri,
        E2EBenchmarkDurabilityMode durabilityMode,
        CancellationToken cancellationToken,
        bool warmUpClient = false)
    {
        TempDirectory? dataDir = null;

        TestNodeHost host;
        if (durabilityMode is E2EBenchmarkDurabilityMode.Persistence)
        {
            dataDir = new TempDirectory("squirix-e2e-bench");
            host = await TestNodeHostFactory.StartNodeAsync(nodeId, uri, dataDir, cancellationToken).ConfigureAwait(false);
        }
        else
        {
            host = await TestNodeHostFactory.StartNodeAsync(nodeId, uri, cancellationToken).ConfigureAwait(false);
        }

        try
        {
            if (!warmUpClient)
                return new E2EBenchmarkNodeScope(host, host.Uri, dataDir);

            var unused = await E2EBenchmarkClientLease.ConnectAsync(host.Uri, cancellationToken).ConfigureAwait(false);
            await unused.DisposeAsync().ConfigureAwait(false);

            return new E2EBenchmarkNodeScope(host, host.Uri, dataDir);
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
