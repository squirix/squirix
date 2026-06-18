using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Benchmarks.Support.Client;
using Squirix.Benchmarks.Support.IO;
using Squirix.Benchmarks.Support.Runtime;
using Squirix.Server.TestKit.Hosting;
using Squirix.Server.TestKit.Networking;

namespace Squirix.Benchmarks.Support.Cluster;

/// <summary>Owns one in-process Squirix node used as the remote server for client SDK benchmarks.</summary>
internal sealed class BenchmarkNodeScope : IAsyncDisposable
{
    private int _disposed;

    private BenchmarkNodeScope(TestNodeHost host, string endpoint)
    {
        Host = host;
        Endpoint = endpoint;
    }

    internal string Endpoint { get; }

    internal TestNodeHost Host { get; }

    public async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref _disposed, 1) is 1)
            return;

        await Host.DisposeAsync().ConfigureAwait(false);
    }

    internal static Task<BenchmarkNodeScope> StartAsync(CancellationToken cancellationToken, BenchmarkDurabilityMode durabilityMode = BenchmarkDurabilityMode.Ephemeral)
    {
        var nodeId = $"bench-{Guid.NewGuid():N}";
        var address = ListenPortPool.ServerBenchmarks.NextHttpUri().AbsoluteUri;
        return StartAsync(nodeId, address, [(nodeId, address)], durabilityMode, cancellationToken, true);
    }

    internal Task<BenchmarkClientLease> OpenClientAsync(CancellationToken cancellationToken) => BenchmarkClientLease.ConnectAsync(Endpoint, cancellationToken);

    private static async Task<BenchmarkNodeScope> StartAsync(
        string nodeId,
        string address,
        (string NodeId, string Address)[] topology,
        BenchmarkDurabilityMode durabilityMode,
        CancellationToken cancellationToken,
        bool warmUpClient = false)
    {
        BenchmarkRuntime.EnsureInitialized();

        var usePersistence = durabilityMode is BenchmarkDurabilityMode.Persistence;
        var dataDir = usePersistence ? DirectoryKit.CreateTempDirectory("squirix-bench") : null;

        var host = usePersistence ? await TestNodeHostFactory.StartNodeAsync(nodeId, address, topology, dataDir, cancellationToken).ConfigureAwait(false)
            : await TestNodeHostFactory.StartNodeAsync(nodeId, address, topology, cancellationToken).ConfigureAwait(false);

        try
        {
            if (!warmUpClient)
                return new BenchmarkNodeScope(host, host.Address);

            var unused = await BenchmarkClientLease.ConnectAsync(host.Address, cancellationToken).ConfigureAwait(false);
            await unused.DisposeAsync().ConfigureAwait(false);

            return new BenchmarkNodeScope(host, host.Address);
        }
        catch (InvalidOperationException)
        {
            await host.DisposeAsync().ConfigureAwait(false);
            DirectoryKit.TryDeleteDirectory(dataDir);
            throw;
        }
        catch (IOException)
        {
            await host.DisposeAsync().ConfigureAwait(false);
            DirectoryKit.TryDeleteDirectory(dataDir);
            throw;
        }
    }
}
