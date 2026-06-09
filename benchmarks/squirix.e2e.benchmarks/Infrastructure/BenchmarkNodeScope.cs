using System;
using System.IO;
using System.Net;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;
using Squirix.E2EBenchmarks.Utils;
using Squirix.Server.TestKit.AspNetCore;

namespace Squirix.E2EBenchmarks.Infrastructure;

/// <summary>
/// Owns one in-process Squirix node used as the remote server for end-to-end benchmarks.
/// </summary>
internal sealed class BenchmarkNodeScope : IAsyncDisposable
{
    private readonly string _dataDir;
    private readonly TestNodeHost _host;
    private int _disposed;

    private BenchmarkNodeScope(TestNodeHost host, string dataDir, string endpoint)
    {
        _host = host;
        _dataDir = dataDir;
        Endpoint = endpoint;
    }

    private string Endpoint { get; }

    public async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref _disposed, 1) == 1)
            return;

        try
        {
            await _host.DisposeAsync().ConfigureAwait(false);
        }
        finally
        {
            DirectoryKit.TryDeleteDirectory(_dataDir);
        }
    }

    internal static Task<BenchmarkNodeScope> StartAsync(CancellationToken cancellationToken)
    {
        var nodeId = $"bench-{Guid.NewGuid():N}";
        var address = $"https://127.0.0.1:{AllocatePort()}";
        return StartAsync(nodeId, address, [(nodeId, address)], cancellationToken, true);
    }

    internal Task<BenchmarkClientLease> OpenClientAsync(CancellationToken cancellationToken) => BenchmarkClientLease.ConnectAsync(Endpoint, cancellationToken);

    private static async Task<BenchmarkNodeScope> StartAsync(
        string nodeId,
        string address,
        (string NodeId, string Address)[] topology,
        CancellationToken cancellationToken,
        bool warmUpClient = false)
    {
        BenchmarkRuntime.EnsureInitialized();

        var dataDir = PathKit.Combine(Path.GetTempPath(), $"squirix-e2e-bench-{Guid.NewGuid():N}");
        _ = Directory.CreateDirectory(dataDir);

        var host = await TestNodeHostFactory.StartNodeAsync(nodeId, address, topology, dataDir, cancellationToken).ConfigureAwait(false);

        try
        {
            if (warmUpClient)
            {
                await using var unused = await BenchmarkClientLease.ConnectAsync(host.Address, cancellationToken).ConfigureAwait(false);
            }

            return new BenchmarkNodeScope(host, dataDir, host.Address);
        }
        catch
        {
            await host.DisposeAsync().ConfigureAwait(false);
            DirectoryKit.TryDeleteDirectory(dataDir);
            throw;
        }
    }

    private static int AllocatePort()
    {
        using var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        var port = ((IPEndPoint)listener.LocalEndpoint).Port;
        listener.Stop();
        return port;
    }
}
