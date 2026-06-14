using System;
using System.IO;
using System.Net;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;
using Squirix.E2EBenchmarks.Scenarios;
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
            if (!string.IsNullOrWhiteSpace(_dataDir))
                DirectoryKit.TryDeleteDirectory(_dataDir);
        }
    }

    internal static Task<BenchmarkNodeScope> StartAsync(CancellationToken cancellationToken, E2EBenchmarkDurabilityMode durabilityMode = E2EBenchmarkDurabilityMode.Ephemeral) =>
        StartAsync(Guid.NewGuid().ToString("N"), durabilityMode, cancellationToken);

    internal Task<BenchmarkClientLease> OpenClientAsync(CancellationToken cancellationToken) => BenchmarkClientLease.ConnectAsync(Endpoint, cancellationToken);

    private static int AllocatePort()
    {
        using var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        var port = ((IPEndPoint)listener.LocalEndpoint).Port;
        listener.Stop();
        return port;
    }

    private static Task<BenchmarkNodeScope> StartAsync(string scopeId, E2EBenchmarkDurabilityMode durabilityMode, CancellationToken cancellationToken)
    {
        var nodeId = $"bench-{scopeId}";
        var address = $"https://127.0.0.1:{AllocatePort()}";
        return StartAsync(nodeId, address, [(nodeId, address)], durabilityMode, cancellationToken, true);
    }

    private static async Task<BenchmarkNodeScope> StartAsync(
        string nodeId,
        string address,
        (string NodeId, string Address)[] topology,
        E2EBenchmarkDurabilityMode durabilityMode,
        CancellationToken cancellationToken,
        bool warmUpClient = false)
    {
        BenchmarkRuntime.EnsureInitialized();

        var usePersistence = durabilityMode == E2EBenchmarkDurabilityMode.Persistence;
        var dataDir = usePersistence ? PathKit.Combine(Path.GetTempPath(), $"squirix-e2e-bench-{Guid.NewGuid():N}") : string.Empty;
        if (usePersistence)
            _ = Directory.CreateDirectory(dataDir);

        var host = usePersistence ? await TestNodeHostFactory.StartNodeAsync(nodeId, address, topology, dataDir, cancellationToken).ConfigureAwait(false)
            : await TestNodeHostFactory.StartNodeAsync(nodeId, address, topology, cancellationToken).ConfigureAwait(false);

        try
        {
            if (!warmUpClient)
                return new BenchmarkNodeScope(host, dataDir, host.Address);

            var unused = await BenchmarkClientLease.ConnectAsync(host.Address, cancellationToken).ConfigureAwait(false);
            await unused.DisposeAsync().ConfigureAwait(false);

            return new BenchmarkNodeScope(host, dataDir, host.Address);
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
