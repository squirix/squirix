using System;
using System.Diagnostics.CodeAnalysis;
using System.Globalization;
using System.Threading;
using System.Threading.Tasks;
using BenchmarkDotNet.Attributes;
using Grpc.Core;
using Grpc.Net.Client;
using Microsoft.Extensions.Caching.Distributed;
using Microsoft.Extensions.Options;
using Squirix.Benchmarks.Support;
using Squirix.Extensions.Caching;
using Squirix.Internal.Cluster.Reliability;
using Squirix.Server.TestKit;
using Squirix.Server.TestKit.Benchmarks;
using Squirix.Server.TestKit.Hosting;
using Squirix.Server.TestKit.Networking;
using Squirix.Transport.Grpc;
using Squirix.Transport.Grpc.Cache;

namespace Squirix.Benchmarks.Server;

/// <summary>Compares owner access, real HTTPS distributed Get RPCs and the official byte-cache adapter on identical live data.</summary>
[MemoryDiagnoser]
public class DistributedReadTransportBenchmarks
{
    private const int Batch = 4096;
    private const string CacheName = "distributed-read-transport";
    private const string NodeId = "distributed-read-node";
    private readonly byte[] _payload = new byte[1024];
    private readonly CancellationTokenSource _rawLifetime = new();
    private SquirixDistributedCache? _adapter;
    private GrpcChannel? _channel;
    private TestCluster<ClusterStartOptions>? _cluster;
    private BenchmarkDistributedReadSurface? _direct;
    private string[] _keys = [];
    private SquirixCacheService.SquirixCacheServiceClient? _raw;

    private enum ReadMode
    {
        Direct = 0,
        RawGrpc = 1,
        Adapter = 2,
        BoundedRawGrpc = 3,
    }

    /// <summary>Gets or sets the number of extra completed mutation outcomes created before measurement.</summary>
    [Params(0, 60000)]
    public int Outcomes { get; set; }

    /// <summary>Gets or sets the number of concurrent logical readers.</summary>
    [Params(1, 8, 32)]
    public int Workers { get; set; }

    /// <summary>Reads through the official IDistributedCache provider and its reliability policy.</summary>
    /// <returns>The validated batch.</returns>
    [Benchmark(OperationsPerInvoke = Batch)]
    public Task AdapterAsync() => ReadBatchAsync(ReadMode.Adapter);

    /// <summary>Closes clients before stopping their server.</summary>
    /// <returns>The asynchronous cleanup.</returns>
    [GlobalCleanup]
    public async Task CleanupAsync()
    {
        if (_adapter != null)
            await _adapter.DisposeAsync().ConfigureAwait(false);
        _channel?.Dispose();
        if (_cluster != null)
            await _cluster.DisposeAsync().ConfigureAwait(false);
        _rawLifetime.Dispose();
    }

    /// <summary>Calls owner access directly, preserving both probes and both response payload copies.</summary>
    /// <returns>The validated batch.</returns>
    [Benchmark(Baseline = true, OperationsPerInvoke = Batch)]
    public Task DirectAsync() => ReadBatchAsync(ReadMode.Direct);

    /// <summary>Reads through the generated GetDistributedAsync stub over HTTPS.</summary>
    /// <returns>The validated batch.</returns>
    [Benchmark(OperationsPerInvoke = Batch)]
    public Task RawGrpcAsync() => ReadBatchAsync(ReadMode.RawGrpc);

    /// <summary>Runs raw gRPC with the same operation-deadline and attempt-timer durations as the official client.</summary>
    /// <returns>The validated batch; failover, retry and policy admission are excluded.</returns>
    [Benchmark(OperationsPerInvoke = Batch)]
    public Task RawGrpcWithBoundsAsync() => ReadBatchAsync(ReadMode.BoundedRawGrpc);

    /// <summary>Starts an ephemeral RF=1 node and seeds identical absolute-expiration values for every reader.</summary>
    /// <returns>The asynchronous setup.</returns>
    [GlobalSetup]
    public async Task SetupAsync()
    {
        var configured = Environment.GetEnvironmentVariable("SQUIRIX_BENCHMARK_URI");
        var uri = string.IsNullOrWhiteSpace(configured) ? ListenPortPool.ServerBenchmarks.HoldHttpUri() : new Uri(configured);
        _cluster = TestCluster<ClusterStartOptions>.Create([new ClusterNode(NodeId, uri)]);
        try
        {
            _ = await _cluster.StartNodeAsync(NodeId, new ClusterStartOptions { EnableReplication = false }, CancellationToken.None).ConfigureAwait(false);
            _direct = BenchmarkDistributedReadSurface.ForCache(_cluster[NodeId], CacheName);
            _channel = GrpcChannel.ForAddress(uri, new GrpcChannelOptions { HttpHandler = LoopbackHttp.CreateHandler(), DisposeHttpClient = true });
            _raw = new SquirixCacheService.SquirixCacheServiceClient(_channel);
            var options = new SquirixCacheOptions { CacheName = CacheName };
            options.ClientOptions.Endpoints.Add(uri);
            _adapter = new SquirixDistributedCache(Options.Create(options));
            _keys = new string[Workers];
            for (var index = 0; index < _payload.Length; index++)
                _payload[index] = byte.CreateTruncating(index);
            for (var worker = 0; worker < Workers; worker++)
            {
                _keys[worker] = worker.ToString(CultureInfo.InvariantCulture);
                await _adapter.SetAsync(_keys[worker], _payload, new DistributedCacheEntryOptions { AbsoluteExpirationRelativeToNow = TimeSpan.FromHours(1) }, CancellationToken.None).ConfigureAwait(false);
            }

            _direct.SeedOutcomes(Outcomes);
            await ReadBatchAsync(ReadMode.Direct).ConfigureAwait(false);
            await ReadBatchAsync(ReadMode.RawGrpc).ConfigureAwait(false);
            await ReadBatchAsync(ReadMode.Adapter).ConfigureAwait(false);
        }
        catch
        {
            await CleanupAsync().ConfigureAwait(false);
            throw;
        }
    }

    private async Task ReadBatchAsync(ReadMode mode)
    {
        var start = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var tasks = new Task[Workers];
        for (var worker = 0; worker < tasks.Length; worker++)
            tasks[worker] = ReadWorkerAsync(worker, mode, start.Task);
        start.SetResult();
        await Task.WhenAll(tasks).ConfigureAwait(false);
    }

    [SuppressMessage("Usage", "VSTHRD003", Justification = "The batch owns and completes the start signal after creating every worker; no synchronization context is required.")]
    private async Task ReadWorkerAsync(int worker, ReadMode mode, Task start)
    {
        await start.ConfigureAwait(false);
        var key = _keys[worker];
        for (var index = 0; index < Batch / Workers; index++)
        {
            var bytes = mode switch
            {
                ReadMode.Direct => await _direct!.GetAsync(key, Guid.NewGuid().ToString("N"), CancellationToken.None).ConfigureAwait(false),
                ReadMode.RawGrpc => await GetRawAsync(key).ConfigureAwait(false),
                ReadMode.BoundedRawGrpc => await GetBoundedRawAsync(key).ConfigureAwait(false),
                _ => await _adapter!.GetAsync(key, CancellationToken.None).ConfigureAwait(false),
            };
            if (bytes?.AsSpan().SequenceEqual(_payload) != true)
                throw new InvalidOperationException("Every layer must return the exact seeded payload.");
        }
    }

    private async Task<byte[]?> GetRawAsync(string key)
    {
        var raw = BenchmarkThrowHelper.Required(_raw, "Setup must run first.");
        var request = new GetDistributedAsyncRequest
        {
            CacheName = CacheName,
            Key = key,
            OperationId = Guid.NewGuid().ToString("N"),
        };
        var reply = await raw.GetDistributedAsync(request, cancellationToken: CancellationToken.None).ResponseAsync.ConfigureAwait(false);
        return reply.Found ? reply.Value.ToByteArray() : null;
    }

    private async Task<byte[]?> GetBoundedRawAsync(string key)
    {
        var raw = BenchmarkThrowHelper.Required(_raw, "Setup must run first.");
        var request = new GetDistributedAsyncRequest
        {
            CacheName = CacheName,
            Key = key,
            OperationId = Guid.NewGuid().ToString("N"),
        };
        var deadline = TimeProvider.System.GetUtcNow().UtcDateTime + CallPolicyDefaults.OperationDeadline;
        using var attempt = new TimedCancellation(RpcAttemptTimeout.PerAttempt, TimeProvider.System, _rawLifetime.Token);
        var reply = await raw.GetDistributedAsync(request, new CallOptions(deadline: deadline, cancellationToken: attempt.Token)).ResponseAsync.ConfigureAwait(false);
        return reply.Found ? reply.Value.ToByteArray() : null;
    }
}
