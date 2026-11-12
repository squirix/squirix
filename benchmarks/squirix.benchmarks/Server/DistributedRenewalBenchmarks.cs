using System;
using System.Threading;
using System.Threading.Tasks;
using BenchmarkDotNet.Attributes;
using Google.Protobuf;
using Grpc.Net.Client;
using Squirix.Benchmarks.Support;
using Squirix.Server.TestKit;
using Squirix.Server.TestKit.Hosting;
using Squirix.Server.TestKit.IO;
using Squirix.Server.TestKit.Networking;
using Squirix.Testing;
using Squirix.Transport.Grpc.Cache;

namespace Squirix.Benchmarks.Server;

/// <summary>Compares read-only access and renewal over loopback gRPC on journaled nodes.</summary>
[BenchmarkCategory("e2e", "distributed-cache")]
public class DistributedRenewalBenchmarks
{
    private const string CacheName = "distributed-renewal";
    private GrpcChannel? _channel;
    private SquirixCacheService.SquirixCacheServiceClient? _client;
    private TestCluster<ClusterStartOptions>? _cluster;
    private TempDirectory? _directory;
    private string _key = string.Empty;

    /// <summary>Gets or sets the number of value bytes.</summary>
    [Params(1024, 65536)]
    public int PayloadBytes { get; set; }

    /// <summary>Gets or sets the replica count for the comparison.</summary>
    [Params(1, 3)]
    public int Replicas { get; set; }

    private SquirixCacheService.SquirixCacheServiceClient Client => BenchmarkThrowHelper.Required(_client, "Benchmark setup must run first.");

    /// <summary>Reads a value with absolute expiration.</summary>
    [Benchmark(Baseline = true)]
    public Task<int> AbsoluteGetAsync() => GetAsync();

    /// <summary>Closes the channel, stops the cluster and releases its data directory.</summary>
    [GlobalCleanup]
    public async Task CleanupAsync()
    {
        _channel?.Dispose();
        if (_cluster != null)
            await _cluster.DisposeAsync().ConfigureAwait(false);
        _directory?.Dispose();
    }

    /// <summary>Renews the same sliding entry without returning its payload.</summary>
    [Benchmark]
    public async Task RefreshAsync()
    {
        var request = new RefreshDistributedAsyncRequest
        {
            CacheName = CacheName,
            Key = _key,
            OperationId = Guid.NewGuid().ToString("N"),
        };
        _ = await Client.RefreshDistributedAsync(request, cancellationToken: CancellationToken.None).ResponseAsync.ConfigureAwait(false);
    }

    /// <summary>Starts the cluster and seeds an entry with absolute expiration.</summary>
    [GlobalSetup(Target = nameof(AbsoluteGetAsync))]
    public Task SetupAbsoluteAsync() => SetupAsync(false);

    /// <summary>Starts the cluster and seeds an entry with sliding expiration.</summary>
    [GlobalSetup(Targets = [nameof(SlidingGetAsync), nameof(RefreshAsync)])]
    public Task SetupSlidingAsync() => SetupAsync(true);

    /// <summary>Reads and renews a value with sliding expiration.</summary>
    [Benchmark]
    public Task<int> SlidingGetAsync() => GetAsync();

    private async Task<int> GetAsync()
    {
        var request = new GetDistributedAsyncRequest
        {
            CacheName = CacheName,
            Key = _key,
            OperationId = Guid.NewGuid().ToString("N"),
        };
        var response = await Client.GetDistributedAsync(request, cancellationToken: CancellationToken.None).ResponseAsync.ConfigureAwait(false);
        return response.Found ? response.Value.Length : throw new InvalidOperationException("The seeded entry must be present.");
    }

    private async Task SetupAsync(bool sliding)
    {
        using var first = ListenPortPool.EndToEndBenchmarks.HoldPort();
        using var second = ListenPortPool.EndToEndBenchmarks.HoldPort();
        using var third = ListenPortPool.EndToEndBenchmarks.HoldPort();
        _directory = new TempDirectory("squirix-distributed-renewal");
        ClusterNode[] topology = Replicas == 1
            ? [new ClusterNode("nodeA", first.HttpUri)]
            : [new ClusterNode("nodeA", first.HttpUri), new ClusterNode("nodeB", second.HttpUri), new ClusterNode("nodeC", third.HttpUri)];
        _cluster = TestCluster<ClusterStartOptions>.Create(topology);
        try
        {
            ClusterStartOptions StartOptions(string node)
            {
                return new ClusterStartOptions { ReplicaCount = Replicas, DataDir = PathKit.Combine(_directory, node) };
            }

            _ = await _cluster.StartAllAsync(StartOptions, cancellationToken: CancellationToken.None).ConfigureAwait(false);
            _channel = GrpcChannel.ForAddress(first.HttpUri, new GrpcChannelOptions
            {
                HttpHandler = LoopbackHttp.CreateHandler(),
                DisposeHttpClient = true,
            });
            _client = new SquirixCacheService.SquirixCacheServiceClient(_channel);
            var payload = ByteString.CopyFrom(new byte[PayloadBytes]);
            _key = _cluster["nodeA"].FindKeyOwnedBy(CacheName, "nodeA");
            var request = new SetDistributedAsyncRequest
            {
                CacheName = CacheName,
                Key = _key,
                Value = payload,
                OperationId = Guid.NewGuid().ToString("N"),
            };
            if (sliding)
                request.SlidingTicks = TimeSpan.FromHours(1).Ticks;
            else
                request.ExpiresAtUnixMs = TimeProvider.System.GetUtcNow().AddHours(1).ToUnixTimeMilliseconds();

            _ = await Client.SetDistributedAsync(request, cancellationToken: CancellationToken.None).ResponseAsync.ConfigureAwait(false);
        }
        catch
        {
            await CleanupAsync().ConfigureAwait(false);
            throw;
        }
    }
}
