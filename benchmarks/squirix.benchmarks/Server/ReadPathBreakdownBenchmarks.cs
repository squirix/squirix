using System;
using System.Threading;
using System.Threading.Tasks;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Engines;
using Squirix.Benchmarks.Support.Client;
using Squirix.Benchmarks.Support.Cluster;
using Squirix.Benchmarks.Support.Grpc;
using Squirix.Internal.Cluster.Reliability;
using Squirix.Internal.Cluster.Transport;
using Squirix.Server.TestKit;
using Squirix.Server.TestKit.Benchmarks;
using Squirix.Transport.Grpc.Cache;

namespace Squirix.Benchmarks.Server;

/// <summary>Layer breakdown for the read path using in-process server hooks and internal gRPC stubs (not public e2e APIs).</summary>
[MemoryDiagnoser]
[MinIterationTime(150)]
public class ReadPathBreakdownBenchmarks : IAsyncDisposable
{
    private const string BenchmarkNodeId = "bench-client-pool-node";
    private const string CacheName = "bench-read-path-breakdown";
    private const int KeyCount = 8_192;
    private const int ReadBatch = 1_024;

    private readonly Consumer _consumer = new();
    private readonly string[] _keys = new string[KeyCount];
    private ClientPool? _clientPool;
    private BenchmarkNodeScope? _node;
    private Peer[]? _peers;
    private BenchmarkClientLease? _publicClient;
    private ICache<string>? _publicSdk;
    private BenchmarkRawGrpcCache? _rawGrpc;
    private GetValueAsyncRequest? _reusedRequest;
    private BenchmarkNodeReadSurface? _serverPipeline;

    /// <summary>Stops benchmark dependencies.</summary>
    [GlobalCleanup]
    public ValueTask CleanupAsync() => DisposeAsync();

    /// <summary>Starts an in-process node and seeds keys for breakdown reads.</summary>
    [GlobalSetup]
    public async Task SetupAsync()
    {
        SeedKeys();

        _node = await BenchmarkNodeScope.StartAsync(CancellationToken.None).ConfigureAwait(false);
        _serverPipeline = BenchmarkNodeReadSurface.ForCache(_node.Host, CacheName);
        _rawGrpc = BenchmarkRawGrpcCache.Connect(_node.Uri, CacheName);
        _peers = new Peer[1];
        _peers[0] = new Peer { NodeId = BenchmarkNodeId, Uri = _node.Uri };
        _clientPool = new ClientPool(_peers, static nodeId => new CallPolicy(peer: nodeId));
        _ = await _clientPool.WarmUpAsync(CancellationToken.None).ConfigureAwait(false);
        _publicClient = await _node.OpenClientAsync(CancellationToken.None).ConfigureAwait(false);
        _publicSdk = await _publicClient.Client.GetCacheAsync<string>(CacheName, CancellationToken.None).ConfigureAwait(false);
        _reusedRequest = new GetValueAsyncRequest { CacheName = CacheName };

        await SeedNodeAsync().ConfigureAwait(false);
    }

    /// <summary>Reads through the client pool and call policy, but without the public cache facade.</summary>
    [Benchmark(OperationsPerInvoke = ReadBatch, Description = "ClientPool + CallPolicy GetValue, no public facade")]
    public async Task SquirixClientPoolPolicyReadBatchedAsync()
    {
        var pool = _clientPool!;
        for (var i = 0; i < ReadBatch; i++)
        {
            var response = await pool.PolicyFor(BenchmarkNodeId).ExecuteAsync(
                static (state, ct) => GetValueViaClientAsync(state.Client, state.CacheName, state.Key, ct),
                (Client: pool.ForNode(BenchmarkNodeId), CacheName, Key: _keys[i]),
                CancellationToken.None).ConfigureAwait(false);
            _consumer.Consume(response.Found ? response.Value.StringValue : string.Empty);
        }

        return;

        static async ValueTask<GetValueAsyncResponse> GetValueViaClientAsync(
            SquirixCacheService.SquirixCacheServiceClient client,
            string cacheName,
            string key,
            CancellationToken cancellationToken)
        {
            return await client.GetValueAsync(new GetValueAsyncRequest { CacheName = cacheName, Key = key }, cancellationToken: cancellationToken).ResponseAsync
                               .ConfigureAwait(false);
        }
    }

    /// <summary>Reads through generated gRPC stubs while reusing the request instance, isolating per-call request allocation cost.</summary>
    [Benchmark(OperationsPerInvoke = ReadBatch, Description = "Raw gRPC GetValue found flag, reused request instance")]
    public async Task SquirixGrpcFoundOnlyReusedBatchedAsync()
    {
        var cache = _rawGrpc!;
        var request = _reusedRequest!;
        for (var i = 0; i < ReadBatch; i++)
        {
            request.Key = _keys[i];
            _consumer.Consume(await cache.GetValueFoundAsync(request, CancellationToken.None).ConfigureAwait(false));
        }
    }

    /// <summary>Reads through generated gRPC stubs and consumes only the found flag, avoiding client-side value decoding.</summary>
    [Benchmark(OperationsPerInvoke = ReadBatch, Description = "Raw gRPC GetValue found flag only, no SDK decode")]
    public async Task SquirixGrpcTransportFoundOnlyBatchedAsync()
    {
        var cache = _rawGrpc!;
        for (var i = 0; i < ReadBatch; i++)
            _consumer.Consume(await cache.GetValueFoundAsync(_keys[i], CancellationToken.None).ConfigureAwait(false));
    }

    /// <summary>Reads through generated gRPC stubs only, without the public Squirix client SDK stack.</summary>
    [Benchmark(OperationsPerInvoke = ReadBatch, Description = "Raw gRPC transport + server pipeline, no SDK")]
    public async Task SquirixGrpcTransportReadBatchedAsync()
    {
        var cache = _rawGrpc!;
        for (var i = 0; i < ReadBatch; i++)
            _consumer.Consume(await cache.GetValueOrDefaultAsync(_keys[i], CancellationToken.None).ConfigureAwait(false) ?? string.Empty);
    }

    /// <summary>Reads through the public Squirix client SDK against the same node used by raw gRPC rows.</summary>
    [Benchmark(OperationsPerInvoke = ReadBatch, Description = "Public SDK GetValue, same node as raw gRPC")]
    public async Task SquirixPublicSdkReadBatchedAsync()
    {
        var cache = _publicSdk!;
        for (var i = 0; i < ReadBatch; i++)
            _consumer.Consume((await cache.GetValueAsync(_keys[i], CancellationToken.None).ConfigureAwait(false)).Value ?? string.Empty);
    }

    /// <summary>Reads through the server-side adapter pipeline without HTTP/2 or the public client SDK.</summary>
    [Benchmark(Baseline = true, OperationsPerInvoke = ReadBatch, Description = "Server decorator pipeline only, no network")]
    public async Task SquirixServerPipelineReadBatchedAsync()
    {
        var cache = _serverPipeline!;
        for (var i = 0; i < ReadBatch; i++)
            _consumer.Consume(await cache.GetValueOrDefaultAsync(_keys[i], CancellationToken.None).ConfigureAwait(false) ?? string.Empty);
    }

    /// <inheritdoc />
    public async ValueTask DisposeAsync()
    {
        if (_rawGrpc is not null)
        {
            await _rawGrpc.DisposeAsync().ConfigureAwait(false);
            _rawGrpc = null;
        }

        if (_publicClient is not null)
        {
            await _publicClient.DisposeAsync().ConfigureAwait(false);
            _publicClient = null;
        }

        if (_clientPool is not null)
        {
            await _clientPool.DisposeAsync().ConfigureAwait(false);
            _clientPool = null;
        }

        if (_node is not null)
        {
            await _node.DisposeAsync().ConfigureAwait(false);
            _node = null;
        }

        _peers = null;

        GC.SuppressFinalize(this);
    }

    private static string FormatKey(int index) => InvariantIndexStrings.FormatPrefixedPadded("key", index, "D5", 5);

    private static string FormatValue(int index) => InvariantIndexStrings.FormatPrefixedPadded("value", index, "D5", 5);

    private void SeedKeys()
    {
        for (var i = 0; i < KeyCount; i++)
            _keys[i] = FormatKey(i);
    }

    private async Task SeedNodeAsync()
    {
        if (_node is not null)
        {
            var client = await _node.OpenClientAsync(CancellationToken.None).ConfigureAwait(false);
            await using (client.ConfigureAwait(false))
            {
                var cache = await client.Client.GetCacheAsync<string>(CacheName, CancellationToken.None).ConfigureAwait(false);
                for (var i = 0; i < KeyCount; i++)
                {
                    var key = _keys[i];
                    await cache.SetAsync(key, FormatValue(i), cancellationToken: CancellationToken.None).ConfigureAwait(false);
                }
            }
        }
    }
}
