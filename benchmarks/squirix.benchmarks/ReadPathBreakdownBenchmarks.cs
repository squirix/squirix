using System;
using System.Diagnostics.CodeAnalysis;
using System.Threading;
using System.Threading.Tasks;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Engines;
using Squirix.Benchmarks.Infrastructure;
using Squirix.Internal.Cluster.Reliability;
using Squirix.Internal.Cluster.Transport;
using Squirix.Server.TestKit.Benchmarking;
using Squirix.Transport.Grpc.Cache;

namespace Squirix.Benchmarks;

/// <summary>
/// Layer breakdown for the read path using in-process server hooks and internal gRPC stubs (not public e2e APIs).
/// </summary>
[MemoryDiagnoser]
[MinIterationTime(150)]
[SuppressMessage("Performance", "CA1822:Mark members as static", Justification = "BenchmarkDotNet prefers instance members.")]
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
    private BenchmarkClientLease? _publicClient;
    private ICache<string>? _publicSdk;
    private BenchmarkRawGrpcCache? _rawGrpc;
    private GetValueRequest? _reusedRequest;
    private BenchmarkNodeReadSurface? _serverPipeline;

    /// <summary>
    /// Stops benchmark dependencies.
    /// </summary>
    /// <returns>A <see cref="Task" /> that completes when cleanup is finished.</returns>
    [GlobalCleanup]
    public async Task CleanupAsync() => await DisposeAsync().ConfigureAwait(false);

    /// <summary>
    /// Starts an in-process node and seeds keys for breakdown reads.
    /// </summary>
    /// <returns>A <see cref="Task" /> that completes when setup is finished.</returns>
    [GlobalSetup]
    public async Task SetupAsync()
    {
        BenchmarkRuntime.EnsureInitialized();
        SeedKeys();

        _node = await BenchmarkNodeScope.StartAsync(CancellationToken.None).ConfigureAwait(false);
        _serverPipeline = BenchmarkNodeReadSurface.ForCache(_node.Host, CacheName);
        _rawGrpc = BenchmarkRawGrpcCache.Connect(_node.Endpoint, CacheName);
        _clientPool = new ClientPool([new Peer { NodeId = BenchmarkNodeId, Url = _node.Endpoint }], static nodeId => new CallPolicy(peer: nodeId));
        _ = await _clientPool.WarmUpAsync(CancellationToken.None).ConfigureAwait(false);
        _publicClient = await _node.OpenClientAsync(CancellationToken.None).ConfigureAwait(false);
        _publicSdk = await _publicClient.Client.GetCacheAsync<string>(CacheName, CancellationToken.None).ConfigureAwait(false);
        _reusedRequest = new GetValueRequest { CacheName = CacheName };

        await SeedNodeAsync().ConfigureAwait(false);
    }

    /// <summary>
    /// Reads through the client pool and call policy, but without the public cache facade.
    /// </summary>
    /// <returns>A <see cref="Task" /> that completes when the batch is read.</returns>
    [Benchmark(OperationsPerInvoke = ReadBatch, Description = "ClientPool + CallPolicy GetValue, no public facade")]
    public async Task SquirixClientPoolPolicyReadBatched()
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

        static async ValueTask<GetValueResponse> GetValueViaClientAsync(
            SquirixCacheService.SquirixCacheServiceClient client,
            string cacheName,
            string key,
            CancellationToken cancellationToken)
        {
            return await client.GetValueAsync(new GetValueRequest { CacheName = cacheName, Key = key }, cancellationToken: cancellationToken).ResponseAsync.ConfigureAwait(false);
        }
    }

    /// <summary>
    /// Reads through generated gRPC stubs and consumes only the found flag, avoiding client-side value decoding.
    /// </summary>
    /// <returns>A <see cref="Task" /> that completes when the batch is read.</returns>
    [Benchmark(OperationsPerInvoke = ReadBatch, Description = "Raw gRPC GetValue found flag only, no SDK decode")]
    public async Task SquirixGrpcTransportFoundOnlyBatched()
    {
        var cache = _rawGrpc!;
        for (var i = 0; i < ReadBatch; i++)
            _consumer.Consume(await cache.GetValueFoundAsync(_keys[i], CancellationToken.None).ConfigureAwait(false));
    }

    /// <summary>
    /// Reads through generated gRPC stubs while reusing the request instance, isolating per-call request allocation cost.
    /// </summary>
    /// <returns>A <see cref="Task" /> that completes when the batch is read.</returns>
    [Benchmark(OperationsPerInvoke = ReadBatch, Description = "Raw gRPC GetValue found flag, reused request instance")]
    public async Task SquirixGrpcTransportFoundOnlyReusedRequestBatched()
    {
        var cache = _rawGrpc!;
        var request = _reusedRequest!;
        for (var i = 0; i < ReadBatch; i++)
        {
            request.Key = _keys[i];
            _consumer.Consume(await cache.GetValueFoundAsync(request, CancellationToken.None).ConfigureAwait(false));
        }
    }

    /// <summary>
    /// Reads through generated gRPC stubs only, without the public Squirix client SDK stack.
    /// </summary>
    /// <returns>A <see cref="Task" /> that completes when the batch is read.</returns>
    [Benchmark(OperationsPerInvoke = ReadBatch, Description = "Raw gRPC transport + server pipeline, no SDK")]
    public async Task SquirixGrpcTransportReadBatched()
    {
        var cache = _rawGrpc!;
        for (var i = 0; i < ReadBatch; i++)
            _consumer.Consume(await cache.GetValueOrDefaultAsync(_keys[i], CancellationToken.None).ConfigureAwait(false) ?? string.Empty);
    }

    /// <summary>
    /// Reads through the public Squirix client SDK against the same node used by raw gRPC rows.
    /// </summary>
    /// <returns>A <see cref="Task" /> that completes when the batch is read.</returns>
    [Benchmark(OperationsPerInvoke = ReadBatch, Description = "Public SDK GetValue, same node as raw gRPC")]
    public async Task SquirixPublicSdkReadBatched()
    {
        var cache = _publicSdk!;
        for (var i = 0; i < ReadBatch; i++)
            _consumer.Consume((await cache.GetValueAsync(_keys[i], CancellationToken.None).ConfigureAwait(false)).Value ?? string.Empty);
    }

    /// <summary>
    /// Reads through the server-side adapter pipeline without HTTP/2 or the public client SDK.
    /// </summary>
    /// <returns>A <see cref="Task" /> that completes when the batch is read.</returns>
    [Benchmark(Baseline = true, OperationsPerInvoke = ReadBatch, Description = "Server decorator pipeline only, no network")]
    public async Task SquirixServerPipelineReadBatched()
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

        GC.SuppressFinalize(this);
    }

    private void SeedKeys()
    {
        for (var i = 0; i < KeyCount; i++)
            _keys[i] = $"key:{i:D5}";
    }

    private async Task SeedNodeAsync()
    {
        var client = await _node!.OpenClientAsync(CancellationToken.None).ConfigureAwait(false);
        await using (client.ConfigureAwait(false))
        {
            var cache = await client.Client.GetCacheAsync<string>(CacheName, CancellationToken.None).ConfigureAwait(false);
            for (var i = 0; i < KeyCount; i++)
            {
                var key = _keys[i];
                await cache.SetAsync(key, $"value:{i:D5}", cancellationToken: CancellationToken.None).ConfigureAwait(false);
            }
        }
    }
}
