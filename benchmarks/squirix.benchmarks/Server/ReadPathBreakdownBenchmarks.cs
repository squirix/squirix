using System;
using System.Threading;
using System.Threading.Tasks;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Engines;
using Squirix.Benchmarks.Support.Binary;
using Squirix.Benchmarks.Support.Client;
using Squirix.Benchmarks.Support.Cluster;
using Squirix.Internal.Cluster.Reliability;
using Squirix.Internal.Cluster.Transport;
using Squirix.Server.TestKit;
using Squirix.Server.TestKit.Benchmarks;
using Squirix.Server.TestKit.Hosting;
using Squirix.Transport.Wire.Cache;

namespace Squirix.Benchmarks.Server;

/// <summary>Layer breakdown for the read path using in-process server hooks and binary transport calls (not public e2e APIs).</summary>
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
    private TestCluster<ClusterStartOptions>? _cluster;
    private BenchmarkClientLease? _publicClient;
    private ICache<string>? _publicSdk;
    private BenchmarkRawBinaryCache? _rawBinary;
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

        _cluster = await BenchmarkNodeCluster.StartAsync(cancellationToken: CancellationToken.None).ConfigureAwait(false);
        _serverPipeline = BenchmarkNodeReadSurface.ForCache(_cluster.Host(), CacheName);
        _rawBinary = BenchmarkRawBinaryCache.Connect(_cluster.Uri(), CacheName);
        _clientPool = new ClientPool([new Peer { NodeId = BenchmarkNodeId, Uri = _cluster.Uri() }], static nodeId => new CallPolicy(TimeProvider.System, peer: nodeId), TimeProvider.System);
        _ = await _clientPool.WarmUpAsync(CancellationToken.None).ConfigureAwait(false);
        _publicClient = await _cluster.OpenClientAsync(CancellationToken.None).ConfigureAwait(false);
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
            var node = pool.ForNode(BenchmarkNodeId);
            var response = await node.Policy.ExecuteAsync(
                static (state, ct) => GetValueViaClientAsync(state.Client, state.CacheName, state.Key, ct),
                (node.Client, CacheName, Key: _keys[i]),
                CancellationToken.None).ConfigureAwait(false);
            _consumer.Consume(response.Found ? response.Value!.StringValue : string.Empty);
        }

        return;

        static ValueTask<GetValueAsyncResponse> GetValueViaClientAsync(
            BinaryCacheClient client,
            string cacheName,
            string key,
            CancellationToken cancellationToken)
        {
            return client.GetValueAsync(new GetValueAsyncRequest { CacheName = cacheName, Key = key }, new BinaryCallOptions(DateTime.MaxValue, cancellationToken, cancellationToken));
        }
    }

    /// <summary>Reads through the binary transport client while reusing the request instance, isolating per-call request allocation cost.</summary>
    [Benchmark(OperationsPerInvoke = ReadBatch, Description = "Raw binary GetValue found flag, reused request instance")]
    public async Task SquirixBinaryFoundOnlyReusedBatchedAsync()
    {
        var cache = _rawBinary!;
        var request = _reusedRequest!;
        for (var i = 0; i < ReadBatch; i++)
        {
            request.Key = _keys[i];
            _consumer.Consume(await cache.GetValueFoundAsync(request, CancellationToken.None).ConfigureAwait(false));
        }
    }

    /// <summary>Reads through the binary transport client and consumes only the found flag, avoiding client-side value decoding.</summary>
    [Benchmark(OperationsPerInvoke = ReadBatch, Description = "Raw binary GetValue found flag only, no SDK decode")]
    public async Task TransportFoundOnlyBatchedAsync()
    {
        var cache = _rawBinary!;
        for (var i = 0; i < ReadBatch; i++)
            _consumer.Consume(await cache.GetValueFoundAsync(_keys[i], CancellationToken.None).ConfigureAwait(false));
    }

    /// <summary>Reads through the binary transport client only, without the public Squirix client SDK stack.</summary>
    [Benchmark(OperationsPerInvoke = ReadBatch, Description = "Raw binary transport + server pipeline, no SDK")]
    public async Task SquirixBinaryTransportReadBatchedAsync()
    {
        var cache = _rawBinary!;
        for (var i = 0; i < ReadBatch; i++)
            _consumer.Consume(await cache.GetValueOrDefaultAsync(_keys[i], CancellationToken.None).ConfigureAwait(false) ?? string.Empty);
    }

    /// <summary>Reads through the public Squirix client SDK against the same node used by raw binary rows.</summary>
    [Benchmark(OperationsPerInvoke = ReadBatch, Description = "Public SDK GetValue, same node as raw binary")]
    public async Task SquirixPublicSdkReadBatchedAsync()
    {
        var cache = _publicSdk!;
        for (var i = 0; i < ReadBatch; i++)
            _consumer.Consume((await cache.GetValueAsync(_keys[i], CancellationToken.None).ConfigureAwait(false)).Value ?? string.Empty);
    }

    /// <summary>Reads through the server-side adapter pipeline without network I/O or the public client SDK.</summary>
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
        if (_rawBinary != null)
        {
            await _rawBinary.DisposeAsync().ConfigureAwait(false);
            _rawBinary = null;
        }

        if (_publicClient != null)
        {
            await _publicClient.DisposeAsync().ConfigureAwait(false);
            _publicClient = null;
        }

        if (_clientPool != null)
        {
            await _clientPool.DisposeAsync().ConfigureAwait(false);
            _clientPool = null;
        }

        if (_cluster != null)
        {
            await _cluster.DisposeAsync().ConfigureAwait(false);
            _cluster = null;
        }

        GC.SuppressFinalize(this);
    }

    private static string FormatKey(int index) => NodeInvariantIndexStrings.FormatPrefixedPadded("key", index, "D5", 5);

    private static string FormatValue(int index) => NodeInvariantIndexStrings.FormatPrefixedPadded("value", index, "D5", 5);

    private void SeedKeys()
    {
        for (var i = 0; i < KeyCount; i++)
            _keys[i] = FormatKey(i);
    }

    private async Task SeedNodeAsync()
    {
        if (_cluster != null)
        {
            var client = await _cluster.OpenClientAsync(CancellationToken.None).ConfigureAwait(false);
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
