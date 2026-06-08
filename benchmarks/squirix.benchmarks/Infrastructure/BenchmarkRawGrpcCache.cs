using System;
using System.Threading;
using System.Threading.Tasks;
using Grpc.Net.Client;
using Squirix.Serialization;
using Squirix.Server.TestKit.Http;
using Squirix.Transport.Grpc.Cache;
using Squirix.Utils;

namespace Squirix.Benchmarks.Infrastructure;

/// <summary>
/// Reads through generated gRPC stubs only, without the public Squirix client SDK stack.
/// </summary>
internal sealed class BenchmarkRawGrpcCache : IAsyncDisposable
{
    private static readonly ISquirixSerializer Serializer = new SystemTextJsonSerializer();

    private readonly string _cacheName;
    private readonly SquirixCacheService.SquirixCacheServiceClient _client;
    private readonly GrpcChannel _channel;
    private int _disposed;

    private BenchmarkRawGrpcCache(GrpcChannel channel, SquirixCacheService.SquirixCacheServiceClient client, string cacheName)
    {
        _channel = channel;
        _client = client;
        _cacheName = cacheName;
    }

    public ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref _disposed, 1) == 1)
            return ValueTask.CompletedTask;

        _channel.Dispose();
        return ValueTask.CompletedTask;
    }

    internal static BenchmarkRawGrpcCache Connect(string endpoint, string cacheName)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(endpoint);
        ArgumentException.ThrowIfNullOrWhiteSpace(cacheName);

        BenchmarkRuntime.EnsureInitialized();
        var channel = GrpcChannel.ForAddress(
            endpoint,
            new GrpcChannelOptions
            {
                HttpHandler = LoopbackHttp.CreateHandler(),
            });

        return new BenchmarkRawGrpcCache(channel, new SquirixCacheService.SquirixCacheServiceClient(channel), cacheName);
    }

    internal async ValueTask<string?> GetValueOrDefaultAsync(string key, CancellationToken cancellationToken)
    {
        var response = await _client.GetValueAsync(new GetValueRequest { CacheName = _cacheName, Key = key }, cancellationToken: cancellationToken).ConfigureAwait(false);

        return response.Found ? ProtoEx.FromCacheValue<string>(response.Value, Serializer) : null;
    }

    internal async ValueTask<bool> GetValueFoundAsync(string key, CancellationToken cancellationToken)
    {
        var response = await _client.GetValueAsync(new GetValueRequest { CacheName = _cacheName, Key = key }, cancellationToken: cancellationToken).ConfigureAwait(false);
        return response.Found;
    }

    internal async ValueTask<bool> GetValueFoundAsync(GetValueRequest request, CancellationToken cancellationToken)
    {
        var response = await _client.GetValueAsync(request, cancellationToken: cancellationToken).ConfigureAwait(false);
        return response.Found;
    }
}
