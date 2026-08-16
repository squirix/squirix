using System;
using System.Threading;
using System.Threading.Tasks;
using Grpc.Net.Client;
using Squirix.Attributes;
using Squirix.Internal;
using Squirix.Server.TestKit.Networking;
using Squirix.Transport.Grpc.Cache;

namespace Squirix.Benchmarks.Support.Grpc;

/// <summary>Reads through generated gRPC stubs only, without the public Squirix client SDK stack.</summary>
[Immutable]
internal sealed class BenchmarkRawGrpcCache : IAsyncDisposable
{
    private static readonly ISquirixSerializer Serializer = new SystemTextJsonSerializer();

    private readonly string _cacheName;
    private readonly GrpcChannel _channel;
    private readonly SquirixCacheService.SquirixCacheServiceClient _client;
    private int _disposed;

    private BenchmarkRawGrpcCache(GrpcChannel channel, SquirixCacheService.SquirixCacheServiceClient client, string cacheName)
    {
        _channel = channel;
        _client = client;
        _cacheName = cacheName;
    }

    public ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref _disposed, 1) is 1)
            return ValueTask.CompletedTask;

        _channel.Dispose();
        return ValueTask.CompletedTask;
    }

    internal static BenchmarkRawGrpcCache Connect(Uri uri, string cacheName)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(cacheName);

        var channel = GrpcChannel.ForAddress(
            uri,
            new GrpcChannelOptions
            {
                HttpHandler = LoopbackHttp.CreateHandler(),
            });

        return new BenchmarkRawGrpcCache(channel, new SquirixCacheService.SquirixCacheServiceClient(channel), cacheName);
    }

    internal async ValueTask<bool> GetValueFoundAsync(string key, CancellationToken cancellationToken)
    {
        var response = await _client.GetValueAsync(new GetValueAsyncRequest { CacheName = _cacheName, Key = key }, cancellationToken: cancellationToken).ConfigureAwait(false);
        return response.Found;
    }

    internal async ValueTask<bool> GetValueFoundAsync(GetValueAsyncRequest request, CancellationToken cancellationToken)
    {
        var response = await _client.GetValueAsync(request, cancellationToken: cancellationToken).ConfigureAwait(false);
        return response.Found;
    }

    internal async ValueTask<string?> GetValueOrDefaultAsync(string key, CancellationToken cancellationToken)
    {
        var response = await _client.GetValueAsync(new GetValueAsyncRequest { CacheName = _cacheName, Key = key }, cancellationToken: cancellationToken).ConfigureAwait(false);

        return response.Found ? await ProtoEx.FromCacheValueAsync<string>(response.Value, Serializer).ConfigureAwait(false) : null;
    }
}
