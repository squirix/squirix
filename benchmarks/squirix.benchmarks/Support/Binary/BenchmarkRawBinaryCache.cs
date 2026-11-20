using System;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Attributes;
using Squirix.Internal;
using Squirix.Internal.Cluster.Transport;
using Squirix.Server.TestKit.Networking;
using Squirix.Transport.Wire.Cache;

namespace Squirix.Benchmarks.Support.Binary;

/// <summary>Reads through the binary transport client, without the public Squirix client SDK stack.</summary>
[Immutable]
internal sealed class BenchmarkRawBinaryCache : IAsyncDisposable
{
    private static readonly ISquirixSerializer Serializer = new SystemTextJsonSerializer();

    private readonly string _cacheName;
    private readonly BinaryCacheClient _client;
    private readonly SocketsHttpHandler _handler = LoopbackHttp.CreateHandler();
    private int _disposed;

    private BenchmarkRawBinaryCache(Uri uri, string cacheName)
    {
        _client = new BinaryCacheClient(uri, null, _handler.SslOptions);
        _cacheName = cacheName;
    }

    public async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref _disposed, 1) != 0)
            return;

        try
        {
            await _client.DisposeAsync().ConfigureAwait(false);
        }
        finally
        {
            _handler.Dispose();
        }
    }

    internal static BenchmarkRawBinaryCache Connect(Uri uri, string cacheName)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(cacheName);
        return new BenchmarkRawBinaryCache(uri, cacheName);
    }

    internal async ValueTask<bool> GetValueFoundAsync(string key, CancellationToken cancellationToken)
    {
        var response = await _client.GetValueAsync(new GetValueAsyncRequest { CacheName = _cacheName, Key = key }, new BinaryCallOptions(DateTime.MaxValue, cancellationToken, cancellationToken)).ConfigureAwait(false);
        return response.Found;
    }

    internal async ValueTask<bool> GetValueFoundAsync(GetValueAsyncRequest request, CancellationToken cancellationToken)
    {
        var response = await _client.GetValueAsync(request, new BinaryCallOptions(DateTime.MaxValue, cancellationToken, cancellationToken)).ConfigureAwait(false);
        return response.Found;
    }

    internal async ValueTask<string?> GetValueOrDefaultAsync(string key, CancellationToken cancellationToken)
    {
        var response = await _client.GetValueAsync(new GetValueAsyncRequest { CacheName = _cacheName, Key = key }, new BinaryCallOptions(DateTime.MaxValue, cancellationToken, cancellationToken)).ConfigureAwait(false);

        return response.Found ? ProtoEx.Read<string>(response.Value, Serializer) : null;
    }
}
