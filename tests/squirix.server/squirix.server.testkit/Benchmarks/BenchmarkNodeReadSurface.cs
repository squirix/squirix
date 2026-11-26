using System;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.Core;
using Squirix.Server.Runtime.Contracts;
using Squirix.Server.TestKit.Hosting;

namespace Squirix.Server.TestKit.Benchmarks;

/// <summary>Reads cache values through the inbound server cache pipeline in process.</summary>
[Immutable]
public sealed class BenchmarkNodeReadSurface
{
    private readonly ICacheApi<StoredValue> _cacheApi;

    private BenchmarkNodeReadSurface(ICacheApi<StoredValue> cacheApi)
    {
        ArgumentNullException.ThrowIfNull(cacheApi);
        _cacheApi = cacheApi;
    }

    /// <summary>Resolves the cache operations used for inbound reads.</summary>
    /// <param name="host">A started in-process test node.</param>
    /// <param name="cacheName">Logical cache namespace.</param>
    /// <returns>A read surface for benchmark breakdown measurements.</returns>
    public static BenchmarkNodeReadSurface ForCache(ITestNodeHost host, string cacheName)
    {
        ArgumentNullException.ThrowIfNull(host);
        ArgumentException.ThrowIfNullOrWhiteSpace(cacheName);

        var operations = host.GetRequiredService<IInboundEndpointCacheOperations<StoredValue>>();
        return new BenchmarkNodeReadSurface(operations.ForCache(cacheName));
    }

    /// <summary>Reads an existing string value through the full server decorator stack.</summary>
    /// <param name="key">Cache key.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The stored value when present; otherwise <see langword="null" />.</returns>
    public async ValueTask<string?> GetValueOrDefaultAsync(string key, CancellationToken cancellationToken)
    {
        var result = await _cacheApi.GetValueAsync(key, cancellationToken).ConfigureAwait(false);
        return result.Found && result.Value.Kind == StoredValueKind.Text ? Encoding.UTF8.GetString(result.Value.Bytes) : null;
    }
}
