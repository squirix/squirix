using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using Squirix.Server.Adapters.Grpc;
using Squirix.Server.Contracts;
using Squirix.Server.TestKit.AspNetCore;

namespace Squirix.Server.TestKit.Benchmarking;

/// <summary>
/// Reads cache values through the server-side gRPC adapter pipeline without HTTP/2 or the public client SDK.
/// </summary>
public sealed class BenchmarkNodeReadSurface
{
    private readonly ICacheApi<object?> _cacheApi;

    private BenchmarkNodeReadSurface(ICacheApi<object?> cacheApi)
    {
        _cacheApi = cacheApi ?? throw new ArgumentNullException(nameof(cacheApi));
    }

    /// <summary>
    /// Resolves the same cache surface used by <see cref="Squirix.Server.Adapters.Grpc.SquirixServiceAdapter{T}" /> for inbound reads.
    /// </summary>
    /// <param name="host">A started in-process test node.</param>
    /// <param name="cacheName">Logical cache namespace.</param>
    /// <returns>A read surface for benchmark breakdown measurements.</returns>
    public static BenchmarkNodeReadSurface ForCache(TestNodeHost host, string cacheName)
    {
        ArgumentNullException.ThrowIfNull(host);
        ArgumentException.ThrowIfNullOrWhiteSpace(cacheName);

        var operations = host.Services.GetRequiredService<IGrpcCacheOperations<object?>>();
        return new BenchmarkNodeReadSurface(operations.ForCache(cacheName));
    }

    /// <summary>
    /// Reads an existing string value through the full server decorator stack.
    /// </summary>
    /// <param name="key">Cache key.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The stored value when present; otherwise <see langword="null" />.</returns>
    public async ValueTask<string?> GetValueOrDefaultAsync(string key, CancellationToken cancellationToken)
    {
        var result = await _cacheApi.TryGetValueAsync(key, cancellationToken).ConfigureAwait(false);
        return result.Found ? result.Value as string : null;
    }
}
