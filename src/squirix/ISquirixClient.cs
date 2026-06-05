using System;
using System.Threading;
using System.Threading.Tasks;

namespace Squirix;

/// <summary>
/// Client facade that owns a remote Squirix server session and provides typed cache instances.
/// Callers own the client instance: dispose it when the logical session ends. Disposing the client invalidates cache facades
/// obtained from it; it does not dispose individual <see cref="ICache{T}" /> handles (those are non-owning views) and does not
/// stop server runtime services.
/// </summary>
public interface ISquirixClient : IAsyncDisposable
{
    /// <summary>
    /// Returns the primary <see cref="ICache{T}" /> facade for a logical cache name. This is the normal way to obtain a typed cache handle.
    /// </summary>
    /// <typeparam name="T">The value type stored in the cache instance.</typeparam>
    /// <param name="cacheName">Cache name.</param>
    /// <param name="cancellationToken">Cancellation token that may be used by implementations during lazy/startup operations.</param>
    /// <returns>A non-owning <see cref="ICache{T}" /> facade; repeated calls for the same name share the same logical cache.</returns>
    ValueTask<ICache<T>> GetCacheAsync<T>(string cacheName, CancellationToken cancellationToken);
}
