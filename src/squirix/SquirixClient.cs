using System;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Internal;

namespace Squirix;

/// <summary>
/// Entry point to connect to Squirix servers and get typed cache instances.
/// </summary>
public sealed class SquirixClient : ISquirixClient
{
    private readonly IRemoteClientSession _remoteSession;
    private bool _disposed;
    private int _disposeOnce;

    private SquirixClient(IRemoteClientSession remoteSession)
    {
        _remoteSession = remoteSession ?? throw new ArgumentNullException(nameof(remoteSession));
    }

    /// <summary>
    /// Connects to a Squirix server endpoint.
    /// </summary>
    /// <param name="endpoint">The Squirix server endpoint URL.</param>
    /// <param name="cancellationToken">Cancellation token for client warm-up.</param>
    /// <returns>A remote <see cref="ISquirixClient" /> session.</returns>
    public static ValueTask<ISquirixClient> ConnectAsync(string endpoint, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(endpoint);
        return ConnectAsync(options => options.Endpoints.Add(endpoint), cancellationToken);
    }

    /// <summary>
    /// Connects to Squirix server bootstrap endpoints using client-only options.
    /// </summary>
    /// <remarks>
    /// At least one configured endpoint must be reachable; additional endpoints provide transport failover.
    /// See <see cref="SquirixOptions.Endpoints" /> for HA semantics.
    /// </remarks>
    /// <param name="configure">Configures remote client endpoints and transport settings.</param>
    /// <param name="cancellationToken">Cancellation token for client warm-up.</param>
    /// <returns>A remote <see cref="ISquirixClient" /> session.</returns>
    public static async ValueTask<ISquirixClient> ConnectAsync(Action<SquirixOptions> configure, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(configure);

        var options = new SquirixOptions();
        configure(options);

        return await ConnectAsync(options, options.HttpMessageHandler, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Ends the logical client session. Idempotent. Cache facades obtained via <see cref="GetCacheAsync{T}" /> throw
    /// <see cref="ObjectDisposedException" /> on subsequent operations. Remote transport resources owned by this session are released.
    /// </summary>
    /// <returns>A <see cref="ValueTask" /> that completes when disposal finishes.</returns>
    public async ValueTask DisposeAsync()
    {
        if (Interlocked.CompareExchange(ref _disposeOnce, 1, 0) != 0)
            return;

        _disposed = true;
        await _remoteSession.DisposeAsync().ConfigureAwait(false);
    }

    /// <summary>
    /// Returns the primary <see cref="ICache{T}" /> facade for a logical cache name.
    /// </summary>
    /// <typeparam name="T">The value type stored in the cache.</typeparam>
    /// <param name="cacheName">The logical cache name to access.</param>
    /// <param name="cancellationToken">
    /// A cancellation token used during cache resolution.
    /// </param>
    /// <returns>A non-owning <see cref="ICache{T}" /> facade for the specified cache name.</returns>
    public ValueTask<ICache<T>> GetCacheAsync<T>(string cacheName, CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        ThrowIfDisposed();

        var cache = _remoteSession.GetCache<T>(cacheName);
        return ValueTask.FromResult<ICache<T>>(new ClientScopedCache<T>(this, cache));
    }

    internal static async ValueTask<ISquirixClient> ConnectAsync(SquirixOptions options, HttpMessageHandler? handler, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(options);

        var session = await RemoteClientSessionFactory.ConnectAsync(options, handler, cancellationToken).ConfigureAwait(false);
        return new SquirixClient(session);
    }

    internal void ThrowIfDisposed() => ObjectDisposedException.ThrowIf(_disposed, this);
}
