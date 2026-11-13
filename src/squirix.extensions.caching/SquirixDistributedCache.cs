using System;
using System.Diagnostics.CodeAnalysis;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Caching.Distributed;
using Microsoft.Extensions.Options;
using Squirix.Internal;

namespace Squirix.Extensions.Caching;

/// <summary>Stores Microsoft distributed cache entries in a remote Squirix namespace.</summary>
/// <remarks>The provider owns one lazy client session and keeps no local copies of cache entries.</remarks>
public sealed class SquirixDistributedCache : IDistributedCache, IDisposable, IAsyncDisposable
{
    private readonly DistributedCacheSession<RemoteDistributedCache> _session;

    /// <summary>Initializes a new instance of the <see cref="SquirixDistributedCache"/> class with validated configuration.</summary>
    /// <param name="optionsAccessor">Options for this cache instance.</param>
    /// <exception cref="ArgumentNullException"><paramref name="optionsAccessor"/> is null.</exception>
    /// <exception cref="OptionsValidationException">The cache name or endpoints are invalid.</exception>
    public SquirixDistributedCache(IOptions<SquirixCacheOptions> optionsAccessor)
    {
        ArgumentNullException.ThrowIfNull(optionsAccessor);
        _session = CreateSession(optionsAccessor.Value);
    }

    /// <inheritdoc />
    public void Dispose() => DistributedCacheSync.Run(this, static cache => cache.DisposeAsync().AsTask());

    /// <inheritdoc />
    /// <exception cref="TimeoutException">The owned client did not finish shutting down within 30 seconds.</exception>
    public ValueTask DisposeAsync() => _session.DisposeAsync();

    /// <inheritdoc />
    public byte[]? Get(string key) => DistributedCacheSync.Run((Cache: this, Key: key), static state => state.Cache.GetAsync(state.Key, CancellationToken.None));

    /// <inheritdoc />
    [SuppressMessage("Roslynator", "RCS1229", Justification = "Validation errors are explicitly returned as faulted tasks while warm calls return the native task directly.")]
    public Task<byte[]?> GetAsync(string key, CancellationToken token = default)
    {
        try
        {
            _session.ThrowIfDisposed();
            RemoteDistributedCache.ValidateKey(key);
            return !token.CanBeCanceled && _session.GetOpenedOrNull() is { } opened
                ? opened.GetAsync(key, _session.LifetimeToken)
                : GetCoreAsync(key, token);
        }
        catch (Exception error) when (error is ArgumentException or ObjectDisposedException)
        {
            return Task.FromException<byte[]?>(error);
        }
    }

    /// <inheritdoc />
    public void Refresh(string key) => DistributedCacheSync.Run((Cache: this, Key: key), static state => state.Cache.RefreshAsync(state.Key, CancellationToken.None));

    /// <inheritdoc />
    [SuppressMessage("Roslynator", "RCS1229", Justification = "Validation errors are explicitly returned as faulted tasks while warm calls return the native task directly.")]
    public Task RefreshAsync(string key, CancellationToken token = default)
    {
        try
        {
            _session.ThrowIfDisposed();
            RemoteDistributedCache.ValidateKey(key);
            return !token.CanBeCanceled && _session.GetOpenedOrNull() is { } opened
                ? opened.RefreshAsync(key, _session.LifetimeToken)
                : RefreshCoreAsync(key, token);
        }
        catch (Exception error) when (error is ArgumentException or ObjectDisposedException)
        {
            return Task.FromException(error);
        }
    }

    /// <inheritdoc />
    public void Remove(string key) => DistributedCacheSync.Run((Cache: this, Key: key), static state => state.Cache.RemoveAsync(state.Key, CancellationToken.None));

    /// <inheritdoc />
    [SuppressMessage("Roslynator", "RCS1229", Justification = "Validation errors are explicitly returned as faulted tasks while warm calls return the native task directly.")]
    public Task RemoveAsync(string key, CancellationToken token = default)
    {
        try
        {
            _session.ThrowIfDisposed();
            RemoteDistributedCache.ValidateKey(key);
            return !token.CanBeCanceled && _session.GetOpenedOrNull() is { } opened
                ? opened.RemoveAsync(key, _session.LifetimeToken)
                : RemoveCoreAsync(key, token);
        }
        catch (Exception error) when (error is ArgumentException or ObjectDisposedException)
        {
            return Task.FromException(error);
        }
    }

    /// <inheritdoc />
    public void Set(string key, byte[] value, DistributedCacheEntryOptions options) =>
        DistributedCacheSync.Run((Cache: this, Key: key, Value: value, Options: options), static state => state.Cache.SetAsync(state.Key, state.Value, state.Options, CancellationToken.None));

    /// <inheritdoc />
    /// <remarks>The earlier of the two absolute deadlines wins when both absolute options are set.</remarks>
    public Task SetAsync(string key, byte[] value, DistributedCacheEntryOptions options, CancellationToken token = default)
    {
        _session.ThrowIfDisposed();
        RemoteDistributedCache.ValidateKey(key);
        ArgumentNullException.ThrowIfNull(value);
        ArgumentNullException.ThrowIfNull(options);
        var (cap, ttl, sliding) = Normalize(options);
        if (token.CanBeCanceled || _session.GetOpenedOrNull() is not { } opened)
            return SetCoreAsync(key, value, cap, ttl, sliding, token);
        var lifetime = _session.LifetimeToken;
        return lifetime.IsCancellationRequested
            ? Task.FromCanceled(lifetime)
            : opened.SetAsync(key, CopyValue(value), cap, ttl, sliding, lifetime);
    }

    private static byte[] CopyValue(byte[] value)
    {
        var owned = GC.AllocateUninitializedArray<byte>(value.Length);
        value.AsSpan().CopyTo(owned);
        return owned;
    }

    private static DistributedCacheSession<RemoteDistributedCache> CreateSession(SquirixCacheOptions options)
    {
        try
        {
            var name = RemoteDistributedCache.ValidateCacheName(options.CacheName);
            Uri[] endpoints = [.. options.ClientOptions.Endpoints];
            if (endpoints.Length == 0)
                throw new ArgumentException("Configure at least one HTTPS Squirix endpoint.", nameof(options));

            foreach (var endpoint in endpoints)
            {
                if (endpoint is not { IsAbsoluteUri: true } || !string.Equals(endpoint.Scheme, Uri.UriSchemeHttps, StringComparison.Ordinal))
                    throw new ArgumentException("Squirix endpoints must be absolute HTTPS URIs.", nameof(options));
            }

            var tokenProvider = options.ClientOptions.BearerTokenProvider;
            return new DistributedCacheSession<RemoteDistributedCache>(
                token => RemoteDistributedCache.ConnectAsync(endpoints, tokenProvider, name, token),
                TimeProvider.System);
        }
        catch (ArgumentException ex)
        {
            throw new OptionsValidationException(Options.DefaultName, typeof(SquirixCacheOptions), [ex.Message]);
        }
    }

    private static (long? AbsoluteUnixMs, long? TtlTicks, long? SlidingTicks) Normalize(DistributedCacheEntryOptions options)
    {
        var cap = options.AbsoluteExpiration;
        var relative = options.AbsoluteExpirationRelativeToNow;
        var sliding = options.SlidingExpiration;
        return (cap is { } absolute && absolute <= TimeProvider.System.GetUtcNow()) || relative <= TimeSpan.Zero || sliding <= TimeSpan.Zero
            ? throw new ArgumentOutOfRangeException(nameof(options), "Expiration durations must be positive and an absolute expiration must be in the future.")
            : (cap?.ToUnixTimeMilliseconds(), relative?.Ticks, sliding?.Ticks);
    }

    private async Task<byte[]?> GetCoreAsync(string key, CancellationToken token)
    {
        using var linked = _session.Link(token);
        var operationToken = linked?.Token ?? _session.LifetimeToken;
        var cache = await _session.GetAsync(operationToken).ConfigureAwait(false);
        return await cache.GetAsync(key, operationToken).ConfigureAwait(false);
    }

    private async Task RefreshCoreAsync(string key, CancellationToken token)
    {
        using var linked = _session.Link(token);
        var operationToken = linked?.Token ?? _session.LifetimeToken;
        var cache = await _session.GetAsync(operationToken).ConfigureAwait(false);
        await cache.RefreshAsync(key, operationToken).ConfigureAwait(false);
    }

    private async Task RemoveCoreAsync(string key, CancellationToken token)
    {
        using var linked = _session.Link(token);
        var operationToken = linked?.Token ?? _session.LifetimeToken;
        var cache = await _session.GetAsync(operationToken).ConfigureAwait(false);
        await cache.RemoveAsync(key, operationToken).ConfigureAwait(false);
    }

    private async Task SetCoreAsync(string key, byte[] value, long? cap, long? ttl, long? sliding, CancellationToken token)
    {
        using var linked = _session.Link(token);
        var operationToken = linked?.Token ?? _session.LifetimeToken;
        operationToken.ThrowIfCancellationRequested();
        var ownedValue = CopyValue(value);
        var cache = await _session.GetAsync(operationToken).ConfigureAwait(false);
        await cache.SetAsync(key, ownedValue, cap, ttl, sliding, operationToken).ConfigureAwait(false);
    }
}
