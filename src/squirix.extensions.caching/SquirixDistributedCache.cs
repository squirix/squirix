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
    public byte[]? Get(string key) => Block(InvokeSyncAsync(() => GetAsync(key, CancellationToken.None)));

    /// <inheritdoc />
    public async Task<byte[]?> GetAsync(string key, CancellationToken token = default)
    {
        _session.ThrowIfDisposed();
        RemoteDistributedCache.ValidateKey(key);
        using var linked = _session.Link(token);
        var operationToken = linked?.Token ?? _session.LifetimeToken;
        var cache = await _session.GetAsync(operationToken).ConfigureAwait(false);
        return await cache.GetAsync(key, operationToken).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public void Set(string key, byte[] value, DistributedCacheEntryOptions options) =>
        Block(InvokeSyncAsync(() => SetAsync(key, value, options, CancellationToken.None)));

    /// <inheritdoc />
    /// <remarks>The earlier of the two absolute deadlines wins when both absolute options are set.</remarks>
    public Task SetAsync(string key, byte[] value, DistributedCacheEntryOptions options, CancellationToken token = default)
    {
        _session.ThrowIfDisposed();
        RemoteDistributedCache.ValidateKey(key);
        ArgumentNullException.ThrowIfNull(value);
        ArgumentNullException.ThrowIfNull(options);
        var (cap, sliding) = Normalize(options, TimeProvider.System.GetUtcNow());
        if (!token.CanBeCanceled && _session.GetOpenedOrNull() is { } opened)
        {
            var lifetime = _session.LifetimeToken;
            return lifetime.IsCancellationRequested
                ? Task.FromCanceled(lifetime)
                : opened.SetAsync(key, CopyValue(value), cap, sliding, lifetime);
        }

        return SetCoreAsync(key, value, cap, sliding, token);
    }

    /// <inheritdoc />
    public void Refresh(string key) => Block(InvokeSyncAsync(() => RefreshAsync(key, CancellationToken.None)));

    /// <inheritdoc />
    public async Task RefreshAsync(string key, CancellationToken token = default)
    {
        _session.ThrowIfDisposed();
        RemoteDistributedCache.ValidateKey(key);
        using var linked = _session.Link(token);
        var operationToken = linked?.Token ?? _session.LifetimeToken;
        var cache = await _session.GetAsync(operationToken).ConfigureAwait(false);
        await cache.RefreshAsync(key, operationToken).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public void Remove(string key) => Block(InvokeSyncAsync(() => RemoveAsync(key, CancellationToken.None)));

    /// <inheritdoc />
    public async Task RemoveAsync(string key, CancellationToken token = default)
    {
        _session.ThrowIfDisposed();
        RemoteDistributedCache.ValidateKey(key);
        using var linked = _session.Link(token);
        var operationToken = linked?.Token ?? _session.LifetimeToken;
        var cache = await _session.GetAsync(operationToken).ConfigureAwait(false);
        await cache.RemoveAsync(key, operationToken).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public void Dispose() => Block(DisposeAsync().AsTask());

    /// <inheritdoc />
    /// <exception cref="TimeoutException">The owned client did not finish shutting down within 30 seconds.</exception>
    public ValueTask DisposeAsync() => _session.DisposeAsync();

    [SuppressMessage("Usage", "VSTHRD002", Justification = "The synchronous interface blocks on work started without the caller's synchronization context.")]
    [SuppressMessage("Usage", "MA0045", Justification = "IDistributedCache exposes a synchronous API.")]
    private static T Block<T>(Task<T> task) => task.GetAwaiter().GetResult();

    [SuppressMessage("Usage", "VSTHRD002", Justification = "The synchronous interface blocks on context-independent work.")]
    [SuppressMessage("Usage", "MA0045", Justification = "IDistributedCache and IDisposable expose synchronous APIs.")]
    private static void Block(Task task) => task.GetAwaiter().GetResult();

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

    private static (long? AbsoluteUnixMs, long? SlidingTicks) Normalize(DistributedCacheEntryOptions options, DateTimeOffset now)
    {
        var cap = options.AbsoluteExpiration;
        var relative = options.AbsoluteExpirationRelativeToNow;
        var sliding = options.SlidingExpiration;
        if (cap <= now || relative <= TimeSpan.Zero || sliding <= TimeSpan.Zero || relative > DateTimeOffset.MaxValue - now || sliding > DateTimeOffset.MaxValue - now)
            throw new ArgumentOutOfRangeException(nameof(options), "Expiration must be positive, in the future and representable.");

        if (relative is { } interval)
        {
            var relativeCap = now + interval;
            if (cap == null || relativeCap < cap)
                cap = relativeCap;
        }

        return (cap?.ToUnixTimeMilliseconds(), sliding?.Ticks);
    }

    private static async Task<T> InvokeSyncAsync<T>(Func<Task<T>> operation)
    {
        if (SynchronizationContext.Current != null || TaskScheduler.Current != TaskScheduler.Default)
            await Task.CompletedTask.ConfigureAwait(ConfigureAwaitOptions.ForceYielding);

        return await operation().ConfigureAwait(false);
    }

    private static async Task InvokeSyncAsync(Func<Task> operation)
    {
        if (SynchronizationContext.Current != null || TaskScheduler.Current != TaskScheduler.Default)
            await Task.CompletedTask.ConfigureAwait(ConfigureAwaitOptions.ForceYielding);

        await operation().ConfigureAwait(false);
    }

    private static byte[] CopyValue(byte[] value)
    {
        var owned = GC.AllocateUninitializedArray<byte>(value.Length);
        value.AsSpan().CopyTo(owned);
        return owned;
    }

    private async Task SetCoreAsync(string key, byte[] value, long? cap, long? sliding, CancellationToken token)
    {
        using var linked = _session.Link(token);
        var operationToken = linked?.Token ?? _session.LifetimeToken;
        operationToken.ThrowIfCancellationRequested();
        var ownedValue = CopyValue(value);
        var cache = await _session.GetAsync(operationToken).ConfigureAwait(false);
        await cache.SetAsync(key, ownedValue, cap, sliding, operationToken).ConfigureAwait(false);
    }
}
