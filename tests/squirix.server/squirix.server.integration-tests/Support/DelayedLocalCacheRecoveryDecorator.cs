using System;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Core;
using Squirix.Server.LocalCache;

namespace Squirix.Server.IntegrationTests.Support;

/// <summary>Delays durable recovery replay until a test signal is released.</summary>
/// <typeparam name="T">The stored value type.</typeparam>
internal sealed class DelayedLocalCacheRecoveryDecorator<T> : ILocalCacheRecovery<T>
{
    private readonly ILocalCacheRecovery<T> _inner;
    private readonly RecoveryReplayDelaySignal _signal;

    public DelayedLocalCacheRecoveryDecorator(ILocalCacheRecovery<T> inner, RecoveryReplayDelaySignal signal)
    {
        _inner = inner ?? throw new ArgumentNullException(nameof(inner));
        _signal = signal ?? throw new ArgumentNullException(nameof(signal));
    }

    public async ValueTask InsertForDurableRecoveryAsync(CacheKey key, CacheEntry<T> entry, CancellationToken cancellationToken)
    {
        await _signal.WaitAsync(cancellationToken).ConfigureAwait(false);
        await _inner.InsertForDurableRecoveryAsync(key, entry, cancellationToken).ConfigureAwait(false);
    }

    public async ValueTask<bool> RemoveExpirationForDurableRecoveryAsync(CacheKey key, CancellationToken cancellationToken)
    {
        await _signal.WaitAsync(cancellationToken).ConfigureAwait(false);
        return await _inner.RemoveExpirationForDurableRecoveryAsync(key, cancellationToken).ConfigureAwait(false);
    }

    public async ValueTask<bool> RemoveForDurableRecoveryAsync(CacheKey key, CancellationToken cancellationToken)
    {
        await _signal.WaitAsync(cancellationToken).ConfigureAwait(false);
        return await _inner.RemoveForDurableRecoveryAsync(key, cancellationToken).ConfigureAwait(false);
    }

    public async ValueTask<bool> TouchExpirationForDurableRecoveryAsync(CacheKey key, DateTime expiresUtc, CancellationToken cancellationToken)
    {
        await _signal.WaitAsync(cancellationToken).ConfigureAwait(false);
        return await _inner.TouchExpirationForDurableRecoveryAsync(key, expiresUtc, cancellationToken).ConfigureAwait(false);
    }
}
