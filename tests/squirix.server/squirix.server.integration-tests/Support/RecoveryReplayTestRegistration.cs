using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using Squirix.Server.Core;
using Squirix.Server.LocalCache;
using Squirix.Server.Node.Services;

namespace Squirix.Server.IntegrationTests.Support;

/// <summary>Registers delayed durable replay for recovery integration tests.</summary>
internal static class RecoveryReplayTestRegistration
{
    /// <summary>Builds a <see cref="NodeStartOptions.ServicesConfigure" /> action without a capturing lambda.</summary>
    /// <param name="signal">Replay gate owned by the test.</param>
    /// <returns>Configure callback that registers <paramref name="signal" /> for delayed recovery.</returns>
    internal static Action<IServiceCollection> CreateDelayedReplayConfigure(RecoveryReplayDelaySignal signal)
    {
        ArgumentNullException.ThrowIfNull(signal);
        return new DelayedReplayConfigure(signal).Configure;
    }

    /// <summary>Delays durable recovery replay until a test signal is released.</summary>
    /// <typeparam name="T">The stored value type.</typeparam>
    private sealed class DelayedLocalCacheRecoveryDecorator<T> : ILocalCacheRecovery<T>
    {
        private readonly ILocalCacheRecovery<T> _inner;
        private readonly RecoveryReplayDelaySignal _signal;

        internal DelayedLocalCacheRecoveryDecorator(ILocalCacheRecovery<T> inner, RecoveryReplayDelaySignal signal)
        {
            _inner = inner ?? throw new ArgumentNullException(nameof(inner));
            _signal = signal ?? throw new ArgumentNullException(nameof(signal));
        }

        public async ValueTask InsertForDurableRecoveryAsync(CacheKey key, NodeCacheEntry<T> entry, CancellationToken cancellationToken)
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

    private sealed class DelayedReplayConfigure
    {
        private readonly RecoveryReplayDelaySignal _signal;

        internal DelayedReplayConfigure(RecoveryReplayDelaySignal signal)
        {
            _signal = signal;
        }

        internal void Configure(IServiceCollection services)
        {
            ArgumentNullException.ThrowIfNull(services);

            // Delayed replay must never block host StartAsync; otherwise the test cannot Release().
            ReplaceSingleton(services, new RecoveryOptions { BlockOnStart = false });
            _ = services.AddSingleton(_signal);
            ReplaceSingleton<ILocalCacheRecovery<object?>>(
                services,
                static sp => new DelayedLocalCacheRecoveryDecorator<object?>(sp.GetRequiredService<PhysicalCache<object?>>(), sp.GetRequiredService<RecoveryReplayDelaySignal>()));
        }

        private static void ReplaceSingleton<TService>(IServiceCollection services, Func<IServiceProvider, TService> factory)
            where TService : class
        {
            for (var i = services.Count - 1; i >= 0; i--)
                if (services[i].ServiceType == typeof(TService))
                    services.RemoveAt(i);

            _ = services.AddSingleton(factory);
        }

        private static void ReplaceSingleton<TService>(IServiceCollection services, TService instance)
            where TService : class
        {
            for (var i = services.Count - 1; i >= 0; i--)
                if (services[i].ServiceType == typeof(TService))
                    services.RemoveAt(i);

            _ = services.AddSingleton(instance);
        }
    }
}
