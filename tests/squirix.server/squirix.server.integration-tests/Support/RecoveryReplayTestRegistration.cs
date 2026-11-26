using System;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.Core;
using Squirix.Server.LocalCache;
using Squirix.Server.Node.Services;
using Squirix.Server.TestKit;

namespace Squirix.Server.IntegrationTests.Support;

/// <summary>Registers delayed durable replay for recovery integration tests.</summary>
internal static class RecoveryReplayTestRegistration
{
    /// <summary>Creates a callback that configures services for delayed recovery.</summary>
    /// <param name="signal">Replay gate owned by the test.</param>
    /// <returns>Configure callback that registers <paramref name="signal" /> for delayed recovery.</returns>
    internal static Action<TestServiceCollection> CreateDelayedReplayConfigure(RecoveryReplayDelaySignal signal)
    {
        ArgumentNullException.ThrowIfNull(signal);
        return new DelayedReplayConfigure(signal).Configure;
    }

    /// <summary>Delays durable recovery replay until a test signal is released.</summary>
    /// <typeparam name="T">The stored value type.</typeparam>
    [Immutable]
    private sealed class DelayedLocalCacheRecoveryDecorator<T> : ILocalCacheRecovery<T>
    {
        private readonly ILocalCacheRecovery<T> _inner;
        private readonly RecoveryReplayDelaySignal _signal;

        internal DelayedLocalCacheRecoveryDecorator(ILocalCacheRecovery<T> inner, RecoveryReplayDelaySignal signal)
        {
            ArgumentNullException.ThrowIfNull(inner);
            ArgumentNullException.ThrowIfNull(signal);
            _inner = inner;
            _signal = signal;
        }

        public async ValueTask InsertRecoveryAsync(CacheKey key, NodeCacheEntry<T> entry, CancellationToken cancellationToken, int? encodedLength = null)
        {
            await _signal.WaitAsync(cancellationToken).ConfigureAwait(false);
            await _inner.InsertRecoveryAsync(key, entry, cancellationToken, encodedLength).ConfigureAwait(false);
        }

        public async ValueTask<bool> RemoveRecoveryAsync(CacheKey key, CancellationToken cancellationToken)
        {
            await _signal.WaitAsync(cancellationToken).ConfigureAwait(false);
            return await _inner.RemoveRecoveryAsync(key, cancellationToken).ConfigureAwait(false);
        }
    }

    [Immutable]
    private sealed class DelayedReplayConfigure
    {
        private readonly RecoveryReplayDelaySignal _signal;

        internal DelayedReplayConfigure(RecoveryReplayDelaySignal signal)
        {
            _signal = signal;
        }

        internal void Configure(TestServiceCollection services)
        {
            ArgumentNullException.ThrowIfNull(services);

            // Delayed replay must never block host StartAsync; otherwise the test cannot Release().
            ReplaceSingleton(services, new RecoveryOptions { BlockOnStart = false });
            _ = services.AddSingleton(_signal);
            ReplaceSingleton<ILocalCacheRecovery<StoredValue>>(
                services,
                static sp => new DelayedLocalCacheRecoveryDecorator<StoredValue>(sp.GetRequiredService<PhysicalCache<StoredValue>>(), sp.GetRequiredService<RecoveryReplayDelaySignal>()));
        }

        private static void ReplaceSingleton<TService>(TestServiceCollection services, Func<IServiceProvider, TService> factory)
            where TService : class
        {
            for (var i = services.Count - 1; i >= 0; i--)
            {
                if (services[i].ServiceType == typeof(TService))
                    services.RemoveAt(i);
            }

            _ = services.AddSingleton(factory);
        }

        private static void ReplaceSingleton<TService>(TestServiceCollection services, TService instance)
            where TService : class
        {
            for (var i = services.Count - 1; i >= 0; i--)
            {
                if (services[i].ServiceType == typeof(TService))
                    services.RemoveAt(i);
            }

            _ = services.AddSingleton(instance);
        }
    }
}
