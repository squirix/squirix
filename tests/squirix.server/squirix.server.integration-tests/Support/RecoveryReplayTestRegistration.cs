using Microsoft.Extensions.DependencyInjection;
using Squirix.Server.LocalCache;

namespace Squirix.Server.IntegrationTests.Support;

/// <summary>
/// Registers delayed durable replay for recovery integration tests.
/// </summary>
internal static class RecoveryReplayTestRegistration
{
    internal static void AddDelayedReplay(IServiceCollection services, RecoveryReplayDelaySignal signal)
    {
        _ = services.AddSingleton(signal);
        _ = services.AddSingleton<ILocalCacheRecovery<object?>>(static sp => new DelayedLocalCacheRecoveryDecorator<object?>(
            sp.GetRequiredService<PhysicalCache<object?>>(),
            sp.GetRequiredService<RecoveryReplayDelaySignal>()));
    }
}
