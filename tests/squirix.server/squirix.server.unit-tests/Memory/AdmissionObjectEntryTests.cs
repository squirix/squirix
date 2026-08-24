using System.Threading.Tasks;
using Microsoft.Extensions.Options;
using Squirix.Server.Attributes;
using Squirix.Server.Core;
using Squirix.Server.Errors;
using Squirix.Server.LocalCache;
using Squirix.Server.Node.App.Decorators;
using Squirix.Server.Node.MemoryPressure;
using Squirix.Server.TestKit;
using Squirix.Server.UnitTests.Support;
using Xunit;

namespace Squirix.Server.UnitTests.Memory;

/// <summary>Admission tests for object cache entries with complex payloads.</summary>
[Immutable]
public sealed class AdmissionObjectEntryTests : ServerUnitTestBase
{
    private const string CacheName = "orders";
    private const string Self = "node-a";

    /// <summary>Large object entries are rejected once projected usage exceeds the configured limit.</summary>
    [Fact]
    public async Task OversizedObjectUsageRejectedPastLimit()
    {
        await using var physical = new PhysicalCache<object?>();
        var accounting = new MemoryUsageAccounting();
        var gate = new PressureGate(
            new StateEvaluator(
                Options.Create(
                    new PressureOptions
                    {
                        MaxEstimatedCacheBytes = 400_000,
                        HighPressureThresholdPercent = 80,
                        CriticalPressureThresholdPercent = 95,
                    })),
            accounting,
            Self);
        var estimator = new ObjectCacheEntrySizeEstimator();
        var inner = new ClientCache<object?>(physical, physical);
        var cache = new MemoryAdmissionCacheDecorator<object?>(inner, gate, estimator, accounting, new FixedOwnerLocator(Self), Self);
        var entry = new NodeCacheEntry<object?> { Value = new { Data = new string('y', 250_000) }, Version = 1 };

        Assert.True(await cache.TryAddEntryAsync(UnitMutationOpIds.Default, CacheName, "a", entry, DefaultCancellationToken));
        _ = await NodeAsyncAssert.ThrowsAsync<ResourceExhaustedException, bool>(cache.TryAddEntryAsync(UnitMutationOpIds.Default, CacheName, "b", entry, DefaultCancellationToken));
        Assert.Equal(1, accounting.ReadEntryCount());
    }
}
