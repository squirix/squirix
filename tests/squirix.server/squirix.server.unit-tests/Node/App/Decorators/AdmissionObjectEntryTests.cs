using System;
using System.Diagnostics.Metrics;
using System.Threading;
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
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Node.App.Decorators;

/// <summary>Admission tests for object cache entries with complex payloads.</summary>
[Immutable]
public sealed class AdmissionObjectEntryTests : DisposableServerUnitTestBase
{
    private const string CacheName = "orders";
    private const string Self = "node-a";

    private readonly Meter _testMeter = new("test");

    /// <summary>Large object entries are rejected once the projected usage exceeds the configured limit.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task OversizedObjectUsageRejectedPastLimit(CancellationToken cancellationToken)
    {
        var estimator = new ObjectCacheEntrySizeEstimator();
        var physical = new PhysicalCache<object?>(TimeProvider.System, estimator);
        var accounting = new MemoryUsageAccounting(physical);
        var options = new PressureOptions
        {
            MaxEstimatedCacheBytes = 400_000,
            HighPressureThresholdPercent = 80,
            CriticalPressureThresholdPercent = 95,
        };
        var gate = new PressureGate(new StateEvaluator(Options.Create(options)), accounting, Self, _testMeter);
        var inner = new ClientCache<object?>(physical, physical);
        var cache = new MemoryAdmissionCacheDecorator<object?>(inner, gate, estimator);
        var entry = new NodeCacheEntry<object?> { Value = new AdmissionDataPayload { Data = new string('y', 250_000) }, Version = 1 };

        _ = await Assert.That(await cache.TryAddEntryAsync(UnitMutationOpIds.Default, CacheName, "a", entry, cancellationToken)).IsTrue();
        _ = await NodeAsyncAssert.ThrowsAsync<ResourceExhaustedException, bool>(cache.TryAddEntryAsync(UnitMutationOpIds.Default, CacheName, "b", entry, cancellationToken));
        _ = await Assert.That(accounting.ReadEntryCount()).IsEqualTo(1);
    }

    /// <summary>Removing a deadline cannot grow memory and needs no admission read, even under critical pressure.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task RemoveExpirationSkipsRead(CancellationToken cancellationToken)
    {
        var estimator = new ObjectCacheEntrySizeEstimator();
        var physical = new PhysicalCache<object?>(TimeProvider.System, estimator);
        var inner = new ClientCache<object?>(physical, physical);
        await inner.SetEntryAsync(UnitMutationOpIds.Default, CacheName, "a", new NodeCacheEntry<object?>("value", 1, DateTime.UtcNow.AddHours(1)), cancellationToken);
        var options = Options.Create(new PressureOptions { MaxEstimatedCacheBytes = 1, HighPressureThresholdPercent = 80, CriticalPressureThresholdPercent = 95 });
        var gate = new PressureGate(new StateEvaluator(options), new MemoryUsageAccounting(physical), Self, _testMeter);
        var cache = new MemoryAdmissionCacheDecorator<object?>(
            inner,
            gate,
            estimator,
            readStored: static (_, _, _) => throw new InvalidOperationException("Admission must not read the entry."),
            hasMemberPressure: static (_, _) => true);

        _ = await Assert.That(await cache.RemoveExpirationAsync(UnitMutationOpIds.Default, CacheName, "a", cancellationToken)).IsTrue();
        var stored = await inner.GetEntryAsync(CacheName, "a", cancellationToken);
        _ = await Assert.That(stored!.ExpiresUtc).IsNull();
    }

    /// <inheritdoc />
    protected override void DisposeManaged() => _testMeter.Dispose();
}
