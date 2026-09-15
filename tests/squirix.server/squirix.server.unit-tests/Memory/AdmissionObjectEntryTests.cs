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

namespace Squirix.Server.UnitTests.Memory;

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
        var physical = new PhysicalCache<object?>();
        var accounting = new MemoryUsageAccounting();
        var options = new PressureOptions
        {
            MaxEstimatedCacheBytes = 400_000,
            HighPressureThresholdPercent = 80,
            CriticalPressureThresholdPercent = 95,
        };
        var gate = new PressureGate(new StateEvaluator(Options.Create(options)), accounting, Self, _testMeter);
        var estimator = new ObjectCacheEntrySizeEstimator();
        var inner = new ClientCache<object?>(physical, physical);
        var cache = new MemoryAdmissionCacheDecorator<object?>(inner, gate, estimator, accounting, RocksDoubles.CreateOwnerLocator(Self), Self);
        var entry = new NodeCacheEntry<object?> { Value = new AdmissionDataPayload { Data = new string('y', 250_000) }, Version = 1 };

        _ = await Assert.That(await cache.TryAddEntryAsync(UnitMutationOpIds.Default, CacheName, "a", entry, cancellationToken)).IsTrue();
        _ = await NodeAsyncAssert.ThrowsAsync<ResourceExhaustedException, bool>(cache.TryAddEntryAsync(UnitMutationOpIds.Default, CacheName, "b", entry, cancellationToken));
        _ = await Assert.That(accounting.ReadEntryCount()).IsEqualTo(1);
    }

    /// <inheritdoc />
    protected override void DisposeManaged() => _testMeter.Dispose();
}
