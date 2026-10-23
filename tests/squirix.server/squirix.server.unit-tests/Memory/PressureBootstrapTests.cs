using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.Node.MemoryPressure;
using Squirix.Server.TestKit;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Memory;

/// <summary>Tests for <see cref="PressureBootstrap" /> environment variable overrides.</summary>
[Immutable]
public sealed class PressureBootstrapTests : ServerUnitTestBase
{
    /// <summary>Verifies environment variables override defaults for memory pressure bootstrap.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task EnvironmentOverridesApplyInOrder(CancellationToken cancellationToken)
    {
        using (new TempEnvironmentVariable("SQUIRIX_MEMORY_PRESSURE_MAX_ESTIMATED_CACHE_BYTES", "12345"))
        using (new TempEnvironmentVariable("SQUIRIX_MEMORY_PRESSURE_HIGH_THRESHOLD_PERCENT", "70"))
        using (new TempEnvironmentVariable("SQUIRIX_MEMORY_PRESSURE_CRITICAL_THRESHOLD_PERCENT", "90"))
        {
            var loaded = await PressureBootstrap.LoadAsync(cancellationToken);
            _ = await Assert.That(loaded.MaxEstimatedCacheBytes).IsEqualTo(12345L);
            _ = await Assert.That(loaded.HighPressureThresholdPercent).IsEqualTo(70);
            _ = await Assert.That(loaded.CriticalPressureThresholdPercent).IsEqualTo(90);
        }
    }
}
