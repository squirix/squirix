using System.Threading.Tasks;
using Squirix.Attributes;
using Squirix.Server.Node.MemoryPressure;
using Squirix.Server.TestKit;
using Squirix.Server.UnitTests.Support;
using Xunit;

namespace Squirix.Server.UnitTests.Memory;

/// <summary>
/// Tests for <see cref="PressureBootstrap" /> environment variable overrides.
/// </summary>
[Immutable]
public sealed class PressureBootstrapTests : ServerUnitTestBase
{
    /// <summary>Verifies environment variables override defaults for memory pressure bootstrap.</summary>
    [Fact]
    public async Task EnvironmentOverridesApplyInOrder()
    {
        using (new TempEnvironmentVariable("SQUIRIX_MEMORY_PRESSURE_MAX_ESTIMATED_CACHE_BYTES", "12345"))
        using (new TempEnvironmentVariable("SQUIRIX_MEMORY_PRESSURE_HIGH_THRESHOLD_PERCENT", "70"))
        using (new TempEnvironmentVariable("SQUIRIX_MEMORY_PRESSURE_CRITICAL_THRESHOLD_PERCENT", "90"))
        {
            var loaded = await PressureBootstrap.LoadAsync(DefaultCancellationToken);
            Assert.Equal(12345L, loaded.MaxEstimatedCacheBytes);
            Assert.Equal(70, loaded.HighPressureThresholdPercent);
            Assert.Equal(90, loaded.CriticalPressureThresholdPercent);
        }
    }
}
