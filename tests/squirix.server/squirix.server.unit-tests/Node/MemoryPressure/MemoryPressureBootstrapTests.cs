using Squirix.Server.Node.MemoryPressure;
using Squirix.Server.TestKit;
using Xunit;

namespace Squirix.Server.UnitTests.Node.MemoryPressure;

/// <summary>
/// Tests for <see cref="MemoryPressureBootstrap" /> environment variable overrides.
/// </summary>
public sealed class MemoryPressureBootstrapTests
{
    /// <summary>
    /// Verifies environment variables override defaults for memory pressure bootstrap.
    /// </summary>
    [Fact]
    public void EnvironmentOverridesApplyInOrder()
    {
        using (new TempEnvironmentVariable("SQUIRIX_MEMORY_PRESSURE_ENABLED", "true"))
        using (new TempEnvironmentVariable("SQUIRIX_MEMORY_PRESSURE_MAX_ESTIMATED_CACHE_BYTES", "12345"))
        {
            var loaded = MemoryPressureBootstrap.Load();
            Assert.True(loaded.Enabled);
            Assert.Equal(12345L, loaded.MaxEstimatedCacheBytes);
        }

        using (new TempEnvironmentVariable("SQUIRIX_MEMORY_PRESSURE_ENABLED", "false"))
        {
            var disabled = MemoryPressureBootstrap.Load();
            Assert.False(disabled.Enabled);
        }
    }
}
