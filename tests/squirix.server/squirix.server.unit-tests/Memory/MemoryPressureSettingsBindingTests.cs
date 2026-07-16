using System.Text;
using Squirix.Server.Node.MemoryPressure;
using Squirix.Server.Serialization;
using Squirix.Server.UnitTests.Support;
using Xunit;

namespace Squirix.Server.UnitTests.Memory;

/// <summary>Tests JSON merge and configuration binding for memory pressure settings.</summary>
public sealed class MemoryPressureSettingsBindingTests : UnitTestBase
{
    /// <summary>Verifies System.Text.Json round-trip preserves option values (same shape as JSON configuration files).</summary>
    [Fact]
    public void JsonSerializerRoundTripBindsOptionNames()
    {
        var original = new PressureOptions
        {
            MaxEstimatedCacheBytes = 4096,
            HighPressureThresholdPercent = 70,
            CriticalPressureThresholdPercent = 90,
        };

        var serializer = new ServerJsonSerializer();
        var json = Encoding.UTF8.GetString(serializer.SerializeToUtf8Bytes(original));
        var restored = serializer.Deserialize<PressureOptions>(json);
        Assert.NotNull(restored);
        restored.Validate();
        Assert.Equal(original, restored);
    }
}
