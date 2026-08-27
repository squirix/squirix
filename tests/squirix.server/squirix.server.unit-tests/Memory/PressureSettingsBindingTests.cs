using System.Text;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.Core;
using Squirix.Server.Node.MemoryPressure;
using Squirix.Server.TestKit.IO;
using Squirix.Server.UnitTests.Support;
using Xunit;

namespace Squirix.Server.UnitTests.Memory;

/// <summary>Tests JSON merge and configuration binding for memory pressure settings.</summary>
[Immutable]
public sealed class PressureSettingsBindingTests : ServerUnitTestBase
{
    /// <summary>
    /// Verifies System.Text.Json binds private <c language="csharp">MemoryPressure</c> section properties
    /// (via <see cref="System.Text.Json.Serialization.JsonIncludeAttribute" />) and merge overrides the baseline.
    /// </summary>
    [Fact]
    public async Task MergeAppliesJsonOverrides()
    {
        var baseline = new UnresolvedMemoryPressureOptions
        {
            MaxEstimatedCacheBytes = 1024,
            HighPressureThresholdPercent = 80,
            CriticalPressureThresholdPercent = 95,
        };

        using var settings = await TempSettingsFile.WriteAsync(
            "squirix-mp-",
            """{"MemoryPressure":{"maxEstimatedCacheBytes":4096,"highPressureThresholdPercent":70,"criticalPressureThresholdPercent":90}}""",
            DefaultCancellationToken);
        var (found, merged) = await PressureBootstrap.TryMergeFromSettingsFilePathAsync(settings.Path, baseline, DefaultCancellationToken);

        Assert.True(found);
        Assert.Equal(4096, merged.MaxEstimatedCacheBytes);
        Assert.Equal(70, merged.HighPressureThresholdPercent);
        Assert.Equal(90, merged.CriticalPressureThresholdPercent);
    }

    /// <summary>Verifies a partial JSON section overrides only present fields and keeps baseline for absent ones.</summary>
    [Fact]
    public async Task MergeKeepsBaselineForAbsentFields()
    {
        var baseline = new UnresolvedMemoryPressureOptions
        {
            MaxEstimatedCacheBytes = 1024,
            HighPressureThresholdPercent = 80,
            CriticalPressureThresholdPercent = 95,
        };

        using var settings = await TempSettingsFile.WriteAsync("squirix-mp-", """{"MemoryPressure":{"highPressureThresholdPercent":60}}""", DefaultCancellationToken);
        var (found, merged) = await PressureBootstrap.TryMergeFromSettingsFilePathAsync(settings.Path, baseline, DefaultCancellationToken);

        Assert.True(found);
        Assert.Equal(1024, merged.MaxEstimatedCacheBytes);
        Assert.Equal(60, merged.HighPressureThresholdPercent);
        Assert.Equal(95, merged.CriticalPressureThresholdPercent);
    }

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
