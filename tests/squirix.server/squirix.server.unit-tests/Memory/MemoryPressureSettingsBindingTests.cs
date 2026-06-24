using System.IO;
using System.Text;
using System.Threading.Tasks;
using Squirix.Server.Node.Bootstrap;
using Squirix.Server.Node.MemoryPressure;
using Squirix.Server.Serialization;
using Squirix.Server.TestKit.IO;
using Squirix.Server.UnitTests.Support;
using Xunit;

namespace Squirix.Server.UnitTests.Memory;

/// <summary>Tests JSON merge and configuration binding for memory pressure settings.</summary>
public sealed class MemoryPressureSettingsBindingTests : UnitTestBase
{
    /// <summary>
    /// Verifies System.Text.Json can bind camelCase <c>MemoryPressure</c> threshold fields into <see cref="MemoryPressureSettings" /> for merge.
    /// </summary>
    [Fact]
    public void JsonDeserializeMemoryPressureSettingsBindsThresholdFields()
    {
        const string json = """{"highPressureThresholdPercent":72,"criticalPressureThresholdPercent":91}""";
        var serializer = new SystemTextJsonSerializer();
        var section = serializer.Deserialize<MemoryPressureSettings>(json);
        Assert.NotNull(section);
        var merged = section.MergeInto(new UnresolvedMemoryPressureOptions());
        var resolved = MemoryPressureOptionsResolver.Resolve(merged, new FixedMemoryBudgetProvider(10_000));
        Assert.Equal(72, resolved.HighPressureThresholdPercent);
        Assert.Equal(91, resolved.CriticalPressureThresholdPercent);
    }

    /// <summary>Verifies System.Text.Json round-trip preserves option values (same shape as JSON configuration files).</summary>
    [Fact]
    public void JsonSerializerRoundTripBindsOptionNames()
    {
        var original = new MemoryPressureOptions
        {
            MaxEstimatedCacheBytes = 4096,
            HighPressureThresholdPercent = 70,
            CriticalPressureThresholdPercent = 90,
        };

        var serializer = new SystemTextJsonSerializer();
        var json = Encoding.UTF8.GetString(serializer.SerializeToUtf8Bytes(original));
        var restored = serializer.Deserialize<MemoryPressureOptions>(json);
        Assert.NotNull(restored);
        restored.Validate();
        Assert.Equal(original, restored);
    }

    /// <summary>
    /// Verifies <see cref="MemoryPressureSettings.MergeInto" /> applies optional threshold fields from the partial settings shape.
    /// </summary>
    [Fact]
    public void SettingsMergeAppliesThresholdOverrides()
    {
        var baseline = new UnresolvedMemoryPressureOptions
        {
            MaxEstimatedCacheBytes = 8192,
            HighPressureThresholdPercent = 80,
            CriticalPressureThresholdPercent = 95,
        };

        var section = new MemoryPressureSettings
        {
            HighPressureThresholdPercent = 65,
            CriticalPressureThresholdPercent = 90,
        };

        var merged = section.MergeInto(baseline);
        var resolved = MemoryPressureOptionsResolver.Resolve(merged, new FixedMemoryBudgetProvider(20_000));
        Assert.Equal(8192L, resolved.MaxEstimatedCacheBytes);
        Assert.Equal(65, resolved.HighPressureThresholdPercent);
        Assert.Equal(90, resolved.CriticalPressureThresholdPercent);
    }

    /// <summary>
    /// Verifies partial JSON merges onto defaults via <see cref="MemoryPressureSettings" />.
    /// </summary>
    [Fact]
    public void SettingsMergesPartialJsonOntoDefaults()
    {
        var section = new MemoryPressureSettings { MaxEstimatedCacheBytes = 2048L };
        var merged = section.MergeInto(new UnresolvedMemoryPressureOptions());
        var resolved = MemoryPressureOptionsResolver.Resolve(merged, new FixedMemoryBudgetProvider(10_000));
        Assert.Equal(2048L, resolved.MaxEstimatedCacheBytes);
        Assert.Equal(80, resolved.HighPressureThresholdPercent);
    }

    /// <summary>
    /// Verifies <see cref="UnifiedSettings.TryMergeMemoryPressureFromFileAsync" /> reads the <c>MemoryPressure</c> section.
    /// </summary>
    [Fact]
    public async Task UnifiedSettingsMergesMemoryPressureSectionFromFile()
    {
        using var dir = new TempDirectory("squirix-mp-settings-merge");
        const string json = """{"Squirix":{"MemoryPressure":{"maxEstimatedCacheBytes":1000}}}""";
        var path = PathKit.Combine(dir, "Squirix.settings.json");
        await File.WriteAllTextAsync(path, json, DefaultCancellationToken);
        var (found, merged) = await UnifiedSettings.TryMergeMemoryPressureFromSettingsFilePathAsync(path, new UnresolvedMemoryPressureOptions(), DefaultCancellationToken);
        Assert.True(found);
        var resolved = MemoryPressureOptionsResolver.Resolve(merged, new FixedMemoryBudgetProvider(10_000));
        Assert.Equal(1000L, resolved.MaxEstimatedCacheBytes);
    }

    /// <summary>
    /// Verifies unified settings merge includes optional threshold properties under <c>MemoryPressure</c>.
    /// </summary>
    [Fact]
    public async Task UnifiedSettingsMergesThresholdsFromFile()
    {
        using var dir = new TempDirectory("squirix-mp-settings-thresholds");
        const string json = """{"Squirix":{"MemoryPressure":{"maxEstimatedCacheBytes":5000,"highPressureThresholdPercent":70,"criticalPressureThresholdPercent":88}}}""";
        var path = PathKit.Combine(dir, "Squirix.settings.json");
        await File.WriteAllTextAsync(path, json, DefaultCancellationToken);
        var (found, merged) = await UnifiedSettings.TryMergeMemoryPressureFromSettingsFilePathAsync(path, new UnresolvedMemoryPressureOptions(), DefaultCancellationToken);
        Assert.True(found);
        var resolved = MemoryPressureOptionsResolver.Resolve(merged, new FixedMemoryBudgetProvider(20_000));
        Assert.Equal(5000L, resolved.MaxEstimatedCacheBytes);
        Assert.Equal(70, resolved.HighPressureThresholdPercent);
        Assert.Equal(88, resolved.CriticalPressureThresholdPercent);
    }

    private sealed class FixedMemoryBudgetProvider(long availableBytes) : IMemoryBudgetProvider
    {
        public long GetTotalAvailableBytes() => availableBytes;
    }
}
