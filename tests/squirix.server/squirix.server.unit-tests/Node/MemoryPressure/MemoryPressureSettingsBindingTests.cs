using System.IO;
using System.Text.Json;
using Squirix.Server.Node.Bootstrap;
using Squirix.Server.Node.MemoryPressure;
using Squirix.Server.TestKit.IO;
using Xunit;

namespace Squirix.Server.UnitTests.Node.MemoryPressure;

/// <summary>
/// Tests JSON merge and configuration binding for memory pressure settings.
/// </summary>
public sealed class MemoryPressureSettingsBindingTests
{
    private static readonly JsonSerializerOptions CamelWriteOptions = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
        WriteIndented = true,
    };

    private static readonly JsonSerializerOptions CaseInsensitiveReadOptions = new()
    {
        PropertyNameCaseInsensitive = true,
    };

    /// <summary>
    /// Verifies System.Text.Json can bind camelCase <c>MemoryPressure</c> threshold fields into <see cref="MemoryPressureSettings" /> for merge.
    /// </summary>
    [Fact]
    public void JsonDeserializeMemoryPressureSettingsBindsThresholdFields()
    {
        const string json = """
                            {
                              "highPressureThresholdPercent": 72,
                              "criticalPressureThresholdPercent": 91,
                              "rejectWritesOnCriticalPressure": false
                            }
                            """;

        var section = JsonSerializer.Deserialize<MemoryPressureSettings>(json, CaseInsensitiveReadOptions);
        Assert.NotNull(section);
        var merged = section.MergeInto(new MemoryPressureOptions());
        merged.Validate();
        Assert.Equal(72, merged.HighPressureThresholdPercent);
        Assert.Equal(91, merged.CriticalPressureThresholdPercent);
        Assert.False(merged.RejectWritesOnCriticalPressure);
    }

    /// <summary>
    /// Verifies System.Text.Json round-trip preserves option values (same shape as JSON configuration files).
    /// </summary>
    [Fact]
    public void JsonSerializerRoundTripBindsOptionNames()
    {
        var original = new MemoryPressureOptions
        {
            Enabled = true,
            MaxEstimatedCacheBytes = 4096,
            HighPressureThresholdPercent = 70,
            CriticalPressureThresholdPercent = 90,
            RejectWritesOnCriticalPressure = false,
        };

        var json = JsonSerializer.Serialize(original, CamelWriteOptions);
        var restored = JsonSerializer.Deserialize<MemoryPressureOptions>(json, CaseInsensitiveReadOptions);
        Assert.NotNull(restored);
        restored.Validate();
        Assert.Equal(original, restored);
    }

    /// <summary>
    /// Verifies <see cref="MemoryPressureSettings.MergeInto" /> applies optional threshold and rejection fields from the partial settings shape.
    /// </summary>
    [Fact]
    public void SettingsMergeAppliesThresholdAndWriteRejectionOverrides()
    {
        var baseline = new MemoryPressureOptions
        {
            Enabled = true,
            MaxEstimatedCacheBytes = 8192,
            HighPressureThresholdPercent = 80,
            CriticalPressureThresholdPercent = 95,
            RejectWritesOnCriticalPressure = true,
        };

        var section = new MemoryPressureSettings
        {
            HighPressureThresholdPercent = 65,
            CriticalPressureThresholdPercent = 90,
            RejectWritesOnCriticalPressure = false,
        };

        var merged = section.MergeInto(baseline);
        merged.Validate();
        Assert.True(merged.Enabled);
        Assert.Equal(8192L, merged.MaxEstimatedCacheBytes);
        Assert.Equal(65, merged.HighPressureThresholdPercent);
        Assert.Equal(90, merged.CriticalPressureThresholdPercent);
        Assert.False(merged.RejectWritesOnCriticalPressure);
    }

    /// <summary>
    /// Verifies partial JSON merges onto defaults via <see cref="MemoryPressureSettings" />.
    /// </summary>
    [Fact]
    public void SettingsMergesPartialJsonOntoDefaults()
    {
        var section = new MemoryPressureSettings { Enabled = true, MaxEstimatedCacheBytes = 2048L };
        var merged = section.MergeInto(new MemoryPressureOptions());
        Assert.True(merged.Enabled);
        Assert.Equal(2048L, merged.MaxEstimatedCacheBytes);
        Assert.Equal(80, merged.HighPressureThresholdPercent);
    }

    /// <summary>
    /// Verifies <see cref="UnifiedSettings.TryMergeMemoryPressureFromFile" /> reads the <c>MemoryPressure</c> section.
    /// </summary>
    [Fact]
    public void UnifiedSettingsMergesMemoryPressureSectionFromFile()
    {
        var dir = DirectoryKit.CreateTempDirectory("squirix-mp-settings-merge");
        try
        {
            const string json = """
                                {
                                  "Squirix": {
                                    "MemoryPressure": {
                                      "enabled": true,
                                      "maxEstimatedCacheBytes": 1000
                                    }
                                  }
                                }
                                """;
            var path = PathKit.Combine(dir, "Squirix.settings.json");
            File.WriteAllText(path, json);
            var ok = UnifiedSettings.TryMergeMemoryPressureFromSettingsFilePath(path, new MemoryPressureOptions(), out var merged);
            Assert.True(ok);
            Assert.True(merged.Enabled);
            Assert.Equal(1000L, merged.MaxEstimatedCacheBytes);
        }
        finally
        {
            DirectoryKit.TryDeleteDirectory(dir);
        }
    }

    /// <summary>
    /// Verifies unified settings merge includes optional threshold and rejection properties under <c>MemoryPressure</c>.
    /// </summary>
    [Fact]
    public void UnifiedSettingsMergesThresholdAndRejectionFromFile()
    {
        var dir = DirectoryKit.CreateTempDirectory("squirix-mp-settings-thresholds");
        try
        {
            const string json = """
                                {
                                  "Squirix": {
                                    "MemoryPressure": {
                                      "enabled": true,
                                      "maxEstimatedCacheBytes": 5000,
                                      "highPressureThresholdPercent": 70,
                                      "criticalPressureThresholdPercent": 88,
                                      "rejectWritesOnCriticalPressure": false
                                    }
                                  }
                                }
                                """;
            var path = PathKit.Combine(dir, "Squirix.settings.json");
            File.WriteAllText(path, json);
            var ok = UnifiedSettings.TryMergeMemoryPressureFromSettingsFilePath(path, new MemoryPressureOptions(), out var merged);
            Assert.True(ok);
            merged.Validate();
            Assert.True(merged.Enabled);
            Assert.Equal(5000L, merged.MaxEstimatedCacheBytes);
            Assert.Equal(70, merged.HighPressureThresholdPercent);
            Assert.Equal(88, merged.CriticalPressureThresholdPercent);
            Assert.False(merged.RejectWritesOnCriticalPressure);
        }
        finally
        {
            DirectoryKit.TryDeleteDirectory(dir);
        }
    }
}
