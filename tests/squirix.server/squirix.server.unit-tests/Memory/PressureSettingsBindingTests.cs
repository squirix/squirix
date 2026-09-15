using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.Core;
using Squirix.Server.Node.MemoryPressure;
using Squirix.Server.TestKit.IO;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Memory;

/// <summary>Tests JSON merge and configuration binding for memory pressure settings.</summary>
[Immutable]
public sealed class PressureSettingsBindingTests : ServerUnitTestBase
{
    /// <summary>Verifies System.Text.Json round-trip preserves option values (same shape as JSON configuration files).</summary>
    [Test]
    public async Task JsonSerializerRoundTripBindsOptionNames()
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
        _ = await Assert.That(restored).IsNotNull();
        restored.Validate();
        _ = await Assert.That(restored).IsEqualTo(original);
    }

    /// <summary>
    /// Verifies System.Text.Json binds private <c language="csharp">MemoryPressure</c> section properties
    /// (via <see cref="System.Text.Json.Serialization.JsonIncludeAttribute" />) and merge overrides the baseline.
    /// </summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task MergeAppliesJsonOverrides(CancellationToken cancellationToken)
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
            cancellationToken);
        var (found, merged) = await PressureBootstrap.MergeFromSettingsFilePathAsync(settings.Path, baseline, cancellationToken);

        _ = await Assert.That(found).IsTrue();
        _ = await Assert.That(merged.MaxEstimatedCacheBytes).IsEqualTo(4096);
        _ = await Assert.That(merged.HighPressureThresholdPercent).IsEqualTo(70);
        _ = await Assert.That(merged.CriticalPressureThresholdPercent).IsEqualTo(90);
    }

    /// <summary>Verifies a partial JSON section overrides only present fields and keeps baseline for absent ones.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task MergeKeepsBaselineForAbsentFields(CancellationToken cancellationToken)
    {
        var baseline = new UnresolvedMemoryPressureOptions
        {
            MaxEstimatedCacheBytes = 1024,
            HighPressureThresholdPercent = 80,
            CriticalPressureThresholdPercent = 95,
        };

        using var settings = await TempSettingsFile.WriteAsync("squirix-mp-", """{"MemoryPressure":{"highPressureThresholdPercent":60}}""", cancellationToken);
        var (found, merged) = await PressureBootstrap.MergeFromSettingsFilePathAsync(settings.Path, baseline, cancellationToken);

        _ = await Assert.That(found).IsTrue();
        _ = await Assert.That(merged.MaxEstimatedCacheBytes).IsEqualTo(1024);
        _ = await Assert.That(merged.HighPressureThresholdPercent).IsEqualTo(60);
        _ = await Assert.That(merged.CriticalPressureThresholdPercent).IsEqualTo(95);
    }
}
