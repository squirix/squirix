using System;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.Core;
using Squirix.Server.Node.MemoryPressure;
using Squirix.Server.TestKit;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Memory;

/// <summary>Tests for <see cref="PressureOptions" /> defaults and validation.</summary>
[Immutable]
public sealed class PressureOptionsTests
{
    /// <summary>Verifies default threshold values match the contract.</summary>
    [Test]
    public async Task DefaultsMatchContract()
    {
        var resolved = OptionsResolver.Resolve(new UnresolvedMemoryPressureOptions(), RocksDoubles.CreateMemoryBudget(10_000));
        _ = await Assert.That(resolved.MaxEstimatedCacheBytes).IsEqualTo(8_000L);
        _ = await Assert.That(resolved.HighPressureThresholdPercent).IsEqualTo(80);
        _ = await Assert.That(resolved.CriticalPressureThresholdPercent).IsEqualTo(95);
    }

    /// <summary>Verifies local threshold boundaries remain accepted before cross-property validation runs.</summary>
    [Test]
    public async Task FieldValidationAcceptsBoundaries()
    {
        var options = new PressureOptions
        {
            MaxEstimatedCacheBytes = 1,
            HighPressureThresholdPercent = 1,
            CriticalPressureThresholdPercent = 100,
        };

        options.Validate();
        _ = await Assert.That(options.MaxEstimatedCacheBytes).IsEqualTo(1);
        _ = await Assert.That(options.HighPressureThresholdPercent).IsEqualTo(1);
        _ = await Assert.That(options.CriticalPressureThresholdPercent).IsEqualTo(100);
    }

    /// <summary>Verifies JSON binding still applies valid option values through init setters.</summary>
    [Test]
    public async Task JsonDeserializeBindsValidatedScalars()
    {
        const string json = """{"maxEstimatedCacheBytes":4096,"highPressureThresholdPercent":60,"criticalPressureThresholdPercent":90}""";
        var options = new ServerJsonSerializer().Deserialize<PressureOptions>(json);
        _ = await Assert.That(options).IsNotNull();
        options.Validate();
        _ = await Assert.That(options.MaxEstimatedCacheBytes).IsEqualTo(4096);
        _ = await Assert.That(options.HighPressureThresholdPercent).IsEqualTo(60);
        _ = await Assert.That(options.CriticalPressureThresholdPercent).IsEqualTo(90);
    }

    /// <summary>Verifies invalid threshold combinations are rejected.</summary>
    /// <param name="critical">Critical threshold value.</param>
    /// <param name="high">High threshold value.</param>
    /// <param name="expectedMessageFragment">Expected validation detail fragment.</param>
    [Test]
    [Arguments(101, 80, nameof(PressureOptions.CriticalPressureThresholdPercent))]
    [Arguments(90, 90, "HighPressureThresholdPercent")]
    [Arguments(90, 0, nameof(PressureOptions.HighPressureThresholdPercent))]
    public async Task RejectsInvalidThresholdCombos(int critical, int high, string expectedMessageFragment)
    {
        var options = new PressureOptions
        {
            MaxEstimatedCacheBytes = 1024,
            HighPressureThresholdPercent = high,
            CriticalPressureThresholdPercent = critical,
        };

        var ex = NodeExceptionAssert.For<InvalidOperationException>().Throws(options, static value => value.Validate());

        _ = await Assert.That(ex.Message).Contains(expectedMessageFragment, StringComparison.Ordinal);
    }

    /// <summary>Verifies a representative valid configuration passes <see cref="PressureOptions.Validate" />.</summary>
    [Test]
    public void ValidateAcceptsValidConfiguration()
    {
        var options = new PressureOptions
        {
            MaxEstimatedCacheBytes = 1024,
            HighPressureThresholdPercent = 50,
            CriticalPressureThresholdPercent = 90,
        };

        options.Validate();
    }

    /// <summary>Verifies non-positive byte limits are rejected.</summary>
    /// <param name="maxBytes">Invalid limit value.</param>
    [Test]
    [Arguments(0)]
    [Arguments(-1)]
    [Arguments(-1000)]
    public async Task ValidateRejectsNonPositiveMaxBytes(long maxBytes)
    {
        var options = new PressureOptions { MaxEstimatedCacheBytes = maxBytes };
        var ex = NodeExceptionAssert.For<InvalidOperationException>().Throws(options, static value => value.Validate());

        _ = await Assert.That(ex.Message).Contains(nameof(PressureOptions.MaxEstimatedCacheBytes), StringComparison.Ordinal);
    }
}
