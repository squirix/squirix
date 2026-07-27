using System;
using Squirix.Server.Node.MemoryPressure;
using Squirix.Server.TestKit;
using Squirix.Server.UnitTests.Support;
using Xunit;

namespace Squirix.Server.UnitTests.Memory;

/// <summary>
/// Tests for <see cref="OptionsResolver" />.
/// </summary>
public sealed class PressureOptionsResolverTests
{
    /// <summary>Verifies unset max bytes defaults to 80% of available memory.</summary>
    [Fact]
    public void ResolveDefaultsMaxBytesToRamCap()
    {
        var resolved = OptionsResolver.Resolve(new UnresolvedMemoryPressureOptions(), new FixedMemoryBudgetProvider(1_000_000));

        Assert.Equal(800_000L, resolved.MaxEstimatedCacheBytes);
    }

    /// <summary>Verifies explicit max bytes below the RAM cap are preserved.</summary>
    [Fact]
    public void ResolvePreservesConfiguredMaxBelowCap()
    {
        var resolved = OptionsResolver.Resolve(new UnresolvedMemoryPressureOptions { MaxEstimatedCacheBytes = 500_000 }, new FixedMemoryBudgetProvider(1_000_000));

        Assert.Equal(500_000L, resolved.MaxEstimatedCacheBytes);
    }

    /// <summary>Verifies explicit max bytes above the RAM cap fail resolution.</summary>
    [Fact]
    public void ResolveRejectsConfiguredMaxAboveRamCap()
    {
        var ex = NodeExceptionAssert.For<InvalidOperationException>().Throws(
            900_000L,
            static value => _ = OptionsResolver.Resolve(new UnresolvedMemoryPressureOptions { MaxEstimatedCacheBytes = value }, new FixedMemoryBudgetProvider(1_000_000)));

        Assert.Contains("exceeds the 80% RAM cap", ex.Message, StringComparison.Ordinal);
    }

    /// <summary>Verifies non-positive explicit max bytes fail resolution.</summary>
    [Fact]
    public void ResolveRejectsNonPositiveConfiguredMax()
    {
        var ex = NodeExceptionAssert.For<InvalidOperationException>().Throws(
            0L,
            static value => _ = OptionsResolver.Resolve(new UnresolvedMemoryPressureOptions { MaxEstimatedCacheBytes = value }, new FixedMemoryBudgetProvider(1_000_000)));

        Assert.Contains("must be positive", ex.Message, StringComparison.Ordinal);
    }

    /// <summary>Verifies zero available memory fails resolution.</summary>
    [Fact]
    public void ResolveRejectsZeroAvailableMemory()
    {
        var ex = NodeExceptionAssert.For<InvalidOperationException>().Throws(
            0L,
            static value => _ = OptionsResolver.Resolve(new UnresolvedMemoryPressureOptions(), new FixedMemoryBudgetProvider(value)));

        Assert.Contains("available process memory is zero", ex.Message, StringComparison.Ordinal);
    }
}
