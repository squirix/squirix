using System;
using Squirix.Server.Node.MemoryPressure;
using Xunit;

namespace Squirix.Server.UnitTests.Node.MemoryPressure;

/// <summary>
/// Tests for <see cref="MemoryPressureOptionsResolver" />.
/// </summary>
public sealed class MemoryPressureOptionsResolverTests
{
    /// <summary>
    /// Verifies unset max bytes defaults to 80% of available memory.
    /// </summary>
    [Fact]
    public void ResolveDefaultsMaxBytesToRamCap()
    {
        var resolved = MemoryPressureOptionsResolver.Resolve(new UnresolvedMemoryPressureOptions(), new FixedMemoryBudgetProvider(1_000_000));

        Assert.Equal(800_000L, resolved.MaxEstimatedCacheBytes);
    }

    /// <summary>
    /// Verifies explicit max bytes below the RAM cap are preserved.
    /// </summary>
    [Fact]
    public void ResolvePreservesConfiguredMaxBelowCap()
    {
        var resolved = MemoryPressureOptionsResolver.Resolve(
            new UnresolvedMemoryPressureOptions { MaxEstimatedCacheBytes = 500_000 },
            new FixedMemoryBudgetProvider(1_000_000));

        Assert.Equal(500_000L, resolved.MaxEstimatedCacheBytes);
    }

    /// <summary>
    /// Verifies explicit max bytes above the RAM cap fail resolution.
    /// </summary>
    [Fact]
    public void ResolveRejectsConfiguredMaxAboveRamCap()
    {
        var ex = Assert.Throws<InvalidOperationException>(static () => MemoryPressureOptionsResolver.Resolve(
            new UnresolvedMemoryPressureOptions { MaxEstimatedCacheBytes = 900_000 },
            new FixedMemoryBudgetProvider(1_000_000)));

        Assert.Contains("exceeds the 80% RAM cap", ex.Message, StringComparison.Ordinal);
    }

    /// <summary>
    /// Verifies non-positive explicit max bytes fail resolution.
    /// </summary>
    [Fact]
    public void ResolveRejectsNonPositiveConfiguredMax()
    {
        var ex = Assert.Throws<InvalidOperationException>(static () => MemoryPressureOptionsResolver.Resolve(
            new UnresolvedMemoryPressureOptions { MaxEstimatedCacheBytes = 0 },
            new FixedMemoryBudgetProvider(1_000_000)));

        Assert.Contains("must be positive", ex.Message, StringComparison.Ordinal);
    }

    /// <summary>
    /// Verifies zero available memory fails resolution.
    /// </summary>
    [Fact]
    public void ResolveRejectsZeroAvailableMemory()
    {
        var ex = Assert.Throws<InvalidOperationException>(static () => MemoryPressureOptionsResolver.Resolve(
            new UnresolvedMemoryPressureOptions(),
            new FixedMemoryBudgetProvider(0)));

        Assert.Contains("available process memory is zero", ex.Message, StringComparison.Ordinal);
    }

    private sealed class FixedMemoryBudgetProvider(long availableBytes) : IMemoryBudgetProvider
    {
        public long GetTotalAvailableBytes() => availableBytes;
    }
}
