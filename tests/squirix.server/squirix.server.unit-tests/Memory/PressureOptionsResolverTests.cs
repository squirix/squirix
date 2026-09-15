using System;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.Node.MemoryPressure;
using Squirix.Server.TestKit;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Memory;

/// <summary>Tests for <see cref="OptionsResolver" />.</summary>
[Immutable]
public sealed class PressureOptionsResolverTests
{
    /// <summary>Verifies unset max bytes defaults to 80% of available memory.</summary>
    [Test]
    public async Task ResolveDefaultsMaxBytesToRamCap()
    {
        var resolved = OptionsResolver.Resolve(new UnresolvedMemoryPressureOptions(), RocksDoubles.CreateMemoryBudget(1_000_000));

        _ = await Assert.That(resolved.MaxEstimatedCacheBytes).IsEqualTo(800_000L);
    }

    /// <summary>Verifies explicit max bytes below the RAM cap are preserved.</summary>
    [Test]
    public async Task ResolvePreservesConfiguredMaxBelowCap()
    {
        var resolved = OptionsResolver.Resolve(new UnresolvedMemoryPressureOptions { MaxEstimatedCacheBytes = 500_000 }, RocksDoubles.CreateMemoryBudget(1_000_000));

        _ = await Assert.That(resolved.MaxEstimatedCacheBytes).IsEqualTo(500_000L);
    }

    /// <summary>Verifies explicit max bytes above the RAM cap fail resolution.</summary>
    [Test]
    public async Task ResolveRejectsConfiguredMaxAboveRamCap()
    {
        var ex = NodeExceptionAssert.For<InvalidOperationException>().Throws(
            900_000L,
            static value => _ = OptionsResolver.Resolve(new UnresolvedMemoryPressureOptions { MaxEstimatedCacheBytes = value }, RocksDoubles.CreateMemoryBudget(1_000_000)));

        _ = await Assert.That(ex.Message).Contains("exceeds the 80% RAM cap", StringComparison.Ordinal);
    }

    /// <summary>Verifies non-positive explicit max bytes fail resolution.</summary>
    [Test]
    public async Task ResolveRejectsNonPositiveConfiguredMax()
    {
        var ex = NodeExceptionAssert.For<InvalidOperationException>().Throws(
            0L,
            static value => _ = OptionsResolver.Resolve(new UnresolvedMemoryPressureOptions { MaxEstimatedCacheBytes = value }, RocksDoubles.CreateMemoryBudget(1_000_000)));

        _ = await Assert.That(ex.Message).Contains("must be positive", StringComparison.Ordinal);
    }

    /// <summary>Verifies zero available memory fails resolution.</summary>
    [Test]
    public async Task ResolveRejectsZeroAvailableMemory()
    {
        var ex = NodeExceptionAssert.For<InvalidOperationException>().Throws(
            0L,
            static value => _ = OptionsResolver.Resolve(new UnresolvedMemoryPressureOptions(), RocksDoubles.CreateMemoryBudget(value)));

        _ = await Assert.That(ex.Message).Contains("available process memory is zero", StringComparison.Ordinal);
    }
}
