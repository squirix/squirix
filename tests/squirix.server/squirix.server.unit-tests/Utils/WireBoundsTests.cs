using System;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.UnitTests.Support;
using Squirix.Server.Utils;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Utils;

/// <summary>Edge cases of the decoder range checks in <see cref="WireBounds" />.</summary>
[Immutable]
public sealed class WireBoundsTests : ServerUnitTestBase
{
    private const long MaxUnixMilliseconds = 253402300799999L;

    private const long MinUnixMilliseconds = -62135596800000L;

    /// <summary>A range that ends exactly at the buffer end fits, including an empty one at the end.</summary>
    [Test]
    public async Task FitsAcceptsExactEnd()
    {
        _ = await Assert.That(WireBounds.Fits(4, 6, 10)).IsTrue();
        _ = await Assert.That(WireBounds.Fits(0, 10, 10)).IsTrue();
        _ = await Assert.That(WireBounds.Fits(0, 0, 0)).IsTrue();
    }

    /// <summary>A negative length never fits.</summary>
    [Test]
    public async Task FitsRejectsNegativeLength()
    {
        _ = await Assert.That(WireBounds.Fits(-1, 0, 10)).IsFalse();
        _ = await Assert.That(WireBounds.Fits(int.MinValue, 5, 10)).IsFalse();
    }

    /// <summary>An offset before the buffer or past its end never fits.</summary>
    [Test]
    public async Task FitsRejectsOffsetOutsideBuffer()
    {
        _ = await Assert.That(WireBounds.Fits(0, -1, 10)).IsFalse();
        _ = await Assert.That(WireBounds.Fits(0, 11, 10)).IsFalse();
    }

    /// <summary>A length whose end would overflow an <see cref="int" /> sum does not fit.</summary>
    [Test]
    public async Task FitsRejectsOverflowingLength()
    {
        _ = await Assert.That(WireBounds.Fits(int.MaxValue, 8, 10)).IsFalse();
        _ = await Assert.That(WireBounds.Fits(5, 6, 10)).IsFalse();
    }

    /// <summary>Ticks between zero and <see cref="DateTime.MaxValue" /> convert to UTC; others are refused.</summary>
    [Test]
    public async Task UtcTicksBounds()
    {
        _ = await Assert.That(WireBounds.TryUtcFromTicks(0, out var min)).IsTrue();
        _ = await Assert.That(min.Kind).IsEqualTo(DateTimeKind.Utc);
        _ = await Assert.That(WireBounds.TryUtcFromTicks(DateTime.MaxValue.Ticks, out var max)).IsTrue();
        _ = await Assert.That(max.Ticks).IsEqualTo(DateTime.MaxValue.Ticks);
        _ = await Assert.That(WireBounds.IsUtcTicks(-1)).IsFalse();
        _ = await Assert.That(WireBounds.TryUtcFromTicks(DateTime.MaxValue.Ticks + 1, out _)).IsFalse();
    }

    /// <summary>Unix milliseconds inside the <see cref="DateTimeOffset" /> range convert; one past either end is refused.</summary>
    [Test]
    public async Task UnixMillisecondsBounds()
    {
        _ = await Assert.That(WireBounds.TryUtcFromUnixMilliseconds(0, out var epoch)).IsTrue();
        _ = await Assert.That(epoch).IsEqualTo(DateTime.UnixEpoch);
        _ = await Assert.That(WireBounds.IsUnixMilliseconds(MinUnixMilliseconds)).IsTrue();
        _ = await Assert.That(WireBounds.IsUnixMilliseconds(MaxUnixMilliseconds)).IsTrue();
        _ = await Assert.That(WireBounds.IsUnixMilliseconds(MinUnixMilliseconds - 1)).IsFalse();
        _ = await Assert.That(WireBounds.TryUtcFromUnixMilliseconds(MaxUnixMilliseconds + 1, out _)).IsFalse();
    }
}
