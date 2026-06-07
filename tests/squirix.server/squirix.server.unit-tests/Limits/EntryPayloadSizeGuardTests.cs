using System;
using System.Globalization;
using Squirix.Server.Errors;
using Squirix.Server.Limits;
using Squirix.Server.TestKit.Limits;
using Xunit;

namespace Squirix.Server.UnitTests.Limits;

/// <summary>
/// Unit tests for <see cref="EntryPayloadSizeGuard" />.
/// </summary>
public sealed class EntryPayloadSizeGuardTests : ServerUnitTestBase
{
    /// <summary>
    /// Checks if an entry below the limit doesn't throw.
    /// </summary>
    [Fact]
    public void EntryJustBelowLimitDoesNotThrow()
    {
        var value = EntryPayloadLimitTestHelpers.CreateStringValueAtMostSerializedBytes(SquirixEntryLimits.MaxEntrySizeBytes);
        var entry = new CacheEntry<object?> { Value = value, Version = 1 };

        var ex = Record.Exception(() => EntryPayloadSizeGuard.EnsureWithinLimit(entry));

        Assert.Null(ex);
        Assert.True(EntryPayloadSizeGuard.MeasureSerializedBytes(entry) <= SquirixEntryLimits.MaxEntrySizeBytes);
    }

    /// <summary>
    /// Checks if an entry below the limit throws.
    /// </summary>
    [Fact]
    public void EntryJustAboveLimitThrowsPayloadTooLarge()
    {
        var value = EntryPayloadLimitTestHelpers.CreateStringValueExceedingEntryLimit();
        var entry = new CacheEntry<object?> { Value = value, Version = 1 };

        var ex = Assert.Throws<SquirixException>(() => EntryPayloadSizeGuard.EnsureWithinLimit(entry));

        Assert.Equal(SquirixErrorCode.PayloadTooLarge, ex.Code);
        Assert.Equal("PayloadTooLarge", ex.Error);
        Assert.Contains(SquirixEntryLimits.MaxEntrySizeBytes.ToString(CultureInfo.InvariantCulture), ex.Detail, StringComparison.Ordinal);
    }
}
