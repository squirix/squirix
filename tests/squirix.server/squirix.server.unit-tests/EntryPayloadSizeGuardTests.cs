using System;
using System.Globalization;
using System.Threading.Tasks;
using Squirix.Server.Core;
using Squirix.Server.Errors;
using Squirix.Server.Storage.Journaling;
using Squirix.Server.TestKit;
using Squirix.Server.UnitTests.Support;
using Xunit;

namespace Squirix.Server.UnitTests;

/// <summary>
/// Unit tests for <see cref="EntryPayloadSizeGuard" />.
/// </summary>
public sealed class EntryPayloadSizeGuardTests : ServerUnitTestBase
{
    /// <summary>Checks if an entry above the limit throws.</summary>
    [Fact]
    public async Task EntryJustAboveLimitThrowsPayloadTooLarge()
    {
        var value = await EntryLimitKit.CreateStringValueExceedingEntryLimitAsync();
        var entry = new NodeCacheEntry<object?> { Value = value, Version = 1 };

        var ex = Assert.Throws<SquirixException>(() => JournalEntryPayload.EnsureEncodedLengthWithinLimit(entry));

        Assert.Equal(SquirixErrorCode.PayloadTooLarge, ex.Code);
        Assert.Equal("PayloadTooLarge", ex.Error);
        Assert.Contains(EntryLimits.MaxEntrySizeBytes.ToString(CultureInfo.InvariantCulture), ex.Detail, StringComparison.Ordinal);
    }

    /// <summary>Checks if an entry below the limit doesn't throw.</summary>
    [Fact]
    public async Task EntryJustBelowLimitDoesNotThrow()
    {
        var value = await EntryLimitKit.CreateStringValueAtMostSerializedBytesAsync(EntryLimits.MaxEntrySizeBytes);
        var entry = new NodeCacheEntry<object?> { Value = value, Version = 1 };

        var ex = Record.Exception(() => JournalEntryPayload.EnsureEncodedLengthWithinLimit(entry));

        Assert.Null(ex);
        Assert.True(JournalEntryPayload.MeasureSerializedBytes(entry) <= EntryLimits.MaxEntrySizeBytes);
    }
}
