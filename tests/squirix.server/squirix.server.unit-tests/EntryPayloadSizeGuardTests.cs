using System;
using System.Buffers;
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
    /// <summary>Direct length and span overloads reject oversized payloads.</summary>
    [Fact]
    public void EnsureOverloadsRejectOversizedPayloads()
    {
        const int overLength = EntryLimits.MaxEntrySizeBytes + 1;
        var lengthEx = NodeExceptionAssert.For<SquirixException>().Throws(overLength, static value => EntryPayloadSizeGuard.EnsureLengthWithinLimit(value));
        Assert.Equal(SquirixErrorCode.PayloadTooLarge, lengthEx.Code);

        var rented = ArrayPool<byte>.Shared.Rent(overLength);
        try
        {
            EntryPayloadSizeGuard.EnsureEntryBytesWithinLimit(rented.AsSpan(0, overLength));
            Assert.Fail("Expected PayloadTooLarge.");
        }
        catch (SquirixException bytesEx)
        {
            Assert.Equal(SquirixErrorCode.PayloadTooLarge, bytesEx.Code);
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(rented);
        }

        EntryPayloadSizeGuard.EnsureLengthWithinLimit(EntryLimits.MaxEntrySizeBytes);
        EntryPayloadSizeGuard.EnsureEntryBytesWithinLimit([]);
    }

    /// <summary>Checks if an entry above the limit throws.</summary>
    [Fact]
    public async Task EntryJustAboveLimitThrowsPayloadTooLarge()
    {
        var value = await EntryLimitKit.CreateStringValueExceedingEntryLimitAsync();
        var entry = new NodeCacheEntry<object?> { Value = value, Version = 1 };

        var ex = NodeExceptionAssert.For<SquirixException>().Throws(entry, static value => JournalEntryPayload.EnsureEncodedLengthWithinLimit(value));

        Assert.Equal(SquirixErrorCode.PayloadTooLarge, ex.Code);
        Assert.Equal("PayloadTooLarge", ex.Error);
        Assert.Contains("4194304", ex.Detail, StringComparison.Ordinal);
    }

    /// <summary>Checks if an entry below the limit doesn't throw.</summary>
    [Fact]
    public async Task EntryJustBelowLimitDoesNotThrow()
    {
        var value = await EntryLimitKit.CreateStringValueAtMostSerializedBytesAsync(EntryLimits.MaxEntrySizeBytes);
        var entry = new NodeCacheEntry<object?> { Value = value, Version = 1 };

        JournalEntryPayload.EnsureEncodedLengthWithinLimit(entry);
        Assert.True(JournalEntryPayload.MeasureSerializedBytes(entry) <= EntryLimits.MaxEntrySizeBytes);
    }
}
