using System;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.Core;
using Squirix.Server.Errors;
using Squirix.Server.Storage.Journaling;
using Squirix.Server.TestKit;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests;

/// <summary>Unit tests for <see cref="EntryPayloadSizeGuard" />.</summary>
[Immutable]
public sealed class EntryPayloadSizeGuardTests : ServerUnitTestBase
{
    /// <summary>Direct length and span overloads reject oversized payloads.</summary>
    [Test]
    public async Task EnsureOverloadsRejectOversizedPayloads()
    {
        const int overLength = EntryLimits.MaxEntrySizeBytes + 1;
        var lengthEx = NodeExceptionAssert.For<SquirixException>().Throws(overLength, static value => EntryPayloadSizeGuard.EnsureLengthWithinLimit(value));
        _ = await Assert.That(lengthEx.Code).IsEqualTo(SquirixErrorCode.PayloadTooLarge);

        var bytes = new byte[overLength];
        var bytesEx = NodeExceptionAssert.For<SquirixException>().Throws(bytes, static value => EntryPayloadSizeGuard.EnsureEntryBytesWithinLimit(value.AsSpan()));
        _ = await Assert.That(bytesEx.Code).IsEqualTo(SquirixErrorCode.PayloadTooLarge);

        EntryPayloadSizeGuard.EnsureLengthWithinLimit(EntryLimits.MaxEntrySizeBytes);
        EntryPayloadSizeGuard.EnsureEntryBytesWithinLimit([]);
    }

    /// <summary>Checks if an entry above the limit throws.</summary>
    [Test]
    public async Task EntryJustAboveLimitThrowsPayloadTooLarge()
    {
        var value = await EntryLimitKit.CreateStringOverEntryLimitAsync();
        var entry = new NodeCacheEntry<object?> { Value = value, Version = 1 };

        var ex = NodeExceptionAssert.For<SquirixException>().Throws(entry, static value => JournalEntryPayload.EnsureEncodedLengthWithinLimit(value));

        _ = await Assert.That(ex.Code).IsEqualTo(SquirixErrorCode.PayloadTooLarge);
        _ = await Assert.That(ex.Error).IsEqualTo("PayloadTooLarge");
        _ = await Assert.That(ex.Detail).Contains("4194304", StringComparison.Ordinal);
    }

    /// <summary>Checks if an entry below the limit doesn't throw.</summary>
    [Test]
    public async Task EntryJustBelowLimitDoesNotThrow()
    {
        var value = await EntryLimitKit.CreateStringAtMostSerializedBytesAsync(EntryLimits.MaxEntrySizeBytes);
        var entry = new NodeCacheEntry<object?> { Value = value, Version = 1 };

        JournalEntryPayload.EnsureEncodedLengthWithinLimit(entry);
        _ = await Assert.That(JournalEntryPayload.MeasureSerializedBytes(entry) <= EntryLimits.MaxEntrySizeBytes).IsTrue();
    }
}
