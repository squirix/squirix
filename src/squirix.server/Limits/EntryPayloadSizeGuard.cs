using System;
using Squirix.Server.Errors;
using Squirix.Server.Storage.Journaling.Entries;

namespace Squirix.Server.Limits;

/// <summary>
/// Validates journal entry payload size against <see cref="SquirixEntryLimits.MaxEntrySizeBytes" />.
/// </summary>
internal static class EntryPayloadSizeGuard
{
    public static void EnsureEncodedLengthWithinLimit<T>(CacheEntry<T> entry)
    {
        if (JournalEntryPayload.ComputeEncodedLength(entry) > SquirixEntryLimits.MaxEntrySizeBytes)
            throw CacheOperationContract.PayloadTooLarge(SquirixEntryLimits.MaxEntrySizeBytes);
    }

    public static void EnsureEntryBytesWithinLimit(ReadOnlySpan<byte> entryBytes)
    {
        if (entryBytes.Length > SquirixEntryLimits.MaxEntrySizeBytes)
            throw CacheOperationContract.PayloadTooLarge(SquirixEntryLimits.MaxEntrySizeBytes);
    }

    public static void EnsureWithinLimit<T>(CacheEntry<T> entry)
    {
        var bytes = JournalEntryPayload.Encode(entry);
        entry.PreparedJournalEntryBytes = bytes;
        EnsureEntryBytesWithinLimit(bytes);
    }

    public static int MeasureSerializedBytes<T>(CacheEntry<T> entry) => JournalEntryPayload.ComputeEncodedLength(entry);
}
