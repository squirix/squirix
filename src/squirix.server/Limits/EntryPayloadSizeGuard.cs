using System;
using Squirix.Server.Errors;
using Squirix.Server.Storage.Journaling;

namespace Squirix.Server.Limits;

/// <summary>
/// Validates discriminated journal entry JSON size against <see cref="SquirixEntryLimits.MaxEntrySizeBytes" />.
/// </summary>
internal static class EntryPayloadSizeGuard
{
    public static void EnsureDiscriminatedJsonWithinLimit(ReadOnlySpan<byte> discriminatedEntryJson)
    {
        if (discriminatedEntryJson.Length > SquirixEntryLimits.MaxEntrySizeBytes)
            throw CacheOperationContract.PayloadTooLarge(SquirixEntryLimits.MaxEntrySizeBytes);
    }

    public static void EnsureWithinLimit<T>(CacheEntry<T> entry)
    {
        if (MeasureSerializedBytes(entry) > SquirixEntryLimits.MaxEntrySizeBytes)
            throw CacheOperationContract.PayloadTooLarge(SquirixEntryLimits.MaxEntrySizeBytes);
    }

    public static int MeasureSerializedBytes<T>(CacheEntry<T> entry) =>
        DiscriminatedEntryJsonWriter.BuildEntryJson(entry.Value, entry.ExpiresUtc, entry.Expiration, entry.Version, entry.Tags).Length;
}
