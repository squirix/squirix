using System;
using System.Threading.Tasks;
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

    public static async Task EnsureWithinLimitAsync<T>(CacheEntry<T> entry)
    {
        if (await MeasureSerializedBytesAsync(entry).ConfigureAwait(false) > SquirixEntryLimits.MaxEntrySizeBytes)
            throw CacheOperationContract.PayloadTooLarge(SquirixEntryLimits.MaxEntrySizeBytes);
    }

    public static async Task<int> MeasureSerializedBytesAsync<T>(CacheEntry<T> entry) =>
        (await DiscriminatedEntryJsonWriter.BuildEntryJsonAsync(entry.Value, entry.ExpiresUtc, entry.Expiration, entry.Version, entry.Tags).ConfigureAwait(false)).Length;
}
