using System;
using System.Threading.Tasks;
using Squirix.Server.Errors;
using Squirix.Server.Storage.Journaling;
using Squirix.Server.Storage.Journaling.Entries;

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
        var bytes = await BuildJournalDiscriminatedJsonAsync(entry).ConfigureAwait(false);
        entry.PreparedJournalDiscriminatedJson = bytes;
        EnsureDiscriminatedJsonWithinLimit(bytes);
    }

    public static async Task<int> MeasureSerializedBytesAsync<T>(CacheEntry<T> entry) =>
        (await BuildJournalDiscriminatedJsonAsync(entry).ConfigureAwait(false)).Length;

    private static async Task<byte[]> BuildJournalDiscriminatedJsonAsync<T>(CacheEntry<T> entry)
    {
        var (expiresUtc, expiration) = JournalEntryExpirationMaterializer.ForJournalWrite(entry.ExpiresUtc, entry.Expiration);
        return await DiscriminatedEntryJsonWriter.BuildEntryJsonAsync(entry.Value, expiresUtc, expiration, entry.Version, entry.Tags).ConfigureAwait(false);
    }
}
