using System.Threading.Tasks;
using Squirix.Server.Limits;
using Squirix.Server.Storage.Journaling.Entries;

namespace Squirix.Server.TestKit.Journaling;

/// <summary>Helpers for constructing entry payloads near fixed size limits in tests.</summary>
public static class EntryLimitKit
{
    /// <summary>Returns the largest string payload whose encoded entry bytes fit the fixed server entry limit.</summary>
    /// <returns>A near-limit string value for benchmarks and integration tests.</returns>
    public static Task<string> CreateNearLimitStringValueAsync() => CreateStringValueAtMostSerializedBytesAsync(EntryLimits.MaxEntrySizeBytes);

    /// <summary>
    /// Returns the largest string payload whose encoded entry size is at most <paramref name="maxSerializedBytes" />.
    /// </summary>
    /// <param name="maxSerializedBytes">Maximum allowed entry byte length.</param>
    /// <returns>A string value whose serialized entry size is within the limit.</returns>
    public static Task<string> CreateStringValueAtMostSerializedBytesAsync(int maxSerializedBytes)
    {
        var low = 0;
        var high = maxSerializedBytes;

        while (low < high)
        {
            var mid = low + ((high - low + 1) / 2);
            if (MeasureStringPayload(mid) <= maxSerializedBytes)
                low = mid;
            else
                high = mid - 1;
        }

        return Task.FromResult(new string('x', low));
    }

    /// <summary>
    /// Returns the smallest string payload whose encoded entry exceeds <see cref="EntryLimits.MaxEntrySizeBytes" />.
    /// </summary>
    /// <returns>A string value guaranteed to exceed the entry limit once serialized.</returns>
    public static async Task<string> CreateStringValueExceedingEntryLimitAsync() => new(
        'x',
        (await CreateStringValueAtMostSerializedBytesAsync(EntryLimits.MaxEntrySizeBytes).ConfigureAwait(false)).Length + 1);

    private static int MeasureStringPayload(int stringLength) =>
        JournalEntryPayload.MeasureSerializedBytes(new NodeCacheEntry<object?> { Value = new string('x', stringLength), Version = 1 });
}
