using System.Buffers;
using System.Text.Json;
using Squirix.Server.Limits;
using Squirix.Server.Serialization;

namespace Squirix.Server.TestKit.Limits;

/// <summary>
/// Helpers for constructing entry payloads near fixed size limits in tests.
/// </summary>
public static class EntryPayloadLimitTestHelpers
{
    /// <summary>
    /// Returns the largest string payload whose discriminated entry JSON is at most <paramref name="maxSerializedBytes" />.
    /// </summary>
    /// <param name="maxSerializedBytes">Maximum allowed discriminated JSON byte length.</param>
    /// <returns>A string value whose serialized entry size is within the limit.</returns>
    public static string CreateStringValueAtMostSerializedBytes(int maxSerializedBytes)
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

        return new string('x', low);
    }

    /// <summary>
    /// Returns the largest string payload whose REST <see cref="CacheEntry{T}" /> JSON is at most <paramref name="maxSerializedBytes" />.
    /// </summary>
    /// <param name="maxSerializedBytes">Maximum allowed REST entry JSON byte length.</param>
    /// <returns>A string value whose REST entry JSON is within the limit.</returns>
    public static string CreateStringValueAtMostRestEntryBytes(int maxSerializedBytes)
    {
        var options = CreateRestJsonOptions();
        var low = 0;
        var high = maxSerializedBytes;

        while (low < high)
        {
            var mid = low + ((high - low + 1) / 2);
            if (MeasureRestEntryString(mid, options) <= maxSerializedBytes)
                low = mid;
            else
                high = mid - 1;
        }

        return new string('x', low);
    }

    /// <summary>
    /// Returns the smallest string payload whose discriminated entry JSON exceeds <see cref="SquirixEntryLimits.MaxEntrySizeBytes" />.
    /// </summary>
    /// <returns>A string value guaranteed to exceed the entry limit once serialized.</returns>
    public static string CreateStringValueExceedingEntryLimit() =>
        new('x', CreateStringValueAtMostSerializedBytes(SquirixEntryLimits.MaxEntrySizeBytes).Length + 1);

    /// <summary>
    /// Returns the largest string payload whose discriminated entry JSON fits the fixed server entry limit.
    /// </summary>
    /// <returns>A near-limit string value for benchmarks and integration tests.</returns>
    public static string CreateNearLimitDiscriminatedStringValue() =>
        CreateStringValueAtMostSerializedBytes(SquirixEntryLimits.MaxEntrySizeBytes);

    private static int MeasureStringPayload(int stringLength) =>
        EntryPayloadSizeGuard.MeasureSerializedBytes(new CacheEntry<object?> { Value = new string('x', stringLength), Version = 1 });

    private static JsonSerializerOptions CreateRestJsonOptions()
    {
        var options = new JsonSerializerOptions(JsonSerializerDefaults.Web) { PropertyNameCaseInsensitive = true };
        options.Converters.Add(new StoredJsonPayloadConverter());
        return options;
    }

    private static int MeasureRestEntryString(int stringLength, JsonSerializerOptions options)
    {
        var writer = new ArrayBufferWriter<byte>();
        using (var jsonWriter = new Utf8JsonWriter(writer))
            JsonSerializer.Serialize(jsonWriter, new CacheEntry<string> { Value = new string('x', stringLength) }, options);

        return writer.WrittenCount;
    }
}
