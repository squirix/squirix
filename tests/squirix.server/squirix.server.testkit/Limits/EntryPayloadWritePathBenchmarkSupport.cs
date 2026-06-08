using Squirix.Server.Limits;
using Squirix.Server.Storage.Journaling;

namespace Squirix.Server.TestKit.Limits;

/// <summary>
/// Benchmark-facing wrappers for entry payload serialization paths on the write pipeline.
/// </summary>
public static class EntryPayloadWritePathBenchmarkSupport
{
    /// <summary>
    /// Serializes the entry once using the discriminated journal JSON writer.
    /// </summary>
    /// <param name="entry">The cache entry to serialize.</param>
    /// <returns>The serialized byte length.</returns>
    public static int DiscriminatedSerializeOnce(CacheEntry<string> entry)
    {
        var payload = DiscriminatedEntryJsonWriter.BuildEntryJson(entry.Value, entry.ExpiresUtc, entry.Expiration, entry.Version, entry.Tags);
        return payload.Length;
    }

    /// <summary>
    /// Simulates the current validation guard plus journal path with two independent discriminated serializations.
    /// </summary>
    /// <param name="entry">The cache entry to serialize.</param>
    /// <returns>The combined serialized byte length from both passes.</returns>
    public static int DiscriminatedSerializeTwice(CacheEntry<string> entry)
    {
        var guardBytes = EntryPayloadSizeGuard.MeasureSerializedBytes(entry);
        var payload = DiscriminatedEntryJsonWriter.BuildEntryJson(entry.Value, entry.ExpiresUtc, entry.Expiration, entry.Version, entry.Tags);
        return guardBytes + payload.Length;
    }

    /// <summary>
    /// Simulates reusing one discriminated serialization for both validation and journal append.
    /// </summary>
    /// <param name="entry">The cache entry to serialize.</param>
    /// <returns>The serialized byte length after validation.</returns>
    public static int SerializeOnceThenLengthCheck(CacheEntry<string> entry)
    {
        var payload = DiscriminatedEntryJsonWriter.BuildEntryJson(entry.Value, entry.ExpiresUtc, entry.Expiration, entry.Version, entry.Tags);
        EntryPayloadSizeGuard.EnsureDiscriminatedJsonWithinLimit(payload);
        return payload.Length;
    }
}
