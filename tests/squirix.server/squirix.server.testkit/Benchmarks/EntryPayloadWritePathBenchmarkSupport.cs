using System;
using System.Threading.Tasks;
using Squirix.Server.Limits;
using Squirix.Server.Storage.Journaling.JsonFramed;

namespace Squirix.Server.TestKit.Benchmarks;

/// <summary>Benchmark-facing wrappers for entry payload serialization paths on the write pipeline.</summary>
public static class EntryPayloadWritePathBenchmarkSupport
{
    /// <summary>Serializes the entry once using the discriminated journal JSON writer.</summary>
    /// <param name="entry">The cache entry to serialize.</param>
    /// <returns>The serialized byte length.</returns>
    public static async Task<int> DiscriminatedSerializeOnceAsync(CacheEntry<string> entry)
    {
        ArgumentNullException.ThrowIfNull(entry);

        var payload = await DiscriminatedEntryJsonWriter.BuildEntryJsonAsync(
            entry.Value,
            entry.ExpiresUtc,
            entry.Expiration,
            entry.Version,
            entry.Tags).ConfigureAwait(false);
        return payload.Length;
    }

    /// <summary>Simulates the current validation guard plus journal path with two independent discriminated serializations.</summary>
    /// <param name="entry">The cache entry to serialize.</param>
    /// <returns>The combined serialized byte length from both passes.</returns>
    public static async Task<int> DiscriminatedSerializeTwiceAsync(CacheEntry<string> entry)
    {
        ArgumentNullException.ThrowIfNull(entry);

        var guardBytes = await EntryPayloadSizeGuard.MeasureSerializedBytesAsync(entry).ConfigureAwait(false);
        var payload = await DiscriminatedEntryJsonWriter.BuildEntryJsonAsync(
            entry.Value,
            entry.ExpiresUtc,
            entry.Expiration,
            entry.Version,
            entry.Tags).ConfigureAwait(false);
        return guardBytes + payload.Length;
    }

    /// <summary>Simulates reusing one discriminated serialization for both validation and journal append.</summary>
    /// <param name="entry">The cache entry to serialize.</param>
    /// <returns>The serialized byte length after validation.</returns>
    public static async Task<int> SerializeOnceThenLengthCheckAsync(CacheEntry<string> entry)
    {
        ArgumentNullException.ThrowIfNull(entry);

        var payload = await DiscriminatedEntryJsonWriter.BuildEntryJsonAsync(
            entry.Value,
            entry.ExpiresUtc,
            entry.Expiration,
            entry.Version,
            entry.Tags).ConfigureAwait(false);
        EntryPayloadSizeGuard.EnsureDiscriminatedJsonWithinLimit(payload);
        return payload.Length;
    }
}
