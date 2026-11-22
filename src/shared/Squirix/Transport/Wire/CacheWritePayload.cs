using Squirix.Transport.Wire.Cache;
using Squirix.Transport.Wire.Codec;

namespace Squirix.Transport.Wire;

/// <summary>Measures the serialized value carried by a cache write, without its metadata.</summary>
internal static class CacheWritePayload
{
    internal static int? Length(WireMessage request) => request switch
    {
        SetEntryAsyncRequest set => ValueLength(set.Entry?.Value),
        TryAddEntryAsyncRequest add => ValueLength(add.Entry?.Value),
        UpdateAsyncRequest update => ValueLength(update.Entry?.Value),
        GetOrAddAsyncRequest add => ValueLength(add.Entry?.Value),
        SetDistributedAsyncRequest set => set.Value.Length,
        _ => null,
    };

    private static int ValueLength(CacheValue? value) => value?.KindCase switch
    {
        CacheValue.KindOneofCase.StringValue => value.EncodedText?.Length ?? WireReader.Utf8.GetByteCount(value.StringValue),
        CacheValue.KindOneofCase.BinaryValue => value.BinaryValue.Length,
        CacheValue.KindOneofCase.JsonUtf8 => value.JsonUtf8.Length,
        _ => value?.CalculateSize() ?? 0,
    };
}
