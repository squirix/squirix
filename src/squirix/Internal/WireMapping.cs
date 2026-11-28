using System;
using System.Buffers;
using System.Text.Json;
using System.Text.Json.Serialization.Metadata;
using Squirix.Transport;
using Squirix.Transport.Wire;
using Squirix.Transport.Wire.Cache;

namespace Squirix.Internal;

/// <summary>Maps native scalar/binary wire values and typed JSON entries without changing configured typed writer serialization.</summary>
internal static class WireMapping
{
    private const string MalformedDurationDetail = "The server returned a malformed duration.";
    private const string MissingValueDetail = "The server returned a cache value without a kind.";
    private const string IncompatibleBinaryDetail = "The server returned opaque binary data for a non-binary cache value type.";

    internal static T? Read<T>(CacheValue? value, ISquirixSerializer serializer)
    {
        ArgumentNullException.ThrowIfNull(serializer);
        return Decode<T>(value, serializer);
    }

    internal static CacheEntryWire CreateWireEntry<T>(T? value, ISquirixSerializer serializer)
    {
        ArgumentNullException.ThrowIfNull(serializer);
        return new CacheEntryWire { Value = ToCacheValue(value, serializer) };
    }

    internal static CacheEntry<T> MapWireEntryToCacheEntry<T>(CacheEntryWire? entry, ISquirixSerializer serializer, DateTime? rebasedDeadline = null)
    {
        ArgumentNullException.ThrowIfNull(serializer);
        return new CacheEntry<T>
        {
            Value = Decode<T>(entry?.Value, serializer),
            ExpiresUtc = rebasedDeadline ?? entry?.ExpiresUtc,
            Expiration = entry?.Expiration,
        };
    }

    internal static TimeSpan ReadDuration(TimeSpan? duration) => duration ?? MissingDuration();

    private static TimeSpan MissingDuration() => throw new SquirixTransportException(new CallStatus(CacheTransportStatusCode.Internal, MalformedDurationDetail));

    private static T? DeserializeScalar<T>(CacheValue value, ISquirixSerializer serializer, JsonTypeInfo<T>? typeInfo = null)
    {
        var buffer = new ArrayBufferWriter<byte>();
        CacheValueWire.WriteScalarJson(value, buffer);
        return serializer.Deserialize(buffer.WrittenSpan, typeInfo);
    }

    private static T? Decode<T>(CacheValue? value, ISquirixSerializer serializer)
    {
        var present = RequireValue(value);
        return typeof(T) == typeof(object) ? WireScalarMapping.Coerce<T>(DecodeObject(present)) : DecodeTyped<T>(present, serializer);
    }

    private static object? DecodeObject(CacheValue value) => value.KindCase switch
    {
        CacheValue.KindOneofCase.StringValue => value.StringValue,
        CacheValue.KindOneofCase.BoolValue => value.BoolValue,
        CacheValue.KindOneofCase.Int64Value => value.Int64Value,
        CacheValue.KindOneofCase.DoubleValue => value.DoubleValue,
        CacheValue.KindOneofCase.NullValue => null,
        CacheValue.KindOneofCase.JsonUtf8 => ParseElement(value.JsonUtf8.Span),
        CacheValue.KindOneofCase.BinaryValue => value.TakeBinaryValue(),
        _ => throw new SquirixTransportException(new CallStatus(CacheTransportStatusCode.Internal, MissingValueDetail)),
    };

    private static T? DecodeTyped<T>(CacheValue value, ISquirixSerializer serializer) =>
        WireScalarMapping.TryMapTypedPrimitive<T>(value, out var primitive)
            ? primitive
            : value.KindCase switch
            {
                CacheValue.KindOneofCase.NullValue => default,
                CacheValue.KindOneofCase.JsonUtf8 => serializer.Deserialize<T>(value.JsonUtf8.Span),
                CacheValue.KindOneofCase.BinaryValue => DecodeBinary<T>(value),
                _ => DeserializeScalar<T>(value, serializer),
            };

    private static T? DecodeBinary<T>(CacheValue value) => typeof(T) == typeof(byte[])
        ? WireScalarMapping.Coerce<T>(value.TakeBinaryValue())
        : throw new SquirixTransportException(new CallStatus(CacheTransportStatusCode.Internal, IncompatibleBinaryDetail));

    private static JsonElement ParseElement(ReadOnlySpan<byte> json)
    {
        var reader = new Utf8JsonReader(json);
        return JsonElement.ParseValue(ref reader);
    }

    private static CacheValue RequireValue(CacheValue? value) =>
        value == null || value.KindCase == CacheValue.KindOneofCase.None ? throw new SquirixTransportException(new CallStatus(CacheTransportStatusCode.Internal, MissingValueDetail)) : value;

    private static CacheValue ToCacheValue<T>(T? value, ISquirixSerializer serializer)
    {
        return value switch
        {
            null => CacheValueWire.Null(),
            string text => new CacheValue { StringValue = text },
            int number => new CacheValue { Int64Value = number },
            long number => new CacheValue { Int64Value = number },
            double number => new CacheValue { DoubleValue = number },
            bool boolean => new CacheValue { BoolValue = boolean },
            JsonElement element => CacheValueWire.FromElement(element),
            _ => CacheValueWire.FromJson(serializer.SerializeToUtf8Bytes(value)),
        };
    }
}
