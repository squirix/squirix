using System;
using System.Buffers;
using System.Text.Json;
using System.Text.Json.Serialization.Metadata;
using Google.Protobuf.WellKnownTypes;
using Grpc.Core;
using Squirix.Transport.Grpc;
using Squirix.Transport.Grpc.Cache;

namespace Squirix.Internal;

/// <summary>Maps native scalar/binary wire values and typed JSON entries without changing configured typed writer serialization.</summary>
internal static class ProtoEx
{
    private const string MalformedDurationDetail = "The server returned a malformed duration.";
    private const string MalformedTimestampDetail = "The server returned a malformed timestamp.";
    private const string MissingValueDetail = "The server returned a cache value without a kind.";
    private const string IncompatibleBinaryDetail = "The server returned opaque binary data for a non-binary cache value type.";

    internal static T? Read<T>(CacheValue value, ISquirixSerializer serializer)
    {
        ArgumentNullException.ThrowIfNull(serializer);
        return Decode<T>(value, serializer);
    }

    internal static CacheEntryWire MapEntryToProto<T>(CacheEntry<T> entry, ISquirixSerializer serializer)
    {
        ArgumentNullException.ThrowIfNull(entry);
        ArgumentNullException.ThrowIfNull(serializer);

        return new CacheEntryWire
        {
            Value = ToCacheValue(entry.Value, serializer),
            ExpiresUtc = entry.ExpiresUtc == null ? null : Timestamp.FromDateTime(DateTime.SpecifyKind(entry.ExpiresUtc.Value, DateTimeKind.Utc)),
            Expiration = entry.Expiration == null ? null : WireDuration.FromTimeSpan(entry.Expiration.Value),
        };
    }

    internal static CacheEntry<T> MapProtoEntryToCacheEntry<T>(CacheEntryWire entry, ISquirixSerializer serializer)
    {
        ArgumentNullException.ThrowIfNull(serializer);
        return new CacheEntry<T>
        {
            Value = Decode<T>(entry.Value, serializer),
            ExpiresUtc = entry.ExpiresUtc is { } expiresUtc ? ReadTimestamp(expiresUtc) : null,
            Expiration = entry.Expiration is { } expiration ? ReadDuration(expiration) : null,
        };
    }

    /// <summary>Converts a server-reported duration to a <see cref="TimeSpan" />, rejecting a non-normalized value.</summary>
    /// <param name="duration">The wire duration.</param>
    /// <returns>The converted time span.</returns>
    /// <exception cref="RpcException">Thrown with <see cref="StatusCode.Internal" /> when the server sent a malformed or missing duration.</exception>
    internal static TimeSpan ReadDuration(Duration? duration) => duration != null && WireDuration.IsNormalized(duration)
        ? WireDuration.ToTimeSpan(duration)
        : throw new RpcException(new Status(StatusCode.Internal, MalformedDurationDetail));

    /// <summary>Converts a server-reported timestamp to a UTC <see cref="DateTime" />, rejecting a non-normalized value.</summary>
    /// <param name="timestamp">The wire timestamp.</param>
    /// <returns>The converted UTC date and time.</returns>
    /// <exception cref="RpcException">Thrown with <see cref="StatusCode.Internal" /> when the server sent a malformed timestamp.</exception>
    internal static DateTime ReadTimestamp(Timestamp timestamp) => WireTimestamp.IsNormalized(timestamp)
        ? timestamp.ToDateTime().ToUniversalTime()
        : throw new RpcException(new Status(StatusCode.Internal, MalformedTimestampDetail));

    private static T? DeserializeScalar<T>(CacheValue value, ISquirixSerializer serializer, JsonTypeInfo<T>? typeInfo = null)
    {
        var buffer = new ArrayBufferWriter<byte>();
        CacheValueWire.WriteScalarJson(value, buffer);
        return serializer.Deserialize(buffer.WrittenSpan, typeInfo);
    }

    private static T? Decode<T>(CacheValue? value, ISquirixSerializer serializer)
    {
        var present = RequireValue(value);
        return typeof(T) == typeof(object) ? ProtoScalarMapping.Coerce<T>(DecodeObject(present)) : DecodeTyped<T>(present, serializer);
    }

    private static object? DecodeObject(CacheValue value) => value.KindCase switch
    {
        CacheValue.KindOneofCase.StringValue => value.StringValue,
        CacheValue.KindOneofCase.BoolValue => value.BoolValue,
        CacheValue.KindOneofCase.Int64Value => value.Int64Value,
        CacheValue.KindOneofCase.DoubleValue => value.DoubleValue,
        CacheValue.KindOneofCase.NullValue => null,
        CacheValue.KindOneofCase.JsonUtf8 => ParseElement(value.JsonUtf8.Span),
        CacheValue.KindOneofCase.BinaryValue => value.BinaryValue.ToByteArray(),
        _ => throw new RpcException(new Status(StatusCode.Internal, MissingValueDetail)),
    };

    private static T? DecodeTyped<T>(CacheValue value, ISquirixSerializer serializer) =>
        ProtoScalarMapping.TryMapTypedPrimitive<T>(value, out var primitive)
            ? primitive
            : value.KindCase switch
            {
                CacheValue.KindOneofCase.NullValue => default,
                CacheValue.KindOneofCase.JsonUtf8 => serializer.Deserialize<T>(value.JsonUtf8.Span),
                CacheValue.KindOneofCase.BinaryValue => DecodeBinary<T>(value),
                _ => DeserializeScalar<T>(value, serializer),
            };

    private static T? DecodeBinary<T>(CacheValue value) => typeof(T) == typeof(byte[])
        ? ProtoScalarMapping.Coerce<T>(value.BinaryValue.ToByteArray())
        : throw new RpcException(new Status(StatusCode.Internal, IncompatibleBinaryDetail));

    private static JsonElement ParseElement(ReadOnlySpan<byte> json)
    {
        var reader = new Utf8JsonReader(json);
        return JsonElement.ParseValue(ref reader);
    }

    private static CacheValue RequireValue(CacheValue? value) =>
        value == null || value.KindCase == CacheValue.KindOneofCase.None ? throw new RpcException(new Status(StatusCode.Internal, MissingValueDetail)) : value;

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
