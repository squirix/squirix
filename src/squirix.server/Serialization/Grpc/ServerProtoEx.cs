using System;
using System.Buffers;
using System.Runtime.CompilerServices;
using System.Text.Json;
using System.Text.Json.Serialization.Metadata;
using System.Threading.Tasks;
using Google.Protobuf.WellKnownTypes;
using Grpc.Core;
using Squirix.Server.Core;
using Squirix.Transport.Grpc;
using Squirix.Transport.Grpc.Cache;
using RpcEntry = Squirix.Transport.Grpc.Cache.CacheEntryWire;

namespace Squirix.Server.Serialization.Grpc;

internal static class ServerProtoEx
{
    private const string InvalidJsonDetail = "The cache value is not exactly one valid JSON value (UTF-8, no byte order mark, at most 64 levels of nesting, no comments or trailing commas).";
    private const string MissingValueDetail = "The cache value is missing.";
    private const string NonNormalizedDurationDetail = "Duration is not normalized: nanos must be within -999999999..999999999 and share the sign of seconds.";
    private const string NonNormalizedTimestampDetail = "Timestamp is not normalized: nanos must be within 0..999999999 and seconds within 0001-01-01..9999-12-31.";

    /// <summary>Maps a cache value to its wire form.</summary>
    /// <typeparam name="T">Logical cache value type.</typeparam>
    /// <param name="value">Value to encode.</param>
    /// <returns>A scalar kind for a scalar, the stored JSON bytes themselves (shared, not copied) for a structured value, otherwise the serialized JSON.</returns>
    internal static CacheValue CacheValueToGrpcValue<T>(T? value)
    {
        return value switch
        {
            null => CacheValueWire.Null(),
            string text => new CacheValue { StringValue = text },
            int number => new CacheValue { Int64Value = number },
            long number => new CacheValue { Int64Value = number },
            double number => new CacheValue { DoubleValue = number },
            bool boolean => new CacheValue { BoolValue = boolean },
            JsonUtf8Value json => CacheValueWire.FromJson(json.Bytes),
            JsonElement element => CacheValueWire.FromElement(element),
            sbyte or byte or short or ushort or uint => new CacheValue { Int64Value = NarrowIntegerToInt64(value) },
            float number => new CacheValue { DoubleValue = number },
            _ => CacheValueWire.FromJson(ServerJsonSerializer.SerializeToJsonValue<object?>(value).Bytes),
        };
    }

    internal static ValueTask<T?> MapCacheValueAsync<T>(CacheValue value, JsonTypeInfo<T>? typeInfo = null) => new(ReadValue(value, typeInfo));

    internal static ValueTask<NodeCacheEntry<T>> MapFromProtoAsync<T>(this RpcEntry e)
    {
        var value = ReadValue<T>(e.Value);
        DateTime? expires = null;
        if (e.ExpiresUtc != null && (e.ExpiresUtc.Seconds != 0 || e.ExpiresUtc.Nanos != 0))
        {
            if (!WireTimestamp.IsNormalized(e.ExpiresUtc))
                throw new RpcException(new Status(StatusCode.InvalidArgument, NonNormalizedTimestampDetail));

            expires = e.ExpiresUtc.ToDateTime().ToUniversalTime();
        }

        return new ValueTask<NodeCacheEntry<T>>(
            new NodeCacheEntry<T>
            {
                Value = value,
                ExpiresUtc = expires,
                Expiration = e.Expiration is { } expiration ? RequireDuration(expiration) : null,
            });
    }

    /// <summary>Converts a wire duration to a <see cref="TimeSpan" />, rejecting non-normalized input.</summary>
    /// <param name="duration">The wire duration.</param>
    /// <returns>The converted time span.</returns>
    /// <exception cref="RpcException">Thrown with <see cref="StatusCode.InvalidArgument" /> when nanos are out of range or their sign opposes the seconds.</exception>
    internal static TimeSpan RequireDuration(Duration duration) => WireDuration.IsNormalized(duration)
        ? WireDuration.ToTimeSpan(duration)
        : throw new RpcException(new Status(StatusCode.InvalidArgument, NonNormalizedDurationDetail));

    internal static RpcEntry MapToProto<T>(this NodeCacheEntry<T> e) => new()
    {
        Value = CacheValueToGrpcValue(e.Value),
        ExpiresUtc = e.ExpiresUtc == null ? null : Timestamp.FromDateTime(DateTime.SpecifyKind(e.ExpiresUtc.Value, DateTimeKind.Utc)),
        Expiration = e.Expiration == null ? null : WireDuration.FromTimeSpan(e.Expiration.Value),
    };

    private static TOut CastReference<TOut, TIn>(TIn input)
        where TIn : class?
    {
        // Local copy keeps the cast site distinct from client ProtoEx.ReinterpretReference.
        var held = input;
        return Unsafe.As<TIn, TOut>(ref held);
    }

    private static TOut CastScalar<TOut, TIn>(TIn input)
        where TIn : struct => Unsafe.As<TIn, TOut>(ref input);

    private static T? Coerce<T>(object? value) => value is T result ? result : default;

    private static T? DeserializeScalar<T>(CacheValue wire, JsonTypeInfo<T>? typeInfo)
    {
        var buffer = new ArrayBufferWriter<byte>();
        CacheValueWire.WriteScalarJson(wire, buffer);
        return ServerJsonSerializer.Deserialize(buffer.WrittenSpan, typeInfo);
    }

    private static long NarrowIntegerToInt64<T>(T value) => value switch
    {
        sbyte number => number,
        byte number => number,
        short number => number,
        ushort number => number,
        uint number => number,
        _ => throw new ArgumentOutOfRangeException(nameof(value), value, "The value is not a narrow integer."),
    };

    private static object? ReadObject(CacheValue wire) => wire.KindCase switch
    {
        CacheValue.KindOneofCase.StringValue => wire.StringValue,
        CacheValue.KindOneofCase.BoolValue => wire.BoolValue,
        CacheValue.KindOneofCase.Int64Value => wire.Int64Value,
        CacheValue.KindOneofCase.DoubleValue => wire.DoubleValue,
        CacheValue.KindOneofCase.NullValue => null,
        CacheValue.KindOneofCase.JsonUtf8 => JsonUtf8Value.TryCreate(wire.JsonUtf8.Memory, out var json)
            ? json
            : throw new RpcException(new Status(StatusCode.InvalidArgument, InvalidJsonDetail)),
        _ => throw new RpcException(new Status(StatusCode.InvalidArgument, MissingValueDetail)),
    };

    private static T? ReadTyped<T>(CacheValue wire, JsonTypeInfo<T>? typeInfo) => TryReadExactPrimitive(wire, out T? exact)
        ? exact
        : wire.KindCase switch
        {
            CacheValue.KindOneofCase.NullValue => default,
            CacheValue.KindOneofCase.JsonUtf8 when JsonUtf8Validator.IsValid(wire.JsonUtf8.Span) => ServerJsonSerializer.Deserialize(wire.JsonUtf8.Span, typeInfo),
            CacheValue.KindOneofCase.JsonUtf8 => throw new RpcException(new Status(StatusCode.InvalidArgument, InvalidJsonDetail)),
            CacheValue.KindOneofCase.StringValue or CacheValue.KindOneofCase.BoolValue or CacheValue.KindOneofCase.Int64Value or CacheValue.KindOneofCase.DoubleValue =>
                DeserializeScalar(wire, typeInfo),
            _ => throw new RpcException(new Status(StatusCode.InvalidArgument, MissingValueDetail)),
        };

    private static T? ReadValue<T>(CacheValue? wire, JsonTypeInfo<T>? typeInfo = null)
    {
        var present = RequireValue(wire);
        return typeof(T) == typeof(object) ? Coerce<T>(ReadObject(present)) : ReadTyped(present, typeInfo);
    }

    private static CacheValue RequireValue(CacheValue? wire) =>
        wire == null || wire.KindCase == CacheValue.KindOneofCase.None ? throw new RpcException(new Status(StatusCode.InvalidArgument, MissingValueDetail)) : wire;

    private static bool TryReadInt32<T>(CacheValue wire, out T? decoded)
    {
        decoded = default;
        if (wire.KindCase != CacheValue.KindOneofCase.Int64Value || wire.Int64Value is < int.MinValue or > int.MaxValue)
            return false;

        decoded = CastScalar<T, int>(int.CreateChecked(wire.Int64Value));
        return true;
    }

    private static bool TryReadExactPrimitive<T>(CacheValue wire, out T? decoded)
    {
        // Dispatch by destination CLR type first (client ProtoEx switches on KindCase).
        decoded = default;
        var destination = typeof(T);

        if (destination == typeof(string))
        {
            if (wire.KindCase != CacheValue.KindOneofCase.StringValue)
                return false;

            decoded = CastReference<T, string>(wire.StringValue);
            return true;
        }

        if (destination == typeof(bool))
        {
            if (wire.KindCase != CacheValue.KindOneofCase.BoolValue)
                return false;

            decoded = CastScalar<T, bool>(wire.BoolValue);
            return true;
        }

        if (destination == typeof(int))
            return TryReadInt32(wire, out decoded);

        if (destination == typeof(long))
        {
            if (wire.KindCase != CacheValue.KindOneofCase.Int64Value)
                return false;

            decoded = CastScalar<T, long>(wire.Int64Value);
            return true;
        }

        if (destination != typeof(double))
            return false;
        if (wire.KindCase != CacheValue.KindOneofCase.DoubleValue)
            return false;

        decoded = CastScalar<T, double>(wire.DoubleValue);
        return true;
    }
}
