using System;
using System.Diagnostics.CodeAnalysis;
using System.IO;
using System.Runtime.CompilerServices;
using System.Text.Json;
using Google.Protobuf.WellKnownTypes;
using Squirix.Server.Storage.Entries.Binary;
using Squirix.Transport.Grpc.Cache;

namespace Squirix.Server.Adapters.Grpc;

/// <summary>Maps CLR cache values to and from gRPC cache wire messages.</summary>
internal static class CacheWireCodec
{
    internal static T? FromCacheValue<T>(CacheValue value)
    {
        ArgumentNullException.ThrowIfNull(value);

        if (typeof(T) == typeof(object))
            return Coerce<T>(NormalizeUntypedScalar(MapCacheValueAsObject(value)));

        if (TryReadTypedScalar(value, out T? scalar))
            return scalar;

        return value.KindCase switch
        {
            CacheValue.KindOneofCase.NullValue or CacheValue.KindOneofCase.None => default,
            CacheValue.KindOneofCase.Payload when !value.Payload.IsEmpty => DecodePayload<T>(value.Payload.Span),
            _ => throw new InvalidDataException("Unsupported cache value kind."),
        };
    }

    internal static T? FromEntryWireValue<T>(CacheEntryWire entry)
    {
        ArgumentNullException.ThrowIfNull(entry);

        if (entry.Payload.IsEmpty)
            return default;

        if (typeof(T) == typeof(object))
            return Coerce<T>(NormalizeUntypedScalar(DecodeUntypedPayload(entry.Payload.Span)));

        return DecodePayload<T>(entry.Payload.Span);
    }

    internal static CacheValue ToCacheValue<T>(T? value)
    {
        return value switch
        {
            null => new CacheValue { NullValue = NullValue.NullValue },
            string text => new CacheValue { StringValue = text },
            int number => new CacheValue { Int64Value = number },
            long number => new CacheValue { Int64Value = number },
            double number => new CacheValue { DoubleValue = number },
            bool boolean => new CacheValue { BoolValue = boolean },
            JsonElement json => ToCacheValueFromJsonElement(json),
            _ => new CacheValue { Payload = GrpcWireByteStringEx.WrapPayload(CacheEntryCodec.EncodeWireValueToOwned(value)) },
        };
    }

    internal static CacheValue ToCacheValueFromWirePayload(ReadOnlyMemory<byte> wirePayload) => new() { Payload = GrpcWireByteStringEx.WrapPayload(wirePayload) };

    internal static CacheEntryWire ToEntryWire<T>(T? value) => new()
    {
        Payload = GrpcWireByteStringEx.WrapPayload(CacheEntryCodec.EncodeWireValueToOwned(value)),
    };

    private static T? Coerce<T>(object? value) => value is T result ? result : default;

    private static T? DecodePayload<T>(ReadOnlySpan<byte> payload)
    {
        if (!CacheEntryCodec.TryReadWireValue<T>(payload, out var value))
            throw new InvalidDataException("Binary cache value payload is invalid or truncated.");

        return value;
    }

    private static object? DecodeUntypedPayload(ReadOnlySpan<byte> payload)
    {
        if (!CacheEntryCodec.TryReadWireValueUntyped(payload, out var decoded))
            throw new InvalidDataException("Binary cache value payload is invalid or truncated.");

        return decoded switch
        {
            JsonElement element => MaterializeJsonElementUntyped(element),
            _ => decoded,
        };
    }

    private static object? MapCacheValueAsObject(CacheValue value)
    {
        return value.KindCase switch
        {
            CacheValue.KindOneofCase.StringValue => value.StringValue,
            CacheValue.KindOneofCase.BoolValue => value.BoolValue,
            CacheValue.KindOneofCase.Int64Value => value.Int64Value is >= int.MinValue and <= int.MaxValue ? Convert.ToInt32(value.Int64Value) : value.Int64Value,
            CacheValue.KindOneofCase.DoubleValue => value.DoubleValue,
            CacheValue.KindOneofCase.NullValue or CacheValue.KindOneofCase.None => null,
            CacheValue.KindOneofCase.Payload when !value.Payload.IsEmpty => DecodeUntypedPayload(value.Payload.Span),
            _ => throw new InvalidDataException("Unsupported cache value kind."),
        };
    }

    private static object? MaterializeJsonElementUntyped(JsonElement element)
    {
        return element.ValueKind switch
        {
            JsonValueKind.String => element.GetString(),
            JsonValueKind.True => true,
            JsonValueKind.False => false,
            JsonValueKind.Number when element.TryGetInt64(out var integer) => integer is >= int.MinValue and <= int.MaxValue ? Convert.ToInt32(integer) : integer,
            JsonValueKind.Number => element.GetDouble(),
            JsonValueKind.Null => null,
            _ => element.Clone(),
        };
    }

    private static object? NormalizeUntypedScalar(object? value)
    {
        return value switch
        {
            long longValue and >= int.MinValue and <= int.MaxValue => Convert.ToInt32(longValue),
            double doubleValue when double.IsInteger(doubleValue) && doubleValue is >= int.MinValue and <= int.MaxValue => Convert.ToInt32(doubleValue),
            double doubleValue when double.IsInteger(doubleValue) && doubleValue is >= long.MinValue and <= long.MaxValue => Convert.ToInt64(doubleValue),
            _ => value,
        };
    }

    private static TTarget ReinterpretReference<TTarget, TValue>(TValue value)
        where TValue : class?
    {
        var reference = value;
        return Unsafe.As<TValue, TTarget>(ref reference);
    }

    private static TTarget ReinterpretScalar<TTarget, TValue>(TValue value)
        where TValue : struct => Unsafe.As<TValue, TTarget>(ref value);

    private static CacheValue ToCacheValueFromJsonElement(JsonElement json)
    {
        return json.ValueKind switch
        {
            JsonValueKind.Null => new CacheValue { NullValue = NullValue.NullValue },
            JsonValueKind.String => new CacheValue { StringValue = json.GetString() },
            JsonValueKind.True => new CacheValue { BoolValue = true },
            JsonValueKind.False => new CacheValue { BoolValue = false },
            JsonValueKind.Number when json.TryGetInt64(out var integer) => new CacheValue { Int64Value = integer },
            JsonValueKind.Number => new CacheValue { DoubleValue = json.GetDouble() },
            _ => new CacheValue { Payload = GrpcWireByteStringEx.WrapPayload(CacheEntryCodec.EncodeWireValueToOwned(json)) },
        };
    }

    private static bool TryReadTypedScalar<T>(CacheValue value, [MaybeNullWhen(false)] out T result)
    {
        switch (value.KindCase)
        {
            case CacheValue.KindOneofCase.StringValue when typeof(T) == typeof(string):
                result = ReinterpretReference<T, string>(value.StringValue);
                return true;

            case CacheValue.KindOneofCase.BoolValue when typeof(T) == typeof(bool):
                result = ReinterpretScalar<T, bool>(value.BoolValue);
                return true;

            case CacheValue.KindOneofCase.Int64Value when typeof(T) == typeof(long):
                result = ReinterpretScalar<T, long>(value.Int64Value);
                return true;

            case CacheValue.KindOneofCase.Int64Value when typeof(T) == typeof(int) && value.Int64Value is >= int.MinValue and <= int.MaxValue:
                var intValue = Convert.ToInt32(value.Int64Value);
                result = ReinterpretScalar<T, int>(intValue);
                return true;

            case CacheValue.KindOneofCase.DoubleValue when typeof(T) == typeof(double):
                result = ReinterpretScalar<T, double>(value.DoubleValue);
                return true;
            default:
                result = default;
                return false;
        }
    }
}
