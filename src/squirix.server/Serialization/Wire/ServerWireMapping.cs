using System;
using System.Buffers;
using System.Runtime.CompilerServices;
using System.Text.Json;
using Squirix.Server.Core;
using Squirix.Transport.Binary;
using Squirix.Transport.Wire;
using Squirix.Transport.Wire.Cache;
using RpcEntry = Squirix.Transport.Wire.Cache.CacheEntryWire;

namespace Squirix.Server.Serialization.Wire;

internal static class ServerWireMapping
{
    private const string IncompatibleBinaryDetail = "Opaque binary data requires a binary cache value type.";
    private const string InvalidJsonDetail = "The cache value is not exactly one valid JSON value (UTF-8, no byte order mark, at most 64 levels of nesting, no comments or trailing commas).";
    private const string MissingValueDetail = "The cache value is missing.";

    /// <summary>Maps a cache value to its wire form.</summary>
    /// <typeparam name="T">Logical cache value type.</typeparam>
    /// <param name="value">Value to encode.</param>
    /// <returns>A native scalar, an owned binary copy, shared immutable JSON bytes, or serialized JSON for other values.</returns>
    internal static CacheValue CacheValueToWire<T>(T? value)
    {
        return value switch
        {
            StoredValue stored => ToWire(in stored),
            null => CacheValueWire.Null(),
            string text => new CacheValue { StringValue = text },
            int number => new CacheValue { Int64Value = number },
            long number => new CacheValue { Int64Value = number },
            double number => new CacheValue { DoubleValue = number },
            bool boolean => new CacheValue { BoolValue = boolean },
            byte[] bytes => CacheValueWire.FromBinary(bytes),
            JsonUtf8Value json => CacheValueWire.FromJson(json.Bytes),
            JsonElement element => CacheValueWire.FromElement(element),
            sbyte or byte or short or ushort or uint => new CacheValue { Int64Value = NarrowIntegerToInt64(value) },
            float number => new CacheValue { DoubleValue = number },
            _ => CacheValueWire.FromJson(ServerJsonSerializer.SerializeToJsonValue<object?>(value).Bytes),
        };
    }

    internal static T? MapCacheValue<T>(CacheValue? wire)
    {
        var present = RequireValue(wire);
        if (typeof(T) == typeof(StoredValue))
        {
            var stored = FromWire(present);
            return Unsafe.As<StoredValue, T>(ref stored);
        }

        return ReadTyped<T>(present);
    }

    internal static NodeCacheEntry<T> MapFromWire<T>(this RpcEntry? entry) => new()
    {
        Value = MapCacheValue<T>(entry?.Value),
        ExpiresUtc = entry?.ExpiresUtc,
        Expiration = entry?.Expiration,
    };

    internal static RpcEntry MapToWire<T>(this NodeCacheEntry<T> e, bool includeDeadline = true) => new()
    {
        Value = CacheValueToWire(e.Value),
        ExpiresUtc = includeDeadline ? e.ExpiresUtc : null,
        Expiration = e.Expiration,
    };

    private static TOut CastReference<TOut, TIn>(TIn input)
        where TIn : class?
    {
        // Local copy keeps the cast site distinct from client WireMapping.ReinterpretReference.
        var held = input;
        return Unsafe.As<TIn, TOut>(ref held);
    }

    private static TOut CastScalar<TOut, TIn>(TIn input)
        where TIn : struct => Unsafe.As<TIn, TOut>(ref input);

    private static T? DeserializeScalar<T>(CacheValue wire)
    {
        var buffer = new ArrayBufferWriter<byte>();
        CacheValueWire.WriteScalarJson(wire, buffer);
        return ServerJsonSerializer.Deserialize<T>(buffer.WrittenSpan);
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

    private static T? ReadBinary<T>(CacheValue wire) => typeof(T) == typeof(byte[])
        ? CastReference<T, byte[]>(wire.BinaryValue.ToArray())
        : throw new ServerCallException(FrameStatus.InvalidArgument, IncompatibleBinaryDetail);

    private static CacheValue ToWire(in StoredValue value) => value.Kind switch
    {
        StoredValueKind.Null => CacheValueWire.Null(),
        StoredValueKind.Boolean => new CacheValue { BoolValue = value.Boolean },
        StoredValueKind.Integer => new CacheValue { Int64Value = value.Integer },
        StoredValueKind.Double => new CacheValue { DoubleValue = value.Double },
        StoredValueKind.Text => CacheValue.FromText(value.Bytes),
        StoredValueKind.Binary => CacheValueWire.FromBinary(value.Bytes),
        StoredValueKind.Json => CacheValueWire.FromJson(value.Bytes),
        _ => throw new InvalidOperationException("Invalid stored cache value."),
    };

    private static StoredValue FromWire(CacheValue value) => value.KindCase switch
    {
        CacheValue.KindOneofCase.NullValue => default,
        CacheValue.KindOneofCase.BoolValue => StoredValue.FromBoolean(value.BoolValue),
        CacheValue.KindOneofCase.Int64Value => StoredValue.FromInteger(value.Int64Value),
        CacheValue.KindOneofCase.DoubleValue => StoredValue.FromDouble(value.DoubleValue),
        CacheValue.KindOneofCase.StringValue => StoredValue.FromText(value.EncodedText ?? System.Text.Encoding.UTF8.GetBytes(value.StringValue)),
        CacheValue.KindOneofCase.BinaryValue => StoredValue.FromBinary(value.BinaryValue.ToArray()),
        CacheValue.KindOneofCase.JsonUtf8 when JsonUtf8Validator.IsValid(value.JsonUtf8.Span) => StoredValue.FromJson(value.JsonUtf8),
        CacheValue.KindOneofCase.JsonUtf8 => throw new ServerCallException(FrameStatus.InvalidArgument, InvalidJsonDetail),
        _ => throw new ServerCallException(FrameStatus.InvalidArgument, MissingValueDetail),
    };

    private static T? ReadTyped<T>(CacheValue wire) => TryReadExactPrimitive(wire, out T? exact)
        ? exact
        : wire.KindCase switch
        {
            CacheValue.KindOneofCase.NullValue => default,
            CacheValue.KindOneofCase.BinaryValue => ReadBinary<T>(wire),
            CacheValue.KindOneofCase.JsonUtf8 when JsonUtf8Validator.IsValid(wire.JsonUtf8.Span) => ServerJsonSerializer.Deserialize<T>(wire.JsonUtf8.Span),
            CacheValue.KindOneofCase.JsonUtf8 => throw new ServerCallException(FrameStatus.InvalidArgument, InvalidJsonDetail),
            CacheValue.KindOneofCase.StringValue or CacheValue.KindOneofCase.BoolValue or CacheValue.KindOneofCase.Int64Value or CacheValue.KindOneofCase.DoubleValue =>
                DeserializeScalar<T>(wire),
            _ => throw new ServerCallException(FrameStatus.InvalidArgument, MissingValueDetail),
        };

    private static CacheValue RequireValue(CacheValue? wire) =>
        wire == null || wire.KindCase == CacheValue.KindOneofCase.None ? throw new ServerCallException(FrameStatus.InvalidArgument, MissingValueDetail) : wire;

    private static bool TryReadExactPrimitive<T>(CacheValue wire, out T? decoded)
    {
        // Dispatch by destination CLR type first (client WireMapping switches on KindCase).
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

    private static bool TryReadInt32<T>(CacheValue wire, out T? decoded)
    {
        decoded = default;
        if (wire.KindCase != CacheValue.KindOneofCase.Int64Value || wire.Int64Value is < int.MinValue or > int.MaxValue)
            return false;

        decoded = CastScalar<T, int>(int.CreateChecked(wire.Int64Value));
        return true;
    }
}
