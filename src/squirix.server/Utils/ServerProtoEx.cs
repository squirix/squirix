using System;
using System.Buffers;
using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;
using System.Text.Json;
using System.Threading.Tasks;
using Google.Protobuf.WellKnownTypes;
using Squirix.Server.Core;
using Squirix.Server.Runtime;
using Squirix.Transport.Grpc.Cache;
using RpcEntry = Squirix.Transport.Grpc.Cache.CacheEntryWire;

namespace Squirix.Server.Utils;

internal static class ServerProtoEx
{
    /// <summary>Maps a cache value to the compact value-only gRPC wire form.</summary>
    /// <typeparam name="T">Logical cache value type.</typeparam>
    /// <param name="value">Value to encode.</param>
    /// <returns>Compact protobuf value suitable for the value-only read path.</returns>
    internal static CacheValue CacheValueToGrpcValue<T>(T? value)
    {
        return value switch
        {
            null => new CacheValue { NullValue = NullValue.NullValue },
            string text => new CacheValue { StringValue = text },
            int number => new CacheValue { Int32Value = number },
            long number => new CacheValue { Int64Value = number },
            double number => new CacheValue { DoubleValue = number },
            bool boolean => new CacheValue { BoolValue = boolean },
            _ => new CacheValue { StructValue = ToStruct(value) },
        };
    }

    internal static ValueTask<T?> MapCacheValueAsync<T>(CacheValue value)
    {
        if (typeof(T) == typeof(object))
            return new ValueTask<T?>(Coerce<T>(MapCacheValueAsObject(value)));

        if (TryDecodeExactWirePrimitive(value, out T? exact))
            return new ValueTask<T?>(exact);

        return FinishMapCacheValueAfterExactMissAsync<T>(value);
    }

    internal static ValueTask<NodeCacheEntry<T>> MapFromProtoAsync<T>(this RpcEntry e)
    {
        var value = FromStruct<T>(e.Value);
        DateTime? expires = null;
        if (e.ExpiresUtc is not null && (e.ExpiresUtc.Seconds != 0 || e.ExpiresUtc.Nanos is not 0))
            expires = e.ExpiresUtc.ToDateTime().ToUniversalTime();

        if (typeof(T) == typeof(object))
            value = Coerce<T>(value);

        return new ValueTask<NodeCacheEntry<T>>(
            new NodeCacheEntry<T>
            {
                Value = value,
                ExpiresUtc = expires,
                Expiration = e.Expiration?.ToTimeSpan(),
            });
    }

    internal static RpcEntry MapToProto<T>(this NodeCacheEntry<T> e) => new()
    {
        Value = ToStruct(e.Value),
        ExpiresUtc = e.ExpiresUtc is null ? null : Timestamp.FromDateTime(DateTime.SpecifyKind(e.ExpiresUtc.Value, DateTimeKind.Utc)),
        Expiration = e.Expiration is null ? null : Duration.FromTimeSpan(e.Expiration.Value),
    };

    private static ValueTask<T?> FinishMapCacheValueAfterExactMissAsync<T>(CacheValue wire)
    {
        var kind = wire.KindCase;
        if (kind is CacheValue.KindOneofCase.NullValue or CacheValue.KindOneofCase.None)
            return new ValueTask<T?>(default(T?));

        if (kind is CacheValue.KindOneofCase.StructValue)
        {
            if (wire.StructValue is null)
                throw new ArgumentOutOfRangeException(nameof(wire), "Unsupported cache value kind.");

            return new ValueTask<T?>(FromStruct<T>(wire.StructValue));
        }

        if (IsWireScalarKind(kind))
            return new ValueTask<T?>(FromStruct<T>(WrapWireScalarAsStruct(wire)));

        throw new ArgumentOutOfRangeException(nameof(wire), "Unsupported cache value kind.");
    }

    private static bool IsWireScalarKind(CacheValue.KindOneofCase kind) =>
        kind switch
        {
            CacheValue.KindOneofCase.StringValue
                or CacheValue.KindOneofCase.BoolValue
                or CacheValue.KindOneofCase.Int32Value
                or CacheValue.KindOneofCase.Int64Value
                or CacheValue.KindOneofCase.DoubleValue => true,
            _ => false,
        };

    private static bool TryDecodeExactWirePrimitive<T>(CacheValue wire, out T? decoded)
    {
        // Dispatch by destination CLR type first (client ProtoEx switches on KindCase).
        decoded = default;
        var destination = typeof(T);

        if (destination == typeof(string))
        {
            if (wire.KindCase is not CacheValue.KindOneofCase.StringValue)
                return false;

            decoded = CastReference<T, string>(wire.StringValue);
            return true;
        }

        if (destination == typeof(bool))
        {
            if (wire.KindCase is not CacheValue.KindOneofCase.BoolValue)
                return false;

            decoded = CastScalar<T, bool>(wire.BoolValue);
            return true;
        }

        if (destination == typeof(int))
        {
            if (wire.KindCase is not CacheValue.KindOneofCase.Int32Value)
                return false;

            decoded = CastScalar<T, int>(int.CreateChecked(wire.Int32Value));
            return true;
        }

        if (destination == typeof(long))
        {
            if (wire.KindCase is not CacheValue.KindOneofCase.Int64Value)
                return false;

            decoded = CastScalar<T, long>(wire.Int64Value);
            return true;
        }

        if (destination == typeof(double))
        {
            if (wire.KindCase is not CacheValue.KindOneofCase.DoubleValue)
                return false;

            decoded = CastScalar<T, double>(wire.DoubleValue);
            return true;
        }

        return false;
    }

    private static Struct WrapWireScalarAsStruct(CacheValue wire)
    {
        Value boxed;
        switch (wire.KindCase)
        {
            case CacheValue.KindOneofCase.StringValue:
                boxed = Value.ForString(wire.StringValue);
                break;
            case CacheValue.KindOneofCase.BoolValue:
                boxed = Value.ForBool(wire.BoolValue);
                break;
            case CacheValue.KindOneofCase.Int32Value:
                boxed = Value.ForNumber(wire.Int32Value);
                break;
            case CacheValue.KindOneofCase.Int64Value:
                boxed = Value.ForNumber(wire.Int64Value);
                break;
            case CacheValue.KindOneofCase.DoubleValue:
                boxed = Value.ForNumber(wire.DoubleValue);
                break;
            case CacheValue.KindOneofCase.NullValue:
            case CacheValue.KindOneofCase.None:
                boxed = Value.ForNull();
                break;
            case CacheValue.KindOneofCase.StructValue:
                return wire.StructValue;
            default:
                throw new ArgumentOutOfRangeException(nameof(wire), "Unsupported cache value kind.");
        }

        var envelope = new Struct();
        envelope.Fields.Add("value", boxed);
        return envelope;
    }

    private static T? Coerce<T>(object? value) => value is T result ? result : default;

    private static T? DeserializeFromProtoValue<T>(Value value)
    {
        var buffer = WriteValueToBuffer(value);
        return SerializationProvider.Deserialize<T>(buffer.WrittenSpan);
    }

    private static T? FromStruct<T>(Struct s)
    {
        if (typeof(T) != typeof(object))
        {
            if (s.Fields.Count is not 1 || !s.Fields.TryGetValue("value", out var onlyWrapped))
                return DeserializeFromProtoValue<T>(Value.ForStruct(s));

            return TryReadScalarValue<T>(onlyWrapped, out var scalar) ? scalar : DeserializeFromProtoValue<T>(onlyWrapped);
        }

        if (s.Fields.Count is 1 && s.Fields.TryGetValue("value", out var only))
            return Coerce<T>(ProtoValueToClrScalarOrJson(only));

        var buffer = WriteValueToBuffer(Value.ForStruct(s));
        using var document = JsonDocument.Parse(buffer.WrittenMemory);
        return Coerce<T>(document.RootElement.Clone());
    }

    private static ListValue BuildListValueFromJsonArray(JsonElement arrayElement)
    {
        var list = new ListValue();
        var count = arrayElement.GetArrayLength();
        var destination = list.Values;
        for (var offset = 0; offset < count; offset++)
            destination.Add(ConvertJsonElementToProtoValue(arrayElement[offset]));

        return list;
    }

    private static object? MapCacheValueAsObject(CacheValue value) => value.KindCase switch
    {
        CacheValue.KindOneofCase.StringValue => value.StringValue,
        CacheValue.KindOneofCase.BoolValue => value.BoolValue,
        CacheValue.KindOneofCase.Int32Value => int.CreateChecked(value.Int32Value),
        CacheValue.KindOneofCase.Int64Value => value.Int64Value,
        CacheValue.KindOneofCase.DoubleValue => value.DoubleValue,
        CacheValue.KindOneofCase.NullValue or CacheValue.KindOneofCase.None => null,
        CacheValue.KindOneofCase.StructValue when value.StructValue is { } structValue => FromStruct<object?>(structValue),
        _ => FromStruct<object?>(WrapWireScalarAsStruct(value)),
    };

    private static object? ProtoValueToClrScalarOrJson(Value v)
    {
        switch (v.KindCase)
        {
            case Value.KindOneofCase.StringValue:
                return v.StringValue;

            case Value.KindOneofCase.BoolValue:
                return v.BoolValue;

            case Value.KindOneofCase.NumberValue:
                return v.NumberValue;

            case Value.KindOneofCase.NullValue:
                return null;

            case Value.KindOneofCase.StructValue:
            case Value.KindOneofCase.ListValue:
            {
                var buffer = WriteValueToBuffer(v);
                using var document = JsonDocument.Parse(buffer.WrittenMemory);
                return document.RootElement.Clone();
            }

            case Value.KindOneofCase.None:
            default:
                return null;
        }
    }

    private static TOut CastReference<TOut, TIn>(TIn input)
        where TIn : class?
    {
        // Local copy keeps the cast site distinct from client ProtoEx.ReinterpretReference.
        TIn held = input;
        return Unsafe.As<TIn, TOut>(ref held);
    }

    private static TOut CastScalar<TOut, TIn>(TIn input)
        where TIn : struct
    {
        TIn held = input;
        return Unsafe.As<TIn, TOut>(ref held);
    }

    private static Struct BuildStructFromJsonObject(JsonElement objectElement)
    {
        var result = new Struct();
        var fields = result.Fields;
        using var properties = objectElement.EnumerateObject();
        while (properties.MoveNext())
        {
            var property = properties.Current;
            fields[property.Name] = ConvertJsonElementToProtoValue(property.Value);
        }

        return result;
    }

    private static Struct ToStruct<T>(T? value)
    {
        if (value is null)
            return CreateSingleFieldStruct(Value.ForNull());

        if (value is JsonElement jsonElement)
        {
            return jsonElement.ValueKind is JsonValueKind.Object
                ? BuildStructFromJsonObject(jsonElement)
                : CreateSingleFieldStruct(ConvertJsonElementToProtoValue(jsonElement));
        }

        if (value is string text)
            return CreateSingleFieldStruct(Value.ForString(text));

        if (value is int int32)
            return CreateSingleFieldStruct(Value.ForNumber(int32));

        if (value is long int64)
            return CreateSingleFieldStruct(Value.ForNumber(int64));

        if (value is double floating)
            return CreateSingleFieldStruct(Value.ForNumber(floating));

        if (value is bool flag)
            return CreateSingleFieldStruct(Value.ForBool(flag));

        // SerializeToElement uses the same NodeJsonSerializer options as SerializeToUtf8Bytes but avoids an intermediate UTF-8 byte[].
        var root = SerializationProvider.Instance.SerializeToElement(value);
        return root.ValueKind is JsonValueKind.Object
            ? BuildStructFromJsonObject(root)
            : CreateSingleFieldStruct(ConvertJsonElementToProtoValue(root));
    }

    private static bool TryReadScalarValue<T>(Value value, [MaybeNullWhen(false)] out T result)
    {
        if (typeof(T) == typeof(string) && value.KindCase is Value.KindOneofCase.StringValue)
        {
            result = CastReference<T, string>(value.StringValue);
            return true;
        }

        if (typeof(T) == typeof(bool) && value.KindCase is Value.KindOneofCase.BoolValue)
        {
            result = CastScalar<T, bool>(value.BoolValue);
            return true;
        }

        if (value.KindCase is Value.KindOneofCase.NumberValue && typeof(T) == typeof(double))
        {
            result = CastScalar<T, double>(value.NumberValue);
            return true;
        }

        result = default;
        return false;
    }

    /// <summary>
    /// Maps a <see cref="JsonElement" /> subtree into protobuf well-known <see cref="Value" /> form.
    /// </summary>
    /// <remarks>
    /// JSON strings use <see cref="JsonElement.GetString" /> because protobuf <see cref="Value.ForString" /> only accepts a CLR <see cref="string" /> (decoded UTF-16), not UTF-8 spans.
    /// </remarks>
    /// <param name="element">JSON subtree to convert.</param>
    /// <exception cref="ArgumentOutOfRangeException">Thrown when <paramref name="element" /> has an unsupported <see cref="JsonValueKind" />.</exception>
    private static Value ConvertJsonElementToProtoValue(JsonElement element)
    {
        var kind = element.ValueKind;
        if (kind is JsonValueKind.Object)
            return Value.ForStruct(BuildStructFromJsonObject(element));

        if (kind is JsonValueKind.Array)
            return new Value { ListValue = BuildListValueFromJsonArray(element) };

        if (kind is JsonValueKind.String)
            return Value.ForString(element.GetString());

        if (kind is JsonValueKind.Number)
            return element.TryGetInt64(out var asInt64) ? Value.ForNumber(asInt64) : Value.ForNumber(element.GetDouble());

        if (kind is JsonValueKind.True)
            return Value.ForBool(true);

        if (kind is JsonValueKind.False)
            return Value.ForBool(false);

        if (kind is JsonValueKind.Null or JsonValueKind.Undefined)
            return Value.ForNull();

        throw new ArgumentOutOfRangeException(nameof(element), "Unsupported JSON value kind.");
    }

    private static Struct CreateSingleFieldStruct(Value fieldValue)
    {
        var envelope = new Struct();
        envelope.Fields.Add("value", fieldValue);
        return envelope;
    }

    private static void WriteValue(Utf8JsonWriter writer, Value protoValue)
    {
        var kind = protoValue.KindCase;
        if (kind is Value.KindOneofCase.NullValue or Value.KindOneofCase.None)
        {
            writer.WriteNullValue();
            return;
        }

        if (kind is Value.KindOneofCase.BoolValue)
        {
            writer.WriteBooleanValue(protoValue.BoolValue);
            return;
        }

        if (kind is Value.KindOneofCase.NumberValue)
        {
            writer.WriteNumberValue(protoValue.NumberValue);
            return;
        }

        if (kind is Value.KindOneofCase.StringValue)
        {
            writer.WriteStringValue(protoValue.StringValue);
            return;
        }

        if (kind is Value.KindOneofCase.StructValue)
        {
            writer.WriteStartObject();
            var fields = protoValue.StructValue.Fields;
            using var fieldEnumerator = fields.GetEnumerator();
            for (var index = 0; index < fields.Count; index++)
            {
                _ = fieldEnumerator.MoveNext();
                var field = fieldEnumerator.Current;
                writer.WritePropertyName(field.Key);
                WriteValue(writer, field.Value);
            }

            writer.WriteEndObject();
            return;
        }

        if (kind is Value.KindOneofCase.ListValue)
        {
            writer.WriteStartArray();
            var items = protoValue.ListValue.Values;
            for (var itemIndex = 0; itemIndex < items.Count; itemIndex++)
                WriteValue(writer, items[itemIndex]);

            writer.WriteEndArray();
            return;
        }

        throw new InvalidOperationException($"Unsupported protobuf value kind: {protoValue.KindCase}.");
    }

    private static ArrayBufferWriter<byte> WriteValueToBuffer(Value value)
    {
        var buffer = new ArrayBufferWriter<byte>(256);

        // Sync flush: WriteValue is synchronous; async Utf8JsonWriter disposal would allocate a state machine on every decode.
#pragma warning disable MA0045
        using var writer = new Utf8JsonWriter(buffer);
        WriteValue(writer, value);
        writer.Flush();
#pragma warning restore MA0045

        return buffer;
    }
}
