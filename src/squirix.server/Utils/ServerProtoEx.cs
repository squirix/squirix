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

        if (TryMapTypedPrimitive<T>(value, out var primitive))
            return new ValueTask<T?>(primitive);

        if (value.KindCase is CacheValue.KindOneofCase.NullValue or CacheValue.KindOneofCase.None)
            return new ValueTask<T?>(default(T?));

        if (value.KindCase is CacheValue.KindOneofCase.StructValue && value.StructValue is { } structValue)
            return new ValueTask<T?>(FromStruct<T>(structValue));

        if (IsTypedPrimitiveKind(value.KindCase))
            return new ValueTask<T?>(FromStruct<T>(CacheValueToStruct(value)));

        throw new ArgumentOutOfRangeException(nameof(value), "Unsupported cache value kind.");
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

    private static bool IsTypedPrimitiveKind(CacheValue.KindOneofCase kind) =>
        kind is CacheValue.KindOneofCase.StringValue
            or CacheValue.KindOneofCase.BoolValue
            or CacheValue.KindOneofCase.Int32Value
            or CacheValue.KindOneofCase.Int64Value
            or CacheValue.KindOneofCase.DoubleValue;

    private static bool TryMapTypedPrimitive<T>(CacheValue value, out T? result)
    {
        result = default;
        switch (value.KindCase)
        {
            case CacheValue.KindOneofCase.StringValue when typeof(T) == typeof(string):
                result = ReinterpretReference<T, string>(value.StringValue);
                return true;
            case CacheValue.KindOneofCase.BoolValue when typeof(T) == typeof(bool):
                result = ReinterpretScalar<T, bool>(value.BoolValue);
                return true;
            case CacheValue.KindOneofCase.Int32Value when typeof(T) == typeof(int):
                result = ReinterpretScalar<T, int>(int.CreateChecked(value.Int32Value));
                return true;
            case CacheValue.KindOneofCase.Int64Value when typeof(T) == typeof(long):
                result = ReinterpretScalar<T, long>(value.Int64Value);
                return true;
            case CacheValue.KindOneofCase.DoubleValue when typeof(T) == typeof(double):
                result = ReinterpretScalar<T, double>(value.DoubleValue);
                return true;
            default:
                return false;
        }
    }

    private static Struct CacheValueToStruct(CacheValue value) => value.KindCase switch
    {
        CacheValue.KindOneofCase.StringValue => WrapAsStruct("value", Value.ForString(value.StringValue)),
        CacheValue.KindOneofCase.BoolValue => WrapAsStruct("value", Value.ForBool(value.BoolValue)),
        CacheValue.KindOneofCase.Int32Value => WrapAsStruct("value", Value.ForNumber(value.Int32Value)),
        CacheValue.KindOneofCase.Int64Value => WrapAsStruct("value", Value.ForNumber(value.Int64Value)),
        CacheValue.KindOneofCase.DoubleValue => WrapAsStruct("value", Value.ForNumber(value.DoubleValue)),
        CacheValue.KindOneofCase.NullValue or CacheValue.KindOneofCase.None => WrapAsStruct("value", Value.ForNull()),
        CacheValue.KindOneofCase.StructValue => value.StructValue,
        _ => throw new ArgumentOutOfRangeException(nameof(value), "Unsupported cache value kind."),
    };

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

    private static ListValue ListFromJson(JsonElement el)
    {
        var list = new ListValue();
        var values = list.Values;
        var length = el.GetArrayLength();
        for (var index = 0; index < length; index++)
            values.Add(ValueFromJson(el[index]));

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
        _ => FromStruct<object?>(CacheValueToStruct(value)),
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

    private static TTarget ReinterpretReference<TTarget, TValue>(TValue value)
        where TValue : class?
    {
        var reference = value;
        return Unsafe.As<TValue, TTarget>(ref reference);
    }

    private static TTarget ReinterpretScalar<TTarget, TValue>(TValue value)
        where TValue : struct => Unsafe.As<TValue, TTarget>(ref value);

    private static Struct StructFromJson(JsonElement el)
    {
        var s = new Struct();
        foreach (var p in el.EnumerateObject())
            s.Fields[p.Name] = ValueFromJson(p.Value);
        return s;
    }

    private static Struct ToStruct<T>(T? value)
    {
        switch (value)
        {
            case null:
                return WrapAsStruct("value", Value.ForNull());

            case JsonElement je:
                return je.ValueKind is JsonValueKind.Object ? StructFromJson(je) : WrapAsStruct("value", ValueFromJson(je));

            case string text:
                return WrapAsStruct("value", Value.ForString(text));

            case int number:
                return WrapAsStruct("value", Value.ForNumber(number));

            case long number:
                return WrapAsStruct("value", Value.ForNumber(number));

            case double number:
                return WrapAsStruct("value", Value.ForNumber(number));

            case bool boolean:
                return WrapAsStruct("value", Value.ForBool(boolean));
        }

        // SerializeToElement uses the same NodeJsonSerializer options as SerializeToUtf8Bytes but avoids an intermediate UTF-8 byte[].
        var root = SerializationProvider.Instance.SerializeToElement(value);
        return root.ValueKind is JsonValueKind.Object ? StructFromJson(root) : WrapAsStruct("value", ValueFromJson(root));
    }

    private static bool TryReadScalarValue<T>(Value value, [MaybeNullWhen(false)] out T result)
    {
        if (typeof(T) == typeof(string) && value.KindCase is Value.KindOneofCase.StringValue)
        {
            result = ReinterpretReference<T, string>(value.StringValue);
            return true;
        }

        if (typeof(T) == typeof(bool) && value.KindCase is Value.KindOneofCase.BoolValue)
        {
            result = ReinterpretScalar<T, bool>(value.BoolValue);
            return true;
        }

        if (value.KindCase is Value.KindOneofCase.NumberValue && typeof(T) == typeof(double))
        {
            result = ReinterpretScalar<T, double>(value.NumberValue);
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
    /// <param name="el">JSON subtree to convert.</param>
    /// <exception cref="ArgumentOutOfRangeException">Thrown when <paramref name="el" /> has an unsupported <see cref="JsonValueKind" />.</exception>
    private static Value ValueFromJson(JsonElement el)
    {
        return el.ValueKind switch
        {
            JsonValueKind.Object => Value.ForStruct(StructFromJson(el)),
            JsonValueKind.Array => new Value { ListValue = ListFromJson(el) },
            JsonValueKind.String => Value.ForString(el.GetString()),
            JsonValueKind.Number => el.TryGetInt64(out var value) ? Value.ForNumber(value) : Value.ForNumber(el.GetDouble()),
            JsonValueKind.True => Value.ForBool(true),
            JsonValueKind.False => Value.ForBool(false),
            JsonValueKind.Null => Value.ForNull(),
            JsonValueKind.Undefined => Value.ForNull(),
            _ => throw new ArgumentOutOfRangeException(nameof(el), "Unsupported JSON value kind."),
        };
    }

    private static Struct WrapAsStruct(string fieldName, Value v)
    {
        return new Struct
        {
            Fields =
            {
                [fieldName] = v,
            },
        };
    }

    private static void WriteValue(Utf8JsonWriter w, Value v)
    {
        switch (v.KindCase)
        {
            case Value.KindOneofCase.NullValue:
            case Value.KindOneofCase.None:
                w.WriteNullValue();
                break;

            case Value.KindOneofCase.BoolValue:
                w.WriteBooleanValue(v.BoolValue);
                break;

            case Value.KindOneofCase.NumberValue:
                w.WriteNumberValue(v.NumberValue);
                break;

            case Value.KindOneofCase.StringValue:
                w.WriteStringValue(v.StringValue);
                break;

            case Value.KindOneofCase.StructValue:
            {
                w.WriteStartObject();
                var fields = v.StructValue.Fields;
                using var fieldEnumerator = fields.GetEnumerator();
                for (var index = 0; index < fields.Count; index++)
                {
                    _ = fieldEnumerator.MoveNext();
                    var kv = fieldEnumerator.Current;
                    w.WritePropertyName(kv.Key);
                    WriteValue(w, kv.Value);
                }

                w.WriteEndObject();
                break;
            }

            case Value.KindOneofCase.ListValue:
                w.WriteStartArray();
                for (var index = 0; index < v.ListValue.Values.Count; index++)
                    WriteValue(w, v.ListValue.Values[index]);

                w.WriteEndArray();
                break;

            default:
                throw new InvalidOperationException($"Unsupported protobuf value kind: {v.KindCase}.");
        }
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
