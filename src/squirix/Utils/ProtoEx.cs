using System;
using System.Buffers;
using System.Text.Json;
using Google.Protobuf.WellKnownTypes;
using Squirix.Serialization;
using Squirix.Transport.Grpc.Cache;

namespace Squirix.Utils;

/// <summary>
/// Maps CLR and JSON values into protobuf <see cref="Struct" /> payloads for cache entries.
/// </summary>
internal static class ProtoEx
{
    internal static T? FromCacheValue<T>(CacheValue value, ISquirixSerializer serializer)
    {
        ArgumentNullException.ThrowIfNull(value);
        ArgumentNullException.ThrowIfNull(serializer);

        if (typeof(T) == typeof(object))
            return (T?)FromCacheValueAsObject(value, serializer);

        switch (value.KindCase)
        {
            case CacheValue.KindOneofCase.StringValue:
                if (typeof(T) == typeof(string))
                    return (T)(object)value.StringValue;
                break;

            case CacheValue.KindOneofCase.BoolValue:
                if (typeof(T) == typeof(bool))
                    return (T)(object)value.BoolValue;
                break;

            case CacheValue.KindOneofCase.Int64Value:
                if (typeof(T) == typeof(long))
                    return (T)(object)value.Int64Value;
                if (typeof(T) == typeof(int) && value.Int64Value is >= int.MinValue and <= int.MaxValue)
                    return (T)(object)(int)value.Int64Value;
                break;

            case CacheValue.KindOneofCase.DoubleValue:
                if (typeof(T) == typeof(double))
                    return (T)(object)value.DoubleValue;
                break;

            case CacheValue.KindOneofCase.NullValue:
            case CacheValue.KindOneofCase.None:
                return default;

            case CacheValue.KindOneofCase.StructValue when value.StructValue is { } structValue:
                return FromStruct<T>(structValue, serializer);

            default:
                throw new ArgumentOutOfRangeException(nameof(value), value.KindCase, "Unsupported cache value kind.");
        }

        return FromStruct<T>(ToStructValueWrapper(value), serializer);
    }

    internal static T? FromStruct<T>(Struct value, ISquirixSerializer serializer)
    {
        ArgumentNullException.ThrowIfNull(value);
        ArgumentNullException.ThrowIfNull(serializer);

        if (value.Fields.Count == 1 && value.Fields.TryGetValue("value", out var wrapped))
            return FromValue<T>(wrapped, serializer);

        return Deserialize<T>(Value.ForStruct(value), serializer);
    }

    internal static CacheValue ToCacheValue<T>(T? value, ISquirixSerializer serializer)
    {
        ArgumentNullException.ThrowIfNull(serializer);

        return value switch
        {
            null => new CacheValue { NullValue = NullValue.NullValue },
            string text => new CacheValue { StringValue = text },
            int number => new CacheValue { Int64Value = number },
            long number => new CacheValue { Int64Value = number },
            double number => new CacheValue { DoubleValue = number },
            bool boolean => new CacheValue { BoolValue = boolean },
            _ => new CacheValue { StructValue = ToStruct(value, serializer) },
        };
    }

    private static T? Deserialize<T>(Value value, ISquirixSerializer serializer)
    {
        var buffer = new ArrayBufferWriter<byte>();
        using (var writer = new Utf8JsonWriter(buffer))
            WriteValue(writer, value);

        return serializer.Deserialize<T>(buffer.WrittenSpan);
    }

    private static object? FromCacheValueAsObject(CacheValue value, ISquirixSerializer serializer) => value.KindCase switch
    {
        CacheValue.KindOneofCase.StringValue => value.StringValue,
        CacheValue.KindOneofCase.BoolValue => value.BoolValue,
        CacheValue.KindOneofCase.Int64Value => value.Int64Value is >= int.MinValue and <= int.MaxValue ? (int)value.Int64Value : value.Int64Value,
        CacheValue.KindOneofCase.DoubleValue => value.DoubleValue,
        CacheValue.KindOneofCase.NullValue or CacheValue.KindOneofCase.None => null,
        CacheValue.KindOneofCase.StructValue when value.StructValue is { } structValue => FromStruct<object?>(structValue, serializer),
        _ => throw new ArgumentOutOfRangeException(nameof(value), value.KindCase, "Unsupported cache value kind."),
    };

    private static T? FromValue<T>(Value value, ISquirixSerializer serializer)
    {
        if (typeof(T) == typeof(object))
            return (T?)ToUntypedValue(value, serializer);

        return Deserialize<T>(value, serializer);
    }

    private static ListValue ListFromJson(JsonElement el)
    {
        var list = new ListValue();
        foreach (var item in el.EnumerateArray())
            list.Values.Add(ValueFromJson(item));

        return list;
    }

    private static double NormalizeNumber(double value) => value;

    private static Struct StructFromJson(JsonElement el)
    {
        var s = new Struct();
        foreach (var p in el.EnumerateObject())
            s.Fields[p.Name] = ValueFromJson(p.Value);

        return s;
    }

    private static Struct ToStruct<T>(T? value, ISquirixSerializer serializer)
    {
        switch (value)
        {
            case null:
                return WrapAsStruct("value", Value.ForNull());

            case JsonElement je:
                return je.ValueKind == JsonValueKind.Object ? StructFromJson(je) : WrapAsStruct("value", ValueFromJson(je));

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

            default:
                var root = serializer.SerializeToElement(value);
                return root.ValueKind == JsonValueKind.Object ? StructFromJson(root) : WrapAsStruct("value", ValueFromJson(root));
        }
    }

    private static Struct ToStructValueWrapper(CacheValue value) => value.KindCase switch
    {
        CacheValue.KindOneofCase.StringValue => WrapAsStruct("value", Value.ForString(value.StringValue)),
        CacheValue.KindOneofCase.BoolValue => WrapAsStruct("value", Value.ForBool(value.BoolValue)),
        CacheValue.KindOneofCase.Int64Value => WrapAsStruct("value", Value.ForNumber(value.Int64Value)),
        CacheValue.KindOneofCase.DoubleValue => WrapAsStruct("value", Value.ForNumber(value.DoubleValue)),
        CacheValue.KindOneofCase.NullValue or CacheValue.KindOneofCase.None => WrapAsStruct("value", Value.ForNull()),
        CacheValue.KindOneofCase.StructValue => value.StructValue,
        _ => throw new ArgumentOutOfRangeException(nameof(value), value.KindCase, "Unsupported cache value kind."),
    };

    private static object? ToUntypedValue(Value value, ISquirixSerializer serializer) => value.KindCase switch
    {
        Value.KindOneofCase.StringValue => value.StringValue,
        Value.KindOneofCase.BoolValue => value.BoolValue,
        Value.KindOneofCase.NumberValue => NormalizeNumber(value.NumberValue),
        Value.KindOneofCase.NullValue => null,
        Value.KindOneofCase.StructValue or Value.KindOneofCase.ListValue => Deserialize<JsonElement>(value, serializer),
        _ => null,
    };

    /// <summary>
    /// Maps a <see cref="JsonElement" /> subtree into protobuf well-known <see cref="Value" /> form.
    /// </summary>
    /// <remarks>
    /// JSON strings use <see cref="JsonElement.GetString" /> because protobuf <see cref="Value.ForString" /> only accepts a CLR <see cref="string" /> (decoded UTF-16), not UTF-8 spans.
    /// </remarks>
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
            _ => throw new ArgumentOutOfRangeException(nameof(el), el.ValueKind, "Unsupported JSON value kind."),
        };
    }

    private static Struct WrapAsStruct(string fieldName, Value v)
    {
        var s = new Struct
        {
            Fields =
            {
                [fieldName] = v,
            },
        };
        return s;
    }

    private static void WriteValue(Utf8JsonWriter writer, Value value)
    {
        switch (value.KindCase)
        {
            case Value.KindOneofCase.NullValue:
            case Value.KindOneofCase.None:
                writer.WriteNullValue();
                return;
            case Value.KindOneofCase.NumberValue:
                writer.WriteNumberValue(value.NumberValue);
                return;
            case Value.KindOneofCase.StringValue:
                writer.WriteStringValue(value.StringValue);
                return;
            case Value.KindOneofCase.BoolValue:
                writer.WriteBooleanValue(value.BoolValue);
                return;
            case Value.KindOneofCase.StructValue:
                writer.WriteStartObject();
                foreach (var field in value.StructValue.Fields)
                {
                    writer.WritePropertyName(field.Key);
                    WriteValue(writer, field.Value);
                }

                writer.WriteEndObject();
                return;
            case Value.KindOneofCase.ListValue:
                writer.WriteStartArray();
                foreach (var item in value.ListValue.Values)
                    WriteValue(writer, item);

                writer.WriteEndArray();
                return;
            default:
                throw new ArgumentOutOfRangeException(nameof(value), value.KindCase, "Unsupported protobuf value kind.");
        }
    }
}
