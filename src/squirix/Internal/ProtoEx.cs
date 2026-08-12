using System;
using System.Buffers;
using System.Globalization;
using System.Runtime.CompilerServices;
using System.Text.Json;
using System.Threading.Tasks;
using Google.Protobuf.WellKnownTypes;
using Squirix.Transport.Grpc.Cache;

namespace Squirix.Internal;

/// <summary>
/// Maps CLR and JSON values into protobuf <see cref="Struct" /> payloads for cache entries.
/// </summary>
internal static class ProtoEx
{
    private const string ScalarEnvelopeKey = "\0squirix:scalar";
    private const string NumberEnvelopeInt64Key = "\0squirix:int64";
    private const string NumberEnvelopeDecimalKey = "\0squirix:decimal";

    internal static ValueTask<T?> FromCacheValueAsync<T>(CacheValue value, ISquirixSerializer serializer)
    {
        ArgumentNullException.ThrowIfNull(value);
        ArgumentNullException.ThrowIfNull(serializer);

        if (typeof(T) == typeof(object))
            return new ValueTask<T?>(Coerce<T>(FromCacheValueAsObject(value, serializer)));

        if (TryMapTypedPrimitive<T>(value, out var primitive))
            return new ValueTask<T?>(primitive);

        if (value.KindCase is CacheValue.KindOneofCase.NullValue or CacheValue.KindOneofCase.None)
            return new ValueTask<T?>(default(T?));

        if (value.KindCase is CacheValue.KindOneofCase.StructValue && value.StructValue is { } structValue)
            return new ValueTask<T?>(FromStruct<T>(structValue, serializer));

        if (IsTypedPrimitiveKind(value.KindCase))
            return new ValueTask<T?>(FromStruct<T>(ToStructValueWrapper(value), serializer));

        throw new ArgumentOutOfRangeException(nameof(value), "Unsupported cache value kind.");
    }

    internal static CacheEntryWire MapEntryToProto<T>(CacheEntry<T> entry, ISquirixSerializer serializer)
    {
        ArgumentNullException.ThrowIfNull(entry);
        ArgumentNullException.ThrowIfNull(serializer);

        return new CacheEntryWire
        {
            Value = ToStruct(entry.Value, serializer),
            ExpiresUtc = entry.ExpiresUtc is null ? null : Timestamp.FromDateTime(DateTime.SpecifyKind(entry.ExpiresUtc.Value, DateTimeKind.Utc)),
            Expiration = entry.Expiration is null ? null : Duration.FromTimeSpan(entry.Expiration.Value),
        };
    }

    internal static ValueTask<CacheEntry<T>> MapProtoEntryToCacheEntryAsync<T>(CacheEntryWire entry, ISquirixSerializer serializer)
    {
        ArgumentNullException.ThrowIfNull(serializer);
        return new ValueTask<CacheEntry<T>>(
            new CacheEntry<T>
            {
                Value = FromStruct<T>(entry.Value, serializer),
                ExpiresUtc = entry.ExpiresUtc?.ToDateTime().ToUniversalTime(),
                Expiration = entry.Expiration?.ToTimeSpan(),
            });
    }

    private static bool IsTypedPrimitiveKind(CacheValue.KindOneofCase kind) => kind is CacheValue.KindOneofCase.StringValue or CacheValue.KindOneofCase.BoolValue
        or CacheValue.KindOneofCase.Int32Value or CacheValue.KindOneofCase.Int64Value or CacheValue.KindOneofCase.DoubleValue;

    private static bool TryMapTypedPrimitive<T>(CacheValue value, out T? result)
    {
        result = default;
        return value.KindCase switch
        {
            CacheValue.KindOneofCase.StringValue => TryMapString(value, out result),
            CacheValue.KindOneofCase.BoolValue => TryMapBool(value, out result),
            CacheValue.KindOneofCase.Int32Value => TryMapInt32(value, out result),
            CacheValue.KindOneofCase.Int64Value => TryMapInt64(value, out result),
            CacheValue.KindOneofCase.DoubleValue => TryMapDouble(value, out result),
            _ => false,
        };
    }

    private static bool TryMapBool<T>(CacheValue value, out T? result)
    {
        if (typeof(T) == typeof(bool))
        {
            result = ReinterpretScalar<T, bool>(value.BoolValue);
            return true;
        }

        result = default;
        return false;
    }

    private static bool TryMapDouble<T>(CacheValue value, out T? result)
    {
        if (typeof(T) == typeof(double))
        {
            result = ReinterpretScalar<T, double>(value.DoubleValue);
            return true;
        }

        result = default;
        return false;
    }

    private static bool TryMapInt32<T>(CacheValue value, out T? result)
    {
        if (typeof(T) == typeof(int))
        {
            result = ReinterpretScalar<T, int>(int.CreateChecked(value.Int32Value));
            return true;
        }

        result = default;
        return false;
    }

    private static bool TryMapInt64<T>(CacheValue value, out T? result)
    {
        if (typeof(T) == typeof(long))
        {
            result = ReinterpretScalar<T, long>(value.Int64Value);
            return true;
        }

        result = default;
        return false;
    }

    private static bool TryMapString<T>(CacheValue value, out T? result)
    {
        if (typeof(T) == typeof(string))
        {
            result = ReinterpretReference<T, string>(value.StringValue);
            return true;
        }

        result = default;
        return false;
    }

    private static T? Coerce<T>(object? value) => value is T result ? result : default;

    private static T? Deserialize<T>(Value value, ISquirixSerializer serializer)
    {
        var buffer = new ArrayBufferWriter<byte>(256);

        // Sync flush: WriteValue is synchronous; async Utf8JsonWriter disposal would allocate a state machine on every decode.
#pragma warning disable MA0045
        using (var writer = new Utf8JsonWriter(buffer))
        {
            WriteValue(writer, value);
            writer.Flush();
        }
#pragma warning restore MA0045

        return serializer.Deserialize<T>(buffer.WrittenSpan);
    }

    private static object? FromCacheValueAsObject(CacheValue value, ISquirixSerializer serializer) => value.KindCase switch
    {
        CacheValue.KindOneofCase.StringValue => value.StringValue,
        CacheValue.KindOneofCase.BoolValue => value.BoolValue,
        CacheValue.KindOneofCase.Int32Value => int.CreateChecked(value.Int32Value),
        CacheValue.KindOneofCase.Int64Value => value.Int64Value,
        CacheValue.KindOneofCase.DoubleValue => value.DoubleValue,
        CacheValue.KindOneofCase.NullValue or CacheValue.KindOneofCase.None => null,
        CacheValue.KindOneofCase.StructValue when value.StructValue is { } structValue => FromStruct<object?>(structValue, serializer),
        _ => throw new ArgumentOutOfRangeException(nameof(value), "Unsupported cache value kind."),
    };

    private static T? FromStruct<T>(Struct value, ISquirixSerializer serializer)
    {
        ArgumentNullException.ThrowIfNull(value);
        ArgumentNullException.ThrowIfNull(serializer);

        if (value.Fields.Count is 1 && value.Fields.TryGetValue(ScalarEnvelopeKey, out var wrapped))
            return FromValue<T>(wrapped, serializer);

        return Deserialize<T>(Value.ForStruct(value), serializer);
    }

    private static T? FromValue<T>(Value value, ISquirixSerializer serializer)
    {
        if (typeof(T) == typeof(object))
            return Coerce<T>(ToUntypedValue(value, serializer));

        return Deserialize<T>(value, serializer);
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

    private static double NormalizeNumber(double value) => value;

    private static Value ConvertNumberToProtoValue(JsonElement element)
    {
        if (element.TryGetInt64(out var int64))
            return CreateNumberEnvelope(NumberEnvelopeInt64Key, int64.ToString(CultureInfo.InvariantCulture));

        if (element.TryGetDecimal(out var dec))
            return CreateNumberEnvelope(NumberEnvelopeDecimalKey, dec.ToString(CultureInfo.InvariantCulture));

        return Value.ForNumber(element.GetDouble());
    }

    private static Value CreateNumberEnvelope(string markerKey, string numberText)
    {
        var s = new Struct();
        s.Fields.Add(markerKey, Value.ForString(numberText));
        return Value.ForStruct(s);
    }

    private static bool TryWriteNumberEnvelope(Utf8JsonWriter writer, Struct s)
    {
        if (s.Fields.Count is not 1)
            return false;

        if (s.Fields.TryGetValue(NumberEnvelopeInt64Key, out var longField) && longField.KindCase is Value.KindOneofCase.StringValue && long.TryParse(
                longField.StringValue,
                NumberStyles.Integer,
                CultureInfo.InvariantCulture,
                out var longValue))
        {
            writer.WriteNumberValue(longValue);
            return true;
        }

        if (!s.Fields.TryGetValue(NumberEnvelopeDecimalKey, out var decimalField) || decimalField.KindCase is not Value.KindOneofCase.StringValue || !decimal.TryParse(
                decimalField.StringValue,
                NumberStyles.Number,
                CultureInfo.InvariantCulture,
                out var decimalValue))
            return false;
        writer.WriteNumberValue(decimalValue);
        return true;
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

    private static Struct ToStruct<T>(T? value, ISquirixSerializer serializer)
    {
        switch (value)
        {
            case null:
                return WrapAsStruct(ScalarEnvelopeKey, Value.ForNull());

            case JsonElement je:
                return je.ValueKind is JsonValueKind.Object ? StructFromJson(je) : WrapAsStruct(ScalarEnvelopeKey, ValueFromJson(je));

            case string text:
                return WrapAsStruct(ScalarEnvelopeKey, Value.ForString(text));

            case int number:
                return WrapAsStruct(ScalarEnvelopeKey, Value.ForNumber(number));

            case long number:
                return WrapAsStruct(ScalarEnvelopeKey, CreateNumberEnvelope(NumberEnvelopeInt64Key, number.ToString(CultureInfo.InvariantCulture)));

            case double number:
                return WrapAsStruct(ScalarEnvelopeKey, Value.ForNumber(number));

            case bool boolean:
                return WrapAsStruct(ScalarEnvelopeKey, Value.ForBool(boolean));

            case decimal dec:
                return WrapAsStruct(ScalarEnvelopeKey, CreateNumberEnvelope(NumberEnvelopeDecimalKey, dec.ToString(CultureInfo.InvariantCulture)));

            default:
                var root = serializer.SerializeToElement(value);
                return root.ValueKind is JsonValueKind.Object ? StructFromJson(root) : WrapAsStruct(ScalarEnvelopeKey, ValueFromJson(root));
        }
    }

    private static Struct ToStructValueWrapper(CacheValue value) => value.KindCase switch
    {
        CacheValue.KindOneofCase.StringValue => WrapAsStruct(ScalarEnvelopeKey, Value.ForString(value.StringValue)),
        CacheValue.KindOneofCase.BoolValue => WrapAsStruct(ScalarEnvelopeKey, Value.ForBool(value.BoolValue)),
        CacheValue.KindOneofCase.Int32Value => WrapAsStruct(ScalarEnvelopeKey, Value.ForNumber(value.Int32Value)),
        CacheValue.KindOneofCase.Int64Value => WrapAsStruct(
            ScalarEnvelopeKey,
            CreateNumberEnvelope(NumberEnvelopeInt64Key, value.Int64Value.ToString(CultureInfo.InvariantCulture))),
        CacheValue.KindOneofCase.DoubleValue => WrapAsStruct(ScalarEnvelopeKey, Value.ForNumber(value.DoubleValue)),
        CacheValue.KindOneofCase.NullValue or CacheValue.KindOneofCase.None => WrapAsStruct(ScalarEnvelopeKey, Value.ForNull()),
        CacheValue.KindOneofCase.StructValue => value.StructValue,
        _ => throw new ArgumentOutOfRangeException(nameof(value), "Unsupported cache value kind."),
    };

    private static object? ToUntypedValue(Value value, ISquirixSerializer serializer) => value.KindCase switch
    {
        Value.KindOneofCase.StringValue => value.StringValue,
        Value.KindOneofCase.BoolValue => value.BoolValue,
        Value.KindOneofCase.NumberValue => NormalizeNumber(value.NumberValue),
        Value.KindOneofCase.NullValue or Value.KindOneofCase.None => null,
        Value.KindOneofCase.StructValue or Value.KindOneofCase.ListValue => Deserialize<JsonElement>(value, serializer),
        _ => throw new ArgumentOutOfRangeException(nameof(value), value.KindCase, "Unsupported value kind."),
    };

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
            JsonValueKind.Number => ConvertNumberToProtoValue(el),
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

    private static void WriteValue(Utf8JsonWriter writer, Value value)
    {
        // Emit protobuf Value trees as JSON so ISquirixSerializer can deserialize complex cache payloads.
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
            {
                if (TryWriteNumberEnvelope(writer, value.StructValue))
                    return;

                writer.WriteStartObject();
                var fields = value.StructValue.Fields;

                // Index-based loop avoids foreach enumerator allocations while writing nested structs.
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

            case Value.KindOneofCase.ListValue:
                writer.WriteStartArray();
                var values = value.ListValue.Values;

                // Lists recurse through WriteValue so mixed scalar and structured elements round-trip.
                for (var index = 0; index < values.Count; index++)
                    WriteValue(writer, values[index]);

                writer.WriteEndArray();
                return;
            default:
                throw new ArgumentOutOfRangeException(nameof(value), "Unsupported protobuf value kind.");
        }
    }
}
