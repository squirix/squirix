using System;
using System.Buffers;
using System.Runtime.CompilerServices;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Google.Protobuf.WellKnownTypes;
using Squirix.Transport.Grpc.Cache;

namespace Squirix.Internal;

/// <summary>
/// Maps CLR and JSON values into protobuf <see cref="Struct" /> payloads for cache entries.
/// </summary>
internal static class ProtoEx
{
    internal static async ValueTask<T?> FromCacheValueAsync<T>(CacheValue value, ISquirixSerializer serializer)
    {
        ArgumentNullException.ThrowIfNull(value);
        ArgumentNullException.ThrowIfNull(serializer);

        if (typeof(T) == typeof(object))
            return Coerce<T>(await FromCacheValueAsObjectAsync(value, serializer).ConfigureAwait(false));

        switch (value.KindCase)
        {
            case CacheValue.KindOneofCase.StringValue:
                if (typeof(T) == typeof(string))
                    return ReinterpretReference<T, string>(value.StringValue);
                break;

            case CacheValue.KindOneofCase.BoolValue:
                if (typeof(T) == typeof(bool))
                    return ReinterpretScalar<T, bool>(value.BoolValue);
                break;

            case CacheValue.KindOneofCase.Int32Value:
                if (typeof(T) == typeof(int))
                    return ReinterpretScalar<T, int>(int.CreateChecked(value.Int32Value));
                break;

            case CacheValue.KindOneofCase.Int64Value:
                if (typeof(T) == typeof(long))
                    return ReinterpretScalar<T, long>(value.Int64Value);
                break;

            case CacheValue.KindOneofCase.DoubleValue:
                if (typeof(T) == typeof(double))
                    return ReinterpretScalar<T, double>(value.DoubleValue);
                break;

            case CacheValue.KindOneofCase.NullValue:
            case CacheValue.KindOneofCase.None:
                return default;

            case CacheValue.KindOneofCase.StructValue when value.StructValue is { } structValue:
                return await FromStructAsync<T>(structValue, serializer).ConfigureAwait(false);

            default:
                throw new ArgumentOutOfRangeException(nameof(value), "Unsupported cache value kind.");
        }

        return await FromStructAsync<T>(ToStructValueWrapper(value), serializer).ConfigureAwait(false);
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

    internal static async ValueTask<CacheEntry<T>> MapProtoEntryToCacheEntryAsync<T>(CacheEntryWire entry, ISquirixSerializer serializer)
    {
        ArgumentNullException.ThrowIfNull(serializer);
        return new CacheEntry<T>
        {
            Value = await FromStructAsync<T>(entry.Value, serializer).ConfigureAwait(false),
            ExpiresUtc = entry.ExpiresUtc?.ToDateTime().ToUniversalTime(),
            Expiration = entry.Expiration?.ToTimeSpan(),
        };
    }

    private static T? Coerce<T>(object? value) => value is T result ? result : default;

    private static async ValueTask<T?> DeserializeAsync<T>(Value value, ISquirixSerializer serializer)
    {
        var buffer = new ArrayBufferWriter<byte>();
        var writer = new Utf8JsonWriter(buffer);
        await using (writer.ConfigureAwait(false))
        {
            WriteValue(writer, value);
            await writer.FlushAsync(CancellationToken.None).ConfigureAwait(false);
        }

        return serializer.Deserialize<T>(buffer.WrittenSpan);
    }

    private static async ValueTask<object?> FromCacheValueAsObjectAsync(CacheValue value, ISquirixSerializer serializer)
    {
        return value.KindCase switch
        {
            CacheValue.KindOneofCase.StringValue => value.StringValue,
            CacheValue.KindOneofCase.BoolValue => value.BoolValue,
            CacheValue.KindOneofCase.Int32Value => int.CreateChecked(value.Int32Value),
            CacheValue.KindOneofCase.Int64Value => value.Int64Value,
            CacheValue.KindOneofCase.DoubleValue => value.DoubleValue,
            CacheValue.KindOneofCase.NullValue or CacheValue.KindOneofCase.None => null,
            CacheValue.KindOneofCase.StructValue when value.StructValue is { } structValue => await FromStructAsync<object?>(structValue, serializer).ConfigureAwait(false),
            _ => throw new ArgumentOutOfRangeException(nameof(value), "Unsupported cache value kind."),
        };
    }

    private static ValueTask<T?> FromStructAsync<T>(Struct value, ISquirixSerializer serializer)
    {
        ArgumentNullException.ThrowIfNull(value);
        ArgumentNullException.ThrowIfNull(serializer);

        if (value.Fields.Count is 1 && value.Fields.TryGetValue("value", out var wrapped))
            return FromValueAsync<T>(wrapped, serializer);

        return DeserializeAsync<T>(Value.ForStruct(value), serializer);
    }

    private static async ValueTask<T?> FromValueAsync<T>(Value value, ISquirixSerializer serializer)
    {
        if (typeof(T) == typeof(object))
            return Coerce<T>(await ToUntypedValueAsync(value, serializer).ConfigureAwait(false));

        return await DeserializeAsync<T>(value, serializer).ConfigureAwait(false);
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

            default:
                var root = serializer.SerializeToElement(value);
                return root.ValueKind is JsonValueKind.Object ? StructFromJson(root) : WrapAsStruct("value", ValueFromJson(root));
        }
    }

    private static Struct ToStructValueWrapper(CacheValue value) => value.KindCase switch
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

    private static async ValueTask<object?> ToUntypedValueAsync(Value value, ISquirixSerializer serializer)
    {
        switch (value.KindCase)
        {
            case Value.KindOneofCase.StringValue:
                return value.StringValue;
            case Value.KindOneofCase.BoolValue:
                return value.BoolValue;
            case Value.KindOneofCase.NumberValue:
                return NormalizeNumber(value.NumberValue);
            case Value.KindOneofCase.NullValue:
                return null;
            case Value.KindOneofCase.StructValue:
            case Value.KindOneofCase.ListValue:
                return await DeserializeAsync<JsonElement>(value, serializer).ConfigureAwait(false);
            case Value.KindOneofCase.None:
            default:
                return null;
        }
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
