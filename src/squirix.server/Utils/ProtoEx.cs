using System;
using System.Buffers;
using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;
using System.Text.Json;
using Google.Protobuf.WellKnownTypes;
using Squirix.Server.Serialization;
using Squirix.Server.Storage;
using Squirix.Transport.Grpc.Cache;
using RpcEntry = Squirix.Transport.Grpc.Cache.Entry;

namespace Squirix.Server.Utils;

internal static class ProtoEx
{
    public static CacheEntry<T> MapFromProto<T>(this RpcEntry e)
    {
        var value = FromStruct<T>(e.Value);
        DateTime? expires = null;
        if (e.ExpiresUtc is not null && (e.ExpiresUtc.Seconds != 0 || e.ExpiresUtc.Nanos != 0))
            expires = e.ExpiresUtc.ToDateTime().ToUniversalTime();

        if (typeof(T) == typeof(object))
            value = (T)NormalizeUntypedScalarForUntypedCache(value!)!;

        return new CacheEntry<T>
        {
            Value = value,
            ExpiresUtc = expires,
            Expiration = e.Expiration?.ToTimeSpan(),
        };
    }

    public static RpcEntry MapToProto<T>(this CacheEntry<T> e) => new()
    {
        Value = ToStruct(e.Value),
        ExpiresUtc = e.ExpiresUtc is null ? null : Timestamp.FromDateTime(DateTime.SpecifyKind(e.ExpiresUtc.Value, DateTimeKind.Utc)),
        Expiration = e.Expiration is null ? null : Duration.FromTimeSpan(e.Expiration.Value),
    };

    /// <summary>
    /// Maps a cache value to protobuf <c>Struct</c> wire form (single-field wrapper or JSON-derived struct).
    /// </summary>
    /// <typeparam name="T">Logical cache value type.</typeparam>
    /// <param name="value">Value to encode.</param>
    /// <returns>Protobuf struct suitable for well-known <c>Value</c> payloads.</returns>
    internal static Struct CacheValueToGrpcStruct<T>(T? value) => ToStruct(value);

    /// <summary>
    /// Maps a cache value to the compact value-only gRPC wire form.
    /// </summary>
    /// <typeparam name="T">Logical cache value type.</typeparam>
    /// <param name="value">Value to encode.</param>
    /// <returns>Compact protobuf value suitable for the value-only read path.</returns>
    internal static CacheValue CacheValueToGrpcValue<T>(T? value)
    {
        return value switch
        {
            null => new CacheValue { NullValue = NullValue.NullValue },
            string text => new CacheValue { StringValue = text },
            int number => new CacheValue { Int64Value = number },
            long number => new CacheValue { Int64Value = number },
            double number => new CacheValue { DoubleValue = number },
            bool boolean => new CacheValue { BoolValue = boolean },
            _ => new CacheValue { StructValue = ToStruct(value) },
        };
    }

    internal static CacheEntry<T> CacheValueFromGrpcValue<T>(CacheValue value, Timestamp? expiresUtc, Duration? expiration)
    {
        ArgumentNullException.ThrowIfNull(value);

        var mapped = value.KindCase switch
        {
            CacheValue.KindOneofCase.StringValue when typeof(T) == typeof(string) => (T?)(object)value.StringValue,
            CacheValue.KindOneofCase.BoolValue when typeof(T) == typeof(bool) => (T?)(object)value.BoolValue,
            CacheValue.KindOneofCase.Int64Value when typeof(T) == typeof(long) => (T?)(object)value.Int64Value,
            CacheValue.KindOneofCase.Int64Value when typeof(T) == typeof(int) && value.Int64Value is >= int.MinValue and <= int.MaxValue => (T?)(object)(int)value.Int64Value,
            CacheValue.KindOneofCase.DoubleValue when typeof(T) == typeof(double) => (T?)(object)value.DoubleValue,
            CacheValue.KindOneofCase.NullValue or CacheValue.KindOneofCase.None => default,
            CacheValue.KindOneofCase.StructValue => FromStruct<T>(value.StructValue),
            _ => FromStruct<T>(CacheValueToStruct(value)),
        };

        if (typeof(T) == typeof(object))
            mapped = (T?)NormalizeUntypedScalarForUntypedCache(mapped);

        return new CacheEntry<T>
        {
            Value = mapped,
            ExpiresUtc = expiresUtc?.ToDateTime().ToUniversalTime(),
            Expiration = expiration?.ToTimeSpan(),
        };
    }

    private static T? DeserializeFromProtoValue<T>(Value value)
    {
        var buffer = new ArrayBufferWriter<byte>();
        using (var writer = new Utf8JsonWriter(buffer))
            WriteValue(writer, value);

        return SerializationProvider.Deserialize<T>(buffer.WrittenSpan);
    }

    private static T? FromStruct<T>(Struct s)
    {
        if (typeof(T) != typeof(object))
        {
            return s.Fields.Count == 1 && s.Fields.TryGetValue("value", out var onlyWrapped)
                ? TryReadScalarValue<T>(onlyWrapped, out var scalar) ? scalar : DeserializeFromProtoValue<T>(onlyWrapped)
                : DeserializeFromProtoValue<T>(Value.ForStruct(s));
        }

        if (s.Fields.Count == 1 && s.Fields.TryGetValue("value", out var only))
        {
            var obj = ProtoValueToClrScalarOrJson(only);
            return (T?)obj;
        }

        var buffer = new ArrayBufferWriter<byte>();
        using (var writer = new Utf8JsonWriter(buffer))
            WriteValue(writer, Value.ForStruct(s));
        return (T?)(object)new StoredJsonPayload(buffer.WrittenSpan);
    }

    private static ListValue ListFromJson(JsonElement el)
    {
        var list = new ListValue();
        foreach (var item in el.EnumerateArray())
            list.Values.Add(ValueFromJson(item));
        return list;
    }

    /// <summary>
    /// Narrows numeric scalars for untyped (<c>object?</c>) cache values so callers see stable CLR types.
    /// </summary>
    /// <remarks>
    ///     <para>
    ///     Protobuf well-known <c>Value</c> numbers are carried as <see cref="double" />.
    ///     Parsing may also produce <see cref="long" /> (for example JSON numbers decoded with <c>TryGetInt64</c> before
    ///     conversion to proto). Those values are semantically integers but boxed as <see cref="long" /> or <see cref="double" />,
    ///     while many tests and APIs compare against <see cref="int" /> literals (for example xUnit <c>Assert.Equal(0, value)</c>),
    ///     which fails when the runtime type is <see cref="long" /> even though both sides print as <c>0</c>.
    ///     </para>
    ///     <para>
    ///     Non-numeric objects (including <see cref="JsonElement" />) are returned unchanged.
    ///     </para>
    /// </remarks>
    private static object? NormalizeUntypedScalarForUntypedCache(object? value)
    {
        return value switch
        {
            long lv and >= int.MinValue and <= int.MaxValue => (int)lv,
            double dv when double.IsInteger(dv) && dv is >= int.MinValue and <= int.MaxValue => (int)dv,
            double dv when double.IsInteger(dv) && dv is >= long.MinValue and <= long.MaxValue => (long)dv,
            _ => value,
        };
    }

    private static object? ProtoValueToClrScalarOrJson(Value v)
    {
        switch (v.KindCase)
        {
            case Value.KindOneofCase.StringValue:
                return v.StringValue;

            case Value.KindOneofCase.BoolValue:
                return v.BoolValue;

            case Value.KindOneofCase.NumberValue:
            {
                var d = v.NumberValue;
                return double.IsInteger(d) && d is >= int.MinValue and <= int.MaxValue ? (int)d : double.IsInteger(d) && d is >= long.MinValue and <= long.MaxValue ? (long)d : d;
            }

            case Value.KindOneofCase.NullValue:
                return null;

            case Value.KindOneofCase.StructValue:
            case Value.KindOneofCase.ListValue:
            {
                var buffer = new ArrayBufferWriter<byte>();
                using (var writer = new Utf8JsonWriter(buffer))
                    WriteValue(writer, v);
                return new StoredJsonPayload(buffer.WrittenSpan);
            }

            case Value.KindOneofCase.None:
            default:
                return null;
        }
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

    private static Struct CacheValueToStruct(CacheValue value) => value.KindCase switch
    {
        CacheValue.KindOneofCase.StringValue => WrapAsStruct("value", Value.ForString(value.StringValue)),
        CacheValue.KindOneofCase.BoolValue => WrapAsStruct("value", Value.ForBool(value.BoolValue)),
        CacheValue.KindOneofCase.Int64Value => WrapAsStruct("value", Value.ForNumber(value.Int64Value)),
        CacheValue.KindOneofCase.DoubleValue => WrapAsStruct("value", Value.ForNumber(value.DoubleValue)),
        CacheValue.KindOneofCase.NullValue or CacheValue.KindOneofCase.None => WrapAsStruct("value", Value.ForNull()),
        CacheValue.KindOneofCase.StructValue => value.StructValue,
        _ => throw new ArgumentOutOfRangeException(nameof(value), value.KindCase, "Unsupported cache value kind."),
    };

    [SuppressMessage("ReSharper", "RedundantEmptySwitchSection", Justification = "Style rule compatibility")]
    private static Struct ToStruct<T>(T? value)
    {
        switch (value)
        {
            case null:
                return WrapAsStruct("value", Value.ForNull());

            case StoredJsonPayload sjp:
            {
                using var doc = JsonDocument.Parse(sjp.Utf8Memory);
                var je = doc.RootElement;
                return je.ValueKind == JsonValueKind.Object ? StructFromJson(je) : WrapAsStruct("value", ValueFromJson(je));
            }

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
                break;
        }

        // SerializeToElement uses the same JsonSerializer options as SerializeToUtf8Bytes but avoids an intermediate UTF-8 byte[].
        var root = SerializationProvider.Instance.SerializeToElement(value);
        return root.ValueKind == JsonValueKind.Object ? StructFromJson(root) : WrapAsStruct("value", ValueFromJson(root));
    }

    private static bool TryReadScalarValue<T>(Value value, [MaybeNullWhen(false)] out T result)
    {
        if (typeof(T) == typeof(string) && value.KindCase == Value.KindOneofCase.StringValue)
        {
            result = (T)(object)value.StringValue;
            return true;
        }

        if (typeof(T) == typeof(bool) && value.KindCase == Value.KindOneofCase.BoolValue)
        {
            result = ReinterpretScalar<T, bool>(value.BoolValue);
            return true;
        }

        if (value.KindCase == Value.KindOneofCase.NumberValue)
        {
            var number = value.NumberValue;
            if (typeof(T) == typeof(double))
            {
                result = ReinterpretScalar<T, double>(number);
                return true;
            }

            if (typeof(T) == typeof(int) && double.IsInteger(number) && number is >= int.MinValue and <= int.MaxValue)
            {
                var intValue = (int)number;
                result = ReinterpretScalar<T, int>(intValue);
                return true;
            }

            if (typeof(T) == typeof(long) && double.IsInteger(number) && number is >= long.MinValue and <= long.MaxValue)
            {
                var longValue = (long)number;
                result = ReinterpretScalar<T, long>(longValue);
                return true;
            }
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

    private static void WriteValue(Utf8JsonWriter w, Value v)
    {
        switch (v.KindCase)
        {
            case Value.KindOneofCase.NullValue:
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
                w.WriteStartObject();
                foreach (var kv in v.StructValue.Fields)
                {
                    w.WritePropertyName(kv.Key);
                    WriteValue(w, kv.Value);
                }

                w.WriteEndObject();
                break;

            case Value.KindOneofCase.ListValue:
                w.WriteStartArray();
                foreach (var item in v.ListValue.Values)
                    WriteValue(w, item);
                w.WriteEndArray();
                break;

            case Value.KindOneofCase.None:
            default:
                w.WriteNullValue();
                break;
        }
    }
}
