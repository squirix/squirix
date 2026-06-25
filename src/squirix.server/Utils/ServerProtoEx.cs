using System;
using System.Buffers;
using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;
using System.Text.Json;
using System.Threading.Tasks;
using Google.Protobuf.WellKnownTypes;
using Squirix.Server.Serialization;
using Squirix.Transport.Grpc.Cache;
using RpcEntry = Squirix.Transport.Grpc.Cache.CacheEntryWire;

namespace Squirix.Server.Utils;

internal static class ServerProtoEx
{
    public static async ValueTask<CacheEntry<T>> MapFromProtoAsync<T>(this RpcEntry e)
    {
        var value = await FromStructAsync<T>(e.Value).ConfigureAwait(false);
        DateTime? expires = null;
        if (e.ExpiresUtc is not null && (e.ExpiresUtc.Seconds != 0 || e.ExpiresUtc.Nanos is not 0))
            expires = e.ExpiresUtc.ToDateTime().ToUniversalTime();

        if (typeof(T) == typeof(object))
            value = Coerce<T>(NormalizeUntypedScalarForUntypedCache(value));

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

    internal static async ValueTask<CacheEntry<T>> CacheValueFromGrpcValueAsync<T>(CacheValue value, Timestamp? expiresUtc, Duration? expiration)
    {
        ArgumentNullException.ThrowIfNull(value);

        return new CacheEntry<T>
        {
            Value = await MapCacheValueAsync<T>(value).ConfigureAwait(false),
            ExpiresUtc = expiresUtc?.ToDateTime().ToUniversalTime(),
            Expiration = expiration?.ToTimeSpan(),
        };
    }

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

    internal static async ValueTask<T?> MapCacheValueAsync<T>(CacheValue value)
    {
        if (typeof(T) == typeof(object))
            return new ValueTask<T?>(Coerce<T>(MapCacheValueAsObject(value)));

        switch (value.KindCase)
        {
            case CacheValue.KindOneofCase.StringValue:
                if (typeof(T) == typeof(string))
                    return new ValueTask<T?>(ReinterpretReference<T, string>(value.StringValue));
                break;

            case CacheValue.KindOneofCase.BoolValue:
                if (typeof(T) == typeof(bool))
                    return new ValueTask<T?>(ReinterpretScalar<T, bool>(value.BoolValue));
                break;

            case CacheValue.KindOneofCase.Int32Value:
                if (typeof(T) == typeof(int))
                    return new ValueTask<T?>(ReinterpretScalar<T, int>(int.CreateChecked(value.Int32Value)));
                break;

            case CacheValue.KindOneofCase.Int64Value:
                if (typeof(T) == typeof(long))
                    return new ValueTask<T?>(ReinterpretScalar<T, long>(value.Int64Value));
                break;

            case CacheValue.KindOneofCase.DoubleValue:
                if (typeof(T) == typeof(double))
                    return new ValueTask<T?>(ReinterpretScalar<T, double>(value.DoubleValue));
                break;

            case CacheValue.KindOneofCase.NullValue:
            case CacheValue.KindOneofCase.None:
                return new ValueTask<T?>(default(T?));

            case CacheValue.KindOneofCase.StructValue when value.StructValue is { } structValue:
                return new ValueTask<T?>(FromStruct<T>(structValue));

            default:
                throw new ArgumentOutOfRangeException(nameof(value), "Unsupported cache value kind.");
        }

        return new ValueTask<T?>(FromStruct<T>(CacheValueToStruct(value)));
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

    private static async ValueTask<T?> DeserializeFromProtoValueAsync<T>(Value value)
    {
        var buffer = WriteValueToBuffer(value);
        return SerializationProvider.Deserialize<T>(buffer.WrittenSpan);
    }

    private static async ValueTask<T?> FromStructAsync<T>(Struct s)
    {
        if (typeof(T) != typeof(object))
        {
            if (s.Fields.Count is not 1 || !s.Fields.TryGetValue("value", out var onlyWrapped))
                return DeserializeFromProtoValue<T>(Value.ForStruct(s));

            return TryReadScalarValue<T>(onlyWrapped, out var scalar) ? scalar : await DeserializeFromProtoValueAsync<T>(onlyWrapped).ConfigureAwait(false);
        }

        if (s.Fields.Count is 1 && s.Fields.TryGetValue("value", out var only))
            return Coerce<T>(ProtoValueToClrScalarOrJson(only));

        var buffer = await WriteValueToBufferAsync(Value.ForStruct(s)).ConfigureAwait(false);
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

    private static async ValueTask<object?> MapCacheValueAsObjectAsync(CacheValue value)
    {
        return value.KindCase switch
        {
            CacheValue.KindOneofCase.StringValue => value.StringValue,
            CacheValue.KindOneofCase.BoolValue => value.BoolValue,
            CacheValue.KindOneofCase.Int64Value => value.Int64Value is >= int.MinValue and <= int.MaxValue ? Convert.ToInt32(value.Int64Value) : value.Int64Value,
            CacheValue.KindOneofCase.DoubleValue => value.DoubleValue,
            CacheValue.KindOneofCase.NullValue or CacheValue.KindOneofCase.None => null,
            CacheValue.KindOneofCase.StructValue when value.StructValue is { } structValue => await FromStructAsync<object?>(structValue).ConfigureAwait(false),
            _ => await FromStructAsync<object?>(CacheValueToStruct(value)).ConfigureAwait(false),
        };
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
    /// <param name="value">Untyped cache scalar to normalize.</param>
    private static object? NormalizeUntypedScalarForUntypedCache(object? value)
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
                var d = v.NumberValue;
                var d2 = double.IsInteger(d) && d is >= long.MinValue and <= long.MaxValue ? Convert.ToInt64(d) : d;
                return double.IsInteger(d) && d is >= int.MinValue and <= int.MaxValue ? Convert.ToInt32(d) : d2;

            case Value.KindOneofCase.NullValue:
                return null;

            case Value.KindOneofCase.StructValue:
            case Value.KindOneofCase.ListValue:
            {
                var buffer = await WriteValueToBufferAsync(v).ConfigureAwait(false);
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

    private static async ValueTask<ArrayBufferWriter<byte>> WriteValueToBufferAsync(Value value)
    {
        var buffer = new ArrayBufferWriter<byte>();
        var writer = new Utf8JsonWriter(buffer);
        await using (writer.ConfigureAwait(false))
        {
            WriteValue(writer, value);
            await writer.FlushAsync(CancellationToken.None).ConfigureAwait(false);
        }

        return buffer;
    }
}
