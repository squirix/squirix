using System;
using System.Buffers;
using System.Diagnostics.CodeAnalysis;
using System.Globalization;
using System.Runtime.CompilerServices;
using System.Text.Json;
using System.Threading.Tasks;
using Google.Protobuf.WellKnownTypes;
using Squirix.Server.Core;
using Squirix.Transport.Grpc.Cache;
using RpcEntry = Squirix.Transport.Grpc.Cache.CacheEntryWire;

namespace Squirix.Server.Utils;

internal static class ServerProtoEx
{
    private const string ScalarEnvelopeKey = "\0squirix:scalar";
    private const string NumberEnvelopeInt64Key = "\0squirix:int64";
    private const string NumberEnvelopeDecimalKey = "\0squirix:decimal";

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

    private static T? DeserializeFromProtoValue<T>(Value value)
    {
        var buffer = ValueJson.WriteValueToBuffer(value);
        return SerializerProvider.Deserialize<T>(buffer.WrittenSpan);
    }

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

    private static T? FromStruct<T>(Struct s)
    {
        if (typeof(T) != typeof(object))
        {
            if (s.Fields.Count is not 1 || !s.Fields.TryGetValue(ScalarEnvelopeKey, out var onlyWrapped))
                return DeserializeFromProtoValue<T>(Value.ForStruct(s));

            return TryReadScalarValue<T>(onlyWrapped, out var scalar) ? scalar : DeserializeFromProtoValue<T>(onlyWrapped);
        }

        if (s.Fields.Count is 1 && s.Fields.TryGetValue(ScalarEnvelopeKey, out var only))
            return Coerce<T>(ProtoValueToClrScalarOrJson(only));

        var buffer = ValueJson.WriteValueToBuffer(Value.ForStruct(s));
        using var document = JsonDocument.Parse(buffer.WrittenMemory);
        return Coerce<T>(document.RootElement.Clone());
    }

    private static bool IsWireScalarKind(CacheValue.KindOneofCase kind) => kind switch
    {
        CacheValue.KindOneofCase.StringValue or CacheValue.KindOneofCase.BoolValue or CacheValue.KindOneofCase.Int32Value or CacheValue.KindOneofCase.Int64Value
            or CacheValue.KindOneofCase.DoubleValue => true,
        _ => false,
    };

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
                var buffer = ValueJson.WriteValueToBuffer(v);
                using var document = JsonDocument.Parse(buffer.WrittenMemory);
                return document.RootElement.Clone();
            }

            case Value.KindOneofCase.None:
            default:
                return null;
        }
    }

    private static Struct ToStruct<T>(T? value) => ValueJson.EncodeToStruct(value);

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

        if (destination != typeof(double))
            return false;
        if (wire.KindCase is not CacheValue.KindOneofCase.DoubleValue)
            return false;

        decoded = CastScalar<T, double>(wire.DoubleValue);
        return true;
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
                boxed = CreateNumberEnvelope(NumberEnvelopeInt64Key, wire.Int64Value.ToString(CultureInfo.InvariantCulture));
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
        envelope.Fields.Add(ScalarEnvelopeKey, boxed);
        return envelope;
    }

    private static Value CreateNumberEnvelope(string markerKey, string numberText)
    {
        var s = new Struct();
        s.Fields.Add(markerKey, Value.ForString(numberText));
        return Value.ForStruct(s);
    }

    /// <summary>JSON ↔ protobuf <see cref="Value" /> helpers owned by <see cref="ServerProtoEx" />.</summary>
    private static class ValueJson
    {
        /// <summary>Maps a CLR/cache value into the protobuf struct envelope used on the wire.</summary>
        /// <typeparam name="T">Logical cache value type.</typeparam>
        /// <param name="value">Value to encode.</param>
        /// <returns>Protobuf struct payload.</returns>
        internal static Struct EncodeToStruct<T>(T? value)
        {
            if (value is null)
                return CreateSingleFieldStruct(Value.ForNull());

            if (value is JsonElement jsonElement)
                return EncodeJsonElement(jsonElement);

            // Scalar and fallback arms live in a helper to keep local-variable count low.
            return EncodeBoxedScalar(value);
        }

        /// <summary>
        /// Serializes a protobuf <see cref="Value" /> tree to a UTF-8 JSON buffer for serializer decode.
        /// </summary>
        /// <param name="value">Protobuf value to encode.</param>
        /// <returns>Buffer containing the JSON payload.</returns>
        internal static ArrayBufferWriter<byte> WriteValueToBuffer(Value value)
        {
            var buffer = new ArrayBufferWriter<byte>(256);

            // Sync flush: WriteValue is synchronous; async Utf8JsonWriter disposal would allocate a state machine on every decoding.
#pragma warning disable MA0045
            using var writer = new Utf8JsonWriter(buffer);
            WriteValue(writer, value);
            writer.Flush();
#pragma warning restore MA0045

            return buffer;
        }

        private static Struct EncodeJsonElement(JsonElement element)
        {
            return element.ValueKind is JsonValueKind.Object ? BuildStructFromJsonObject(element)
                : CreateSingleFieldStruct(ConvertJsonElementToProtoValue(element));
        }

        private static Struct EncodeBoxedScalar<T>(T value)
        {
            if (value is string text)
                return CreateSingleFieldStruct(Value.ForString(text));

            if (value is int int32)
                return CreateSingleFieldStruct(Value.ForNumber(int32));

            if (value is long int64)
                return CreateSingleFieldStruct(CreateNumberEnvelope(NumberEnvelopeInt64Key, int64.ToString(CultureInfo.InvariantCulture)));

            if (value is double floating)
                return CreateSingleFieldStruct(Value.ForNumber(floating));

            if (value is bool flag)
                return CreateSingleFieldStruct(Value.ForBool(flag));

            if (value is decimal dec)
                return CreateSingleFieldStruct(CreateNumberEnvelope(NumberEnvelopeDecimalKey, dec.ToString(CultureInfo.InvariantCulture)));

            // SerializeToElement uses the same NodeJsonSerializer options as SerializeToUtf8Bytes but avoids an intermediate UTF-8 byte[].
            var root = SerializerProvider.Instance.SerializeToElement(value);
            return root.ValueKind is JsonValueKind.Object ? BuildStructFromJsonObject(root) : CreateSingleFieldStruct(ConvertJsonElementToProtoValue(root));
        }

        private static ListValue BuildListValueFromJsonArray(JsonElement arrayElement)
        {
            var list = new ListValue();
            var destination = list.Values;
            var count = arrayElement.GetArrayLength();
            for (var offset = 0; offset < count; offset++)
                destination.Add(ConvertJsonElementToProtoValue(arrayElement[offset]));

            return list;
        }

        private static Struct BuildStructFromJsonObject(JsonElement objectElement)
        {
            var result = new Struct();
            var fields = result.Fields;
            foreach (var property in objectElement.EnumerateObject())
                fields[property.Name] = ConvertJsonElementToProtoValue(property.Value);

            return result;
        }

        private static Value ConvertJsonElementToProtoValue(JsonElement element)
        {
            return element.ValueKind switch
            {
                JsonValueKind.Object => Value.ForStruct(BuildStructFromJsonObject(element)),
                JsonValueKind.Array => new Value { ListValue = BuildListValueFromJsonArray(element) },
                JsonValueKind.String => Value.ForString(element.GetString()),
                JsonValueKind.Number => ConvertJsonNumber(element),
                JsonValueKind.True => Value.ForBool(true),
                JsonValueKind.False => Value.ForBool(false),
                JsonValueKind.Null or JsonValueKind.Undefined => Value.ForNull(),
                _ => throw new ArgumentOutOfRangeException(nameof(element), "Unsupported JSON value kind."),
            };
        }

        private static Value ConvertJsonNumber(JsonElement element)
        {
            if (element.TryGetInt64(out var asInt64))
                return CreateNumberEnvelope(NumberEnvelopeInt64Key, asInt64.ToString(CultureInfo.InvariantCulture));

            if (element.TryGetDecimal(out var asDecimal))
                return CreateNumberEnvelope(NumberEnvelopeDecimalKey, asDecimal.ToString(CultureInfo.InvariantCulture));

            return Value.ForNumber(element.GetDouble());
        }

        private static Struct CreateSingleFieldStruct(Value fieldValue)
        {
            var envelope = new Struct();
            envelope.Fields.Add(ScalarEnvelopeKey, fieldValue);
            return envelope;
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

        /// <summary>Writes list values as a JSON array, recursing through nested values.</summary>
        /// <param name="writer">JSON writer receiving the array.</param>
        /// <param name="listValue">Protobuf list whose items are written.</param>
        private static void WriteListItems(Utf8JsonWriter writer, ListValue listValue)
        {
            writer.WriteStartArray();
            var items = listValue.Values;
            for (var itemIndex = 0; itemIndex < items.Count; itemIndex++)
                WriteValue(writer, items[itemIndex]);

            writer.WriteEndArray();
        }

        /// <summary>Writes struct fields as a JSON object, recursing through nested values.</summary>
        /// <param name="writer">JSON writer receiving the object.</param>
        /// <param name="structValue">Protobuf struct whose fields are written.</param>
        private static void WriteStructFields(Utf8JsonWriter writer, Struct structValue)
        {
            writer.WriteStartObject();
            var fields = structValue.Fields;
            using var fieldEnumerator = fields.GetEnumerator();
            for (var index = 0; index < fields.Count; index++)
            {
                _ = fieldEnumerator.MoveNext();
                var field = fieldEnumerator.Current;
                writer.WritePropertyName(field.Key);
                WriteValue(writer, field.Value);
            }

            writer.WriteEndObject();
        }

        private static void WriteStructValue(Utf8JsonWriter writer, Struct structValue)
        {
            if (TryWriteNumberEnvelope(writer, structValue))
                return;

            WriteStructFields(writer, structValue);
        }

        /// <summary>
        /// Emits a protobuf <see cref="Value" /> tree as JSON so <c>ISquirixSerializer</c> can deserialize complex cache payloads.
        /// </summary>
        /// <param name="writer">JSON writer receiving the encoded tree.</param>
        /// <param name="value">Protobuf value to encode.</param>
        /// <exception cref="InvalidOperationException">Thrown when <paramref name="value" /> has an unsupported kind.</exception>
        private static void WriteValue(Utf8JsonWriter writer, Value value)
        {
            // Dispatch by KindCase; nested struct/list arms live in dedicated helpers to keep locals low.
            switch (value.KindCase)
            {
                case Value.KindOneofCase.NullValue:
                case Value.KindOneofCase.None:
                    writer.WriteNullValue();
                    return;
                case Value.KindOneofCase.BoolValue:
                    writer.WriteBooleanValue(value.BoolValue);
                    return;
                case Value.KindOneofCase.NumberValue:
                    writer.WriteNumberValue(value.NumberValue);
                    return;
                case Value.KindOneofCase.StringValue:
                    writer.WriteStringValue(value.StringValue);
                    return;
                case Value.KindOneofCase.StructValue:
                    WriteStructValue(writer, value.StructValue);
                    return;
                case Value.KindOneofCase.ListValue:
                    WriteListItems(writer, value.ListValue);
                    return;
                default:
                    throw new InvalidOperationException($"Unsupported protobuf value kind: {value.KindCase}.");
            }
        }
    }
}
