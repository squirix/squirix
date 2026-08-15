using System;
using System.Buffers;
using System.Globalization;
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

    internal static ValueTask<T?> FromCacheValueAsync<T>(CacheValue value, ISquirixSerializer serializer)
    {
        ArgumentNullException.ThrowIfNull(value);
        ArgumentNullException.ThrowIfNull(serializer);

        if (typeof(T) == typeof(object))
            return new ValueTask<T?>(ProtoScalarMapping.Coerce<T>(FromCacheValueAsObject(value, serializer)));

        if (ProtoScalarMapping.TryMapTypedPrimitive<T>(value, out var primitive))
            return new ValueTask<T?>(primitive);

        if (value.KindCase is CacheValue.KindOneofCase.NullValue or CacheValue.KindOneofCase.None)
            return new ValueTask<T?>(default(T?));

        if (value.KindCase is CacheValue.KindOneofCase.StructValue && value.StructValue is { } structValue)
            return new ValueTask<T?>(ProtoStructCodec.FromStruct<T>(structValue, serializer));

        if (ProtoScalarMapping.IsTypedPrimitiveKind(value.KindCase))
            return new ValueTask<T?>(ProtoStructCodec.FromStruct<T>(ProtoStructCodec.ToStructValueWrapper(value), serializer));

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
                Value = ProtoStructCodec.FromStruct<T>(entry.Value, serializer),
                ExpiresUtc = entry.ExpiresUtc?.ToDateTime().ToUniversalTime(),
                Expiration = entry.Expiration?.ToTimeSpan(),
            });
    }

    private static Struct ToStruct<T>(T? value, ISquirixSerializer serializer)
    {
        switch (value)
        {
            case null:
                return ProtoStructCodec.WrapAsStruct(ScalarEnvelopeKey, Value.ForNull());

            case JsonElement je:
                return je.ValueKind is JsonValueKind.Object ? ProtoJsonCodec.StructFromJson(je) : ProtoStructCodec.WrapAsStruct(ScalarEnvelopeKey, ProtoJsonCodec.ValueFromJson(je));

            default:
                if (ProtoStructCodec.EncodeScalarAsStruct(value) is { } scalar)
                    return scalar;

                var root = serializer.SerializeToElement(value);
                return root.ValueKind is JsonValueKind.Object ? ProtoJsonCodec.StructFromJson(root) : ProtoStructCodec.WrapAsStruct(ScalarEnvelopeKey, ProtoJsonCodec.ValueFromJson(root));
        }
    }

    private static object? FromCacheValueAsObject(CacheValue value, ISquirixSerializer serializer) => value.KindCase switch
    {
        CacheValue.KindOneofCase.StringValue => value.StringValue,
        CacheValue.KindOneofCase.BoolValue => value.BoolValue,
        CacheValue.KindOneofCase.Int32Value => int.CreateChecked(value.Int32Value),
        CacheValue.KindOneofCase.Int64Value => value.Int64Value,
        CacheValue.KindOneofCase.DoubleValue => value.DoubleValue,
        CacheValue.KindOneofCase.NullValue or CacheValue.KindOneofCase.None => null,
        CacheValue.KindOneofCase.StructValue when value.StructValue is { } structValue => ProtoStructCodec.FromStruct<object?>(structValue, serializer),
        _ => throw new ArgumentOutOfRangeException(nameof(value), "Unsupported cache value kind."),
    };

    /// <summary>Encodes CLR values into protobuf <see cref="Struct" /> payloads and decodes them back.</summary>
    private static class ProtoStructCodec
    {
        internal static Struct? EncodeScalarAsStruct<T>(T? value)
        {
            return value switch
            {
                string text => WrapAsStruct(ScalarEnvelopeKey, Value.ForString(text)),
                int number => WrapAsStruct(ScalarEnvelopeKey, Value.ForNumber(number)),
                long number => WrapAsStruct(ScalarEnvelopeKey, ProtoJsonCodec.CreateNumberEnvelope(ProtoJsonCodec.NumberEnvelopeInt64Key, number.ToString(CultureInfo.InvariantCulture))),
                double number => WrapAsStruct(ScalarEnvelopeKey, Value.ForNumber(number)),
                bool boolean => WrapAsStruct(ScalarEnvelopeKey, Value.ForBool(boolean)),
                decimal dec => WrapAsStruct(ScalarEnvelopeKey, ProtoJsonCodec.CreateNumberEnvelope(ProtoJsonCodec.NumberEnvelopeDecimalKey, dec.ToString(CultureInfo.InvariantCulture))),
                _ => null,
            };
        }

        internal static T? FromStruct<T>(Struct value, ISquirixSerializer serializer)
        {
            ArgumentNullException.ThrowIfNull(value);
            ArgumentNullException.ThrowIfNull(serializer);

            if (value.Fields.Count is 1 && value.Fields.TryGetValue(ScalarEnvelopeKey, out var wrapped))
                return FromValue<T>(wrapped, serializer);

            return Deserialize<T>(Value.ForStruct(value), serializer);
        }

        internal static Struct ToStructValueWrapper(CacheValue value) => value.KindCase switch
        {
            CacheValue.KindOneofCase.StringValue => WrapAsStruct(ScalarEnvelopeKey, Value.ForString(value.StringValue)),
            CacheValue.KindOneofCase.BoolValue => WrapAsStruct(ScalarEnvelopeKey, Value.ForBool(value.BoolValue)),
            CacheValue.KindOneofCase.Int32Value => WrapAsStruct(ScalarEnvelopeKey, Value.ForNumber(value.Int32Value)),
            CacheValue.KindOneofCase.Int64Value => WrapAsStruct(
                ScalarEnvelopeKey,
                ProtoJsonCodec.CreateNumberEnvelope(ProtoJsonCodec.NumberEnvelopeInt64Key, value.Int64Value.ToString(CultureInfo.InvariantCulture))),
            CacheValue.KindOneofCase.DoubleValue => WrapAsStruct(ScalarEnvelopeKey, Value.ForNumber(value.DoubleValue)),
            CacheValue.KindOneofCase.NullValue or CacheValue.KindOneofCase.None => WrapAsStruct(ScalarEnvelopeKey, Value.ForNull()),
            CacheValue.KindOneofCase.StructValue => value.StructValue,
            _ => throw new ArgumentOutOfRangeException(nameof(value), "Unsupported cache value kind."),
        };

        internal static Struct WrapAsStruct(string fieldName, Value v)
        {
            return new Struct
            {
                Fields =
                {
                    [fieldName] = v,
                },
            };
        }

        private static T? Deserialize<T>(Value value, ISquirixSerializer serializer)
        {
            var buffer = new ArrayBufferWriter<byte>(256);

            // Sync flush: WriteValue is synchronous; async Utf8JsonWriter disposal would allocate a state machine on every decoding.
#pragma warning disable MA0045
            using (var writer = new Utf8JsonWriter(buffer))
            {
                WriteValue(writer, value);
                writer.Flush();
            }
#pragma warning restore MA0045

            return serializer.Deserialize<T>(buffer.WrittenSpan);
        }

        private static T? FromValue<T>(Value value, ISquirixSerializer serializer)
        {
            if (typeof(T) == typeof(object))
                return ProtoScalarMapping.Coerce<T>(ToUntypedValue(value, serializer));

            return Deserialize<T>(value, serializer);
        }

        private static object? ToUntypedValue(Value value, ISquirixSerializer serializer) => value.KindCase switch
        {
            Value.KindOneofCase.StringValue => value.StringValue,
            Value.KindOneofCase.BoolValue => value.BoolValue,
            Value.KindOneofCase.NumberValue => ProtoJsonCodec.NormalizeNumber(value.NumberValue),
            Value.KindOneofCase.NullValue or Value.KindOneofCase.None => null,
            Value.KindOneofCase.StructValue or Value.KindOneofCase.ListValue => Deserialize<JsonElement>(value, serializer),
            _ => throw new ArgumentOutOfRangeException(nameof(value), value.KindCase, "Unsupported value kind."),
        };

        private static void WriteListValue(Utf8JsonWriter writer, ListValue listValue)
        {
            writer.WriteStartArray();
            var values = listValue.Values;

            for (var index = 0; index < values.Count; index++)
                WriteValue(writer, values[index]);

            writer.WriteEndArray();
        }

        private static void WriteStructValue(Utf8JsonWriter writer, Struct structValue)
        {
            if (ProtoJsonCodec.TryWriteNumberEnvelope(writer, structValue))
                return;

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
                    WriteStructValue(writer, value.StructValue);
                    return;
                case Value.KindOneofCase.ListValue:
                    WriteListValue(writer, value.ListValue);
                    return;
                default:
                    throw new ArgumentOutOfRangeException(nameof(value), "Unsupported protobuf value kind.");
            }
        }
    }
}
