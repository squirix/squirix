using System;
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
                return ProtoStructCodec.WrapAsStruct(ProtoStructCodec.ScalarEnvelopeKey, Value.ForNull());

            case JsonElement je:
                return je.ValueKind is JsonValueKind.Object ? ProtoJsonCodec.StructFromJson(je) : ProtoStructCodec.WrapAsStruct(ProtoStructCodec.ScalarEnvelopeKey, ProtoJsonCodec.ValueFromJson(je));

            default:
                if (ProtoStructCodec.EncodeScalarAsStruct(value) is { } scalar)
                    return scalar;

                var root = serializer.SerializeToElement(value);
                return root.ValueKind is JsonValueKind.Object ? ProtoJsonCodec.StructFromJson(root) : ProtoStructCodec.WrapAsStruct(ProtoStructCodec.ScalarEnvelopeKey, ProtoJsonCodec.ValueFromJson(root));
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
}
