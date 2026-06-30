using System;
using System.Threading.Tasks;
using Google.Protobuf.WellKnownTypes;
using Squirix.Server.Adapters.Grpc;
using Squirix.Server.Storage.Entries.Binary;
using Squirix.Transport.Grpc.Cache;
using RpcEntry = Squirix.Transport.Grpc.Cache.CacheEntryWire;

namespace Squirix.Server.Utils;

internal static class CacheWireMapper
{
    public static ValueTask<CacheEntry<T>> MapFromProtoAsync<T>(this RpcEntry entry)
    {
        var value = CacheWireCodec.FromEntryWireValue<T>(entry);
        DateTime? expires = null;
        if (entry.ExpiresUtc is not null && (entry.ExpiresUtc.Seconds != 0 || entry.ExpiresUtc.Nanos is not 0))
            expires = entry.ExpiresUtc.ToDateTime().ToUniversalTime();

        var cacheEntry = new CacheEntry<T>
        {
            Value = value,
            WireValuePayload = CacheWirePayloadCapture.CopyFromEntryWire(entry.Payload.Span),
            ExpiresUtc = expires,
            Expiration = entry.Expiration?.ToTimeSpan(),
        };
        return ValueTask.FromResult(cacheEntry);
    }

    public static RpcEntry MapToProto<T>(this CacheEntry<T> entry)
    {
        var wire = CacheWireCodec.ToEntryWire(entry.Value);
        wire.ExpiresUtc = entry.ExpiresUtc is null ? null : Timestamp.FromDateTime(DateTime.SpecifyKind(entry.ExpiresUtc.Value, DateTimeKind.Utc));
        wire.Expiration = entry.Expiration is null ? null : Duration.FromTimeSpan(entry.Expiration.Value);
        return wire;
    }

    internal static ValueTask<CacheEntry<T>> CacheValueFromGrpcValueAsync<T>(CacheValue value, Timestamp? expiresUtc, Duration? expiration)
    {
        ArgumentNullException.ThrowIfNull(value);

        var cacheEntry = new CacheEntry<T>
        {
            Value = CacheWireCodec.FromCacheValue<T>(value),
            ExpiresUtc = expiresUtc?.ToDateTime().ToUniversalTime(),
            Expiration = expiration?.ToTimeSpan(),
        };
        return ValueTask.FromResult(cacheEntry);
    }

    internal static CacheValue CacheValueToGrpcValue<T>(T? value) => CacheWireCodec.ToCacheValue(value);

    internal static CacheValue CacheValueToGrpcValue<T>(T? value, ReadOnlyMemory<byte> wireValuePayload) =>
        !wireValuePayload.IsEmpty ? CacheWireCodec.ToCacheValueFromWirePayload(wireValuePayload) : CacheWireCodec.ToCacheValue(value);

    internal static ValueTask<T?> MapCacheValueAsync<T>(CacheValue value)
    {
        ArgumentNullException.ThrowIfNull(value);
        return ValueTask.FromResult(CacheWireCodec.FromCacheValue<T>(value));
    }
}
