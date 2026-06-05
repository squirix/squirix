using System;
using Squirix.Serialization;
using Squirix.Transport.Grpc.Cache;

namespace Squirix.Utils;

internal static class CacheEntryProtoExtensions
{
    public static CacheEntry<T> MapProtoEntryToCacheEntry<T>(this Entry entry, ISquirixSerializer serializer)
    {
        ArgumentNullException.ThrowIfNull(serializer);
        return new CacheEntry<T>
        {
            Value = ProtoEx.FromStruct<T>(entry.Value, serializer),
            ExpiresUtc = entry.ExpiresUtc?.ToDateTime().ToUniversalTime(),
            Expiration = entry.Expiration?.ToTimeSpan(),
        };
    }
}
