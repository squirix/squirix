using System;
using System.Threading.Tasks;
using Squirix.Serialization;
using Squirix.Transport.Grpc.Cache;

namespace Squirix.Utils;

internal static class CacheEntryProtoExtensions
{
    public static async ValueTask<CacheEntry<T>> MapProtoEntryToCacheEntryAsync<T>(this Entry entry, ISquirixSerializer serializer)
    {
        ArgumentNullException.ThrowIfNull(serializer);
        return new CacheEntry<T>
        {
            Value = await ProtoEx.FromStructAsync<T>(entry.Value, serializer).ConfigureAwait(false),
            ExpiresUtc = entry.ExpiresUtc?.ToDateTime().ToUniversalTime(),
            Expiration = entry.Expiration?.ToTimeSpan(),
        };
    }
}
