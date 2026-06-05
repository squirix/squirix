using Squirix.Server.Contracts;

namespace Squirix.Server.Adapters.Grpc;

internal interface IGrpcCacheOperations<T>
{
    ICacheApi<T> ForCache(string cacheName);
}
