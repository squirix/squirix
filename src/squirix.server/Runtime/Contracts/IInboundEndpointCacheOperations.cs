using Squirix.Server.Contracts;

namespace Squirix.Server.Runtime.Contracts;

/// <summary>
/// Node-side cache operations used by inbound REST/gRPC endpoints.
/// </summary>
/// <typeparam name="T">The cache value type.</typeparam>
internal interface IInboundEndpointCacheOperations<T>
{
    ICacheApi<T> ForCache(string cacheName);
}
