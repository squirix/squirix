using Grpc.Core;
using Squirix.Server.Core;
using Squirix.Server.Errors;

namespace Squirix.Server.Adapters.Grpc;

internal static class SquirixServiceAdapterValidation
{
    internal static string RequireCacheName(string cacheName) => string.IsNullOrWhiteSpace(cacheName)
        ? throw new RpcException(new Status(StatusCode.InvalidArgument, "cache_name is required for internal cluster RPCs.")) : cacheName;

    internal static void RequireValidCacheKey(string key)
    {
        if (!CacheKeyValidator.TryValidate(key, out var error))
            throw ServerOpContract.InvalidCacheKey(CacheKeyValidator.GetMessage(error)).ToRpcException();
    }
}
