using Squirix.Server.Core;
using Squirix.Server.Errors;
using Squirix.Transport.Binary;

namespace Squirix.Server.Adapters.Wire;

internal static class SquirixServiceAdapterValidation
{
    internal static string RequireCacheName(string cacheName) => string.IsNullOrWhiteSpace(cacheName)
        ? throw new ServerCallException(FrameStatus.InvalidArgument, "cache_name is required for internal cluster RPCs.")
        : cacheName;

    internal static void RequireValidCacheKey(string key)
    {
        if (!CacheKeyValidator.TryValidate(key, out var error))
            throw ServerOpContract.InvalidCacheKey(CacheKeyValidator.GetMessage(error)).ToServerCallException();
    }
}
