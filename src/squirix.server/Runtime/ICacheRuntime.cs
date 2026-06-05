using Squirix.Server.Runtime.Contracts;

namespace Squirix.Server.Runtime;

internal interface ICacheRuntime
{
    ILogicalNamespacedCache<T> GetCache<T>(string cacheName);
}
