using System;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Core;
using Squirix.Server.Node.Endpoint;
using Squirix.Server.Runtime.Contracts;
using Squirix.Server.TestKit;
using Xunit;

namespace Squirix.Server.UnitTests.Node.Endpoint;

/// <summary>Constructor contract coverage for the routed cache API adapter.</summary>
public sealed class RoutedCacheApiTests
{
    /// <summary>Verifies that the routed cache API requires a namespaced cache.</summary>
    [Fact]
    public void ConstructorRequiresNamespacedCache()
    {
        ILogicalNamespacedCache<string>? namespaced = null;

        _ = NodeExceptionAssert.For<ArgumentNullException>().Throws(
            namespaced,
            static ns => _ = new RoutedCacheApi<string>(ns!, "cache-a"));
    }

    /// <summary>Verifies that the routed cache API requires a cache name.</summary>
    [Fact]
    public void ConstructorRequiresCacheName()
    {
        var namespaced = new NotSupportedLogicalCache();
        const string? cacheName = null;

        _ = NodeExceptionAssert.For<ArgumentNullException>().Throws(
            namespaced,
            cacheName,
            static (ns, name) => _ = new RoutedCacheApi<string>(ns, name!));
    }

    private sealed class NotSupportedLogicalCache : ILogicalNamespacedCache<string>
    {
        public ValueTask<NodeCacheEntry<string>?> GetEntryAsync(string cacheName, string key, CancellationToken cancellationToken) => throw new NotSupportedException();

        public ValueTask<NodeCacheValueResult<string>> GetValueAsync(string cacheName, string key, CancellationToken cancellationToken) => throw new NotSupportedException();

        public ValueTask<CacheRemoveResult<string>> RemoveAsync(string operationId, string cacheName, string key, CancellationToken cancellationToken) => throw new NotSupportedException();

        public ValueTask<bool> RemoveExpirationAsync(string operationId, string cacheName, string key, CancellationToken cancellationToken) => throw new NotSupportedException();

        public ValueTask SetEntryAsync(string operationId, string cacheName, string key, NodeCacheEntry<string> entry, CancellationToken cancellationToken) => throw new NotSupportedException();

        public ValueTask<bool> TouchAsync(string operationId, string cacheName, string key, TimeSpan expiration, CancellationToken cancellationToken) => throw new NotSupportedException();

        public ValueTask<bool> TryAddEntryAsync(string operationId, string cacheName, string key, NodeCacheEntry<string> entry, CancellationToken cancellationToken) => throw new NotSupportedException();

        public ValueTask<bool> UpdateAsync(string operationId, string cacheName, string key, string? value, CancellationToken cancellationToken) => throw new NotSupportedException();
    }
}
