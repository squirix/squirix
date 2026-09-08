using System;
using Squirix.Server.Attributes;
using Squirix.Server.Node.Endpoint;
using Squirix.Server.Runtime.Contracts;
using Squirix.Server.TestKit;
using Xunit;

namespace Squirix.Server.UnitTests.Node.Endpoint;

/// <summary>Constructor contract coverage for the routed cache API adapter.</summary>
[Immutable]
public sealed class RoutedCacheApiTests
{
    /// <summary>Verifies that the routed cache API requires a namespaced cache.</summary>
    [Fact]
    public void ConstructorRequiresNamespacedCache()
    {
        ILogicalNamespacedCache<string>? namespaced = null;
        _ = NodeExceptionAssert.For<ArgumentNullException>().Throws(namespaced, static ns => _ = new RoutedCacheApi<string>(ns!, "cache-a"));
    }

    /// <summary>Verifies that the routed cache API requires a cache name.</summary>
    [Fact]
    public void ConstructorRequiresCacheName()
    {
        var namespaced = new ILogicalNamespacedCacheCreateExpectations<string>().Instance();
        const string? cacheName = null;
        _ = NodeExceptionAssert.For<ArgumentNullException>().Throws(namespaced, cacheName, static (ns, name) => _ = new RoutedCacheApi<string>(ns, name!));
    }
}
