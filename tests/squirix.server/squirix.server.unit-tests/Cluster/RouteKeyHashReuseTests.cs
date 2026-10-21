using System;
using System.Collections.Generic;
using System.Globalization;
using System.Security.Cryptography;
using System.Text;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.Cluster;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Cluster;

/// <summary>Tests that reusing the last route-key hash never changes the owner of a key.</summary>
[Immutable]
public sealed class RouteKeyHashReuseTests : ServerUnitTestBase
{
    private const int LongKeyLength = 4096;

    private const int VirtualNodes = 128;

    /// <summary>Alternating cache names and keys always match an independent SHA-256 ring.</summary>
    [Test]
    public async Task InterleavedLookupsMatchIndependentRing()
    {
        string[] nodes = ["n1", "n2", "n3"];
        var locator = RuntimeServiceRegistration.CreateHashLocator(nodes, VirtualNodes);
        var reference = ReferenceRing.Create(nodes);
        var keys = SampleKeys();
        for (var i = 0; i < keys.Count * 2; i++)
        {
            var (cacheName, key) = keys[i % keys.Count];
            _ = await Assert.That(locator.GetOwner(cacheName, key)).IsEqualTo(reference.OwnerOf(cacheName, key));
        }
    }

    /// <summary>Repeated lookups of the same key return the owner of a fresh computation.</summary>
    [Test]
    public async Task RepeatedLookupsMatchIndependentRing()
    {
        string[] nodes = ["n1", "n2", "n3"];
        var locator = RuntimeServiceRegistration.CreateHashLocator(nodes, VirtualNodes);
        var reference = ReferenceRing.Create(nodes);
        var keys = SampleKeys();
        for (var i = 0; i < keys.Count; i++)
        {
            var (cacheName, key) = keys[i];
            var expected = reference.OwnerOf(cacheName, key);
            for (var repeat = 0; repeat < 3; repeat++)
                _ = await Assert.That(locator.GetOwner(cacheName, key)).IsEqualTo(expected);
        }
    }

    /// <summary>The same key under another cache name is not served from the reused hash.</summary>
    [Test]
    public async Task SameKeyUnderOtherCacheIsHashedAgain()
    {
        string[] nodes = ["n1", "n2", "n3", "n4"];
        var locator = RuntimeServiceRegistration.CreateHashLocator(nodes, VirtualNodes);
        var reference = ReferenceRing.Create(nodes);
        for (var i = 0; i < 200; i++)
        {
            var key = "key-" + i.ToString(CultureInfo.InvariantCulture);
            _ = await Assert.That(locator.GetOwner("cache-a", key)).IsEqualTo(reference.OwnerOf("cache-a", key));
            _ = await Assert.That(locator.GetOwner("cache-b", key)).IsEqualTo(reference.OwnerOf("cache-b", key));
        }
    }

    private static List<(string CacheName, string Key)> SampleKeys()
    {
        var keys = new List<(string CacheName, string Key)>
        {
            ("cache", string.Empty),
            ("cache", "a"),
            ("cache", "k\u00e9y-\u4e2d\u6587-\ud83d\ude80"),
            ("\u00e7ache", "key"),
            ("cache", new string('x', LongKeyLength)),
            ("cache", new string('\u4e2d', LongKeyLength)),
        };
        for (var i = 0; i < 100; i++)
            keys.Add(("cache" + (i % 3).ToString(CultureInfo.InvariantCulture), "key-" + i.ToString(CultureInfo.InvariantCulture)));

        return keys;
    }

    private sealed class ReferenceRing
    {
        private readonly (ulong Hash, string Node)[] _items;

        private ReferenceRing((ulong Hash, string Node)[] items)
        {
            _items = items;
        }

        internal static ReferenceRing Create(string[] nodes)
        {
            var items = new (ulong Hash, string Node)[nodes.Length * VirtualNodes];
            for (var i = 0; i < items.Length; i++)
            {
                var node = nodes[i / VirtualNodes];
                items[i] = (Hash(node + "#" + (i % VirtualNodes).ToString(CultureInfo.InvariantCulture)), node);
            }

            Array.Sort(items, static (a, b) => a.Hash.CompareTo(b.Hash));
            return new ReferenceRing(items);
        }

        internal string OwnerOf(string cacheName, string key)
        {
            var hash = Hash(cacheName.Length.ToString(CultureInfo.InvariantCulture) + ":" + cacheName + "\u001f" + key);
            foreach (var item in _items)
            {
                if (item.Hash >= hash)
                    return item.Node;
            }

            return _items[0].Node;
        }

        private static ulong Hash(string text) => BitConverter.ToUInt64(SHA256.HashData(Encoding.UTF8.GetBytes(text)));
    }
}
