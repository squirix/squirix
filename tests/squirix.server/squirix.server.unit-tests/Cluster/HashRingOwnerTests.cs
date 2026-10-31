using System;
using System.Collections.Generic;
using System.Globalization;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.Cluster;
using Squirix.Server.UnitTests.Support;
using Squirix.Server.Utils;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Cluster;

/// <summary>Tests that the hash locator agrees with an independent XXH64 ring.</summary>
[Immutable]
public sealed class HashRingOwnerTests : ServerUnitTestBase
{
    private const int LongKeyLength = 4096;

    private const int VirtualNodes = 128;

    /// <summary>Alternating cache names and keys always match an independent XXH64 ring.</summary>
    [Test]
    public async Task InterleavedLookupsMatchIndependentRing()
    {
        string[] nodes = ["n1", "n2", "n3"];
        var locator = RuntimeServiceRegistration.CreateHashLocator(nodes);
        var reference = ReferenceRing.Create(nodes);
        var keys = SampleKeys();
        for (var i = 0; i < keys.Count * 2; i++)
        {
            var (cacheName, key) = keys[i % keys.Count];
            _ = await Assert.That(locator.GetOwner(cacheName, key)).IsEqualTo(reference.OwnerOf(cacheName, key));
        }
    }

    /// <summary>The same key under another cache name matches the independent ring.</summary>
    [Test]
    public async Task OtherCacheNameMatchesIndependentRing()
    {
        string[] nodes = ["n1", "n2", "n3", "n4"];
        var locator = RuntimeServiceRegistration.CreateHashLocator(nodes);
        var reference = ReferenceRing.Create(nodes);
        for (var i = 0; i < 200; i++)
        {
            var key = "key-" + i.ToString(CultureInfo.InvariantCulture);
            _ = await Assert.That(locator.GetOwner("cache-a", key)).IsEqualTo(reference.OwnerOf("cache-a", key));
            _ = await Assert.That(locator.GetOwner("cache-b", key)).IsEqualTo(reference.OwnerOf("cache-b", key));
        }
    }

    /// <summary>Repeated lookups of the same key return a stable owner.</summary>
    [Test]
    public async Task RepeatedLookupsMatchIndependentRing()
    {
        string[] nodes = ["n1", "n2", "n3"];
        var locator = RuntimeServiceRegistration.CreateHashLocator(nodes);
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

    /// <summary>Lookup reuse cannot carry an owner from another ring instance.</summary>
    [Test]
    public async Task InterleavedRingInstancesKeepTheirOwners()
    {
        var first = RuntimeServiceRegistration.CreateHashLocator(["first"]);
        var second = RuntimeServiceRegistration.CreateHashLocator(["second"]);
        for (var i = 0; i < 10; i++)
        {
            var firstOwner = first.GetOwner("cache", "key");
            var secondOwner = second.GetOwner("cache", "key");
            var firstAgain = first.GetOwner("cache", "key");
            _ = await Assert.That(firstOwner).IsEqualTo("first");
            _ = await Assert.That(secondOwner).IsEqualTo("second");
            _ = await Assert.That(firstAgain).IsEqualTo("first");
        }
    }

    /// <summary>Equal strings with different identities still resolve through the normal lookup.</summary>
    [Test]
    public async Task EqualStringsKeepTheirOwners()
    {
        string[] nodes = ["n1", "n2", "n3"];
        var locator = RuntimeServiceRegistration.CreateHashLocator(nodes);
        var reference = ReferenceRing.Create(nodes);
        foreach (var (cacheName, key) in SampleKeys())
        {
            var expected = reference.OwnerOf(cacheName, key);
            _ = await Assert.That(locator.GetOwner(cacheName, key)).IsEqualTo(expected);
            _ = await Assert.That(locator.GetOwner(new string(cacheName.AsSpan()), new string(key.AsSpan()))).IsEqualTo(expected);
        }
    }

    /// <summary>Concurrent keys and namespaces keep matching independently calculated owners.</summary>
    [Test]
    public async Task ConcurrentLookupsMatchIndependentRing()
    {
        string[] nodes = ["n1", "n2", "n3"];
        var locator = RuntimeServiceRegistration.CreateHashLocator(nodes);
        var reference = ReferenceRing.Create(nodes);
        var keys = SampleKeys();
        var expected = new string[keys.Count];
        for (var i = 0; i < keys.Count; i++)
            expected[i] = reference.OwnerOf(keys[i].CacheName, keys[i].Key);

        var tasks = new Task[8];
        for (var worker = 0; worker < tasks.Length; worker++)
        {
            tasks[worker] = Task.Factory.StartNew(
                static state =>
                {
                    var input = state as Tuple<INodeLocator, List<(string CacheName, string Key)>, string[], int>;
                    var (sharedLocator, sharedKeys, owners, offset) = ThrowHelper.Required(input, "Lookup state is required.");
                    for (var i = 0; i < sharedKeys.Count * 4; i++)
                    {
                        var index = (i + offset) % sharedKeys.Count;
                        var (cacheName, key) = sharedKeys[index];
                        for (var repeat = 0; repeat < 3; repeat++)
                        {
                            if (!string.Equals(sharedLocator.GetOwner(cacheName, key), owners[index], StringComparison.Ordinal))
                                throw new InvalidOperationException("Concurrent ownership lookup returned another key's owner.");
                        }
                    }
                },
                Tuple.Create(locator, keys, expected, worker),
                CancellationToken.None,
                TaskCreationOptions.LongRunning,
                TaskScheduler.Default);
        }

        await Task.WhenAll(tasks).ConfigureAwait(false);
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

        private static ulong Hash(string text) => XxHash64.Hash(Encoding.UTF8.GetBytes(text));
    }
}
