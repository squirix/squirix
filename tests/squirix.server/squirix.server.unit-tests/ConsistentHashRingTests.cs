using System;
using System.Linq;
using Squirix.Server.Cluster;
using Squirix.Server.TestKit.Utils;
using Xunit;

namespace Squirix.Server.UnitTests;

/// <summary>
/// Unit tests for <see cref="ConsistentHashRing" />.
/// These tests validate determinism, replica selection, distribution balance,
/// and minimal key movement guarantees when nodes are added.
/// </summary>
public sealed class ConsistentHashRingTests
{
    /// <summary>
    /// Verifies cache route owner lookup does not materialize a route-key string on the steady hot path.
    /// </summary>
    [Fact]
    public void CacheRouteComponentOwnerLookupDoesNotAllocateRouteKeyString()
    {
        var ring = new ConsistentHashRing(["A", "B", "C"], 64);
        const string key = "customer:42:cart:active";

        for (var i = 0; i < 256; i++)
            _ = ring.GetOwner("orders", key);

        var allocated = AllocationTestHelper.MeasureAllocatedBytes(() =>
        {
            var hits = 0;
            for (var i = 0; i < 10_000; i++)
            {
                if (ring.GetOwner("orders", key).Length > 0)
                    hits++;
            }

            if (hits != 10_000)
                throw new InvalidOperationException("Unreachable: expected 10_000 owner hits.");
        });
        Assert.Equal(0, allocated);
    }

    /// <summary>
    /// Verifies allocation-light cache route lookups preserve the materialized route-key ownership contract.
    /// </summary>
    /// <param name="cacheName">The cache name to route.</param>
    /// <param name="key">The user key to route.</param>
    [Theory]
    [InlineData(null, "order:42")]
    [InlineData("", "order:42")]
    [InlineData("   ", "order:42")]
    [InlineData("default", "order:42")]
    [InlineData("orders", "order:42")]
    [InlineData("orders", "order:0042")]
    [InlineData("orders", "prefix\u001Fseparator")]
    [InlineData("orders-v2", "customer:42:cart:active")]
    public void CacheRouteComponentOwnerMatchesMaterializedRouteKey(string? cacheName, string key)
    {
        var rings = new[]
        {
            new ConsistentHashRing(["A"], 8),
            new ConsistentHashRing(["A", "B", "C"], 64),
            new ConsistentHashRing(["node-1", "node-2", "node-3", "node-4"], 127),
        };

        foreach (var ring in rings)
        {
            var canonicalCacheName = CanonicalCacheNameForRouting(cacheName);
            var routeKey = $"{canonicalCacheName.Length}:{canonicalCacheName}{'\x1F'}{key}";

            Assert.Equal(ring.GetOwner(routeKey), ring.GetOwner(canonicalCacheName, key));
        }
    }

    /// <summary>
    /// Verifies cache routing callers must hash the canonical route key rather than the raw user key.
    /// </summary>
    [Fact]
    public void CacheRouteKeyCanSelectDifferentOwnerThanRawUserKey()
    {
        const string canonicalCacheName = "default";
        var ring = new ConsistentHashRing(["A", "B", "C"], 64);
        var foundDifferentOwner = false;

        for (var i = 0; i < 10_000; i++)
        {
            var key = $"route-key:{i}";
            var routeKey = $"{canonicalCacheName.Length}:{canonicalCacheName}{'\x1F'}{key}";
            if (string.Equals(ring.GetOwner(key), ring.GetOwner(routeKey), StringComparison.OrdinalIgnoreCase))
                continue;

            foundDifferentOwner = true;
            break;
        }

        Assert.True(foundDifferentOwner, "Raw user-key routing can disagree with canonical cache route-key routing.");
    }

    /// <summary>
    /// Hashing a key must deterministically return the same owner
    /// on repeated calls with the same ring configuration.
    /// </summary>
    [Fact]
    public void DeterministicOwnerTest()
    {
        var ring = new ConsistentHashRing(["A", "B", "C"], 64);
        const string key = "user:42";
        var owner1 = ring.GetOwner(key);
        var owner2 = ring.GetOwner(key);
        Assert.Equal(owner1, owner2);
    }

    /// <summary>
    /// Keys should be approximately evenly distributed across nodes.
    /// The deviation from perfect balance should remain below 20%.
    /// </summary>
    [Fact]
    public void DistributionBalanceTest()
    {
        var nodes = new[] { "A", "B", "C" };
        var ring = new ConsistentHashRing(nodes);

        var counts = nodes.ToDictionary(static n => n, static _ => 0, StringComparer.OrdinalIgnoreCase);
        const int keys = 20_000;

        for (var i = 0; i < keys; i++)
            counts[ring.GetOwner($"key:{i}")]++;

        var expected = keys / (double)nodes.Length;
        var maxDev = counts.Values.Max(c => Math.Abs(c - expected) / expected);

        Assert.True(maxDev <= 0.20, $"Distribution deviation too high: {maxDev:P1}");
    }

    /// <summary>
    /// Adding a new node should only move a minimal fraction of keys
    /// (ideally ~1/(n+1)). The test allows up to 35% movement
    /// when adding a fourth node to a 3-node ring.
    /// </summary>
    [Fact]
    public void MinimalMovementOnNodeAddTest()
    {
        var baseNodes = new[] { "A", "B", "C" };
        var ring1 = new ConsistentHashRing(baseNodes);
        var ring2 = new ConsistentHashRing(baseNodes.Append("D"));

        const int keys = 50_000;
        var moved = 0;

        for (var i = 0; i < keys; i++)
        {
            var k = $"k:{i}";
            if (!string.Equals(ring1.GetOwner(k), ring2.GetOwner(k), StringComparison.OrdinalIgnoreCase))
                moved++;
        }

        var ratio = moved / (double)keys;

        // In theory ~1/(n+1) ≈ 25% for adding a 4th node; allow up to 35% to be safe.
        Assert.True(ratio <= 0.35, $"Too many keys moved: {ratio:P1}");
    }

    /// <summary>
    /// With a single node in the ring, it must always be returned as the owner
    /// for any key, regardless of replicas or hash distribution.
    /// </summary>
    [Fact]
    public void SingleNodeOwnerTest()
    {
        var ring = new ConsistentHashRing(["A"], 8);
        Assert.Equal("A", ring.GetOwner("any"));
    }

    private static string CanonicalCacheNameForRouting(string? cacheName) => string.IsNullOrWhiteSpace(cacheName) ? "default" : cacheName;
}
