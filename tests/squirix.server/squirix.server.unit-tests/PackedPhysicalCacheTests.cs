using System;
using System.Globalization;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Core;
using Squirix.Server.LocalCache;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests;

/// <summary>Exercises the server store across sealing, slot reuse and relocation with outstanding readers.</summary>
public sealed class PackedPhysicalCacheTests
{
    /// <summary>Returned binary values remain usable across later writes, reclamation and reinsertion.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task PublishedSlicesSurviveReclamation(CancellationToken cancellationToken)
    {
        var cache = new PhysicalCache<StoredValue>(TimeProvider.System, new CacheEntrySizeEstimator<StoredValue>());
        var deadline = DateTime.MaxValue;
        var key = new CacheKey("scope", "ключ-🦊");
        cache.Set(key, new NodeCacheEntry<StoredValue>(StoredValue.FromBinary([4, 7, 9]), 12, deadline), cancellationToken);
        for (var i = 0; i < 320; i++)
            cache.Set(CacheKey.Default(i.ToString(CultureInfo.InvariantCulture)), new NodeCacheEntry<StoredValue>(StoredValue.FromInteger(i)), cancellationToken);

        var held = cache.GetEntry(key, cancellationToken)!;
        _ = await Assert.That(held.Value.IsSlice).IsTrue();
        _ = await Assert.That(held.ExpiresUtc).IsEqualTo(deadline);
        _ = await Assert.That(held.Version).IsEqualTo(12L);
        _ = await Assert.That(cache.GetValue(CacheKey.Default(key.Key), cancellationToken).Found).IsFalse();
        for (var i = 0; i < 320; i++)
            _ = cache.Remove(CacheKey.Default(i.ToString(CultureInfo.InvariantCulture)), cancellationToken);

        var large = new byte[20_000];
        large[^1] = 89;
        _ = await Assert.That(cache.Update(key, StoredValue.FromBinary(large), cancellationToken)).IsTrue();
        var largeRead = cache.GetValue(key, cancellationToken).Value;
        cache.Set(key, new NodeCacheEntry<StoredValue>(StoredValue.FromText("replacement"), 13), cancellationToken);
        _ = await Assert.That(cache.Remove(key, cancellationToken)).IsTrue();
        cache.Set(key, new NodeCacheEntry<StoredValue>(StoredValue.FromBoolean(true)), cancellationToken);
        _ = await Assert.That(held.Value.Memory.Span.SequenceEqual(new byte[] { 4, 7, 9 })).IsTrue();
        _ = await Assert.That(largeRead.Memory.Span[^1]).IsEqualTo(large[^1]);
        _ = await Assert.That(cache.GetValue(key, cancellationToken).Value.Boolean).IsTrue();
    }

    /// <summary>Two namespaces may share a key on the same page through fallback writes, relocation and slot reuse.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task SharedKeyKeepsItsNamespaceWhenSlotsMove(CancellationToken cancellationToken)
    {
        var cache = new PhysicalCache<StoredValue>(TimeProvider.System, new CacheEntrySizeEstimator<StoredValue>());
        const byte first = 1;
        const byte second = 2;
        const byte marker = 11;
        var a = new CacheKey("alpha", "same");
        var b = new CacheKey("beta", "same");
        cache.Set(a, new NodeCacheEntry<StoredValue>(StoredValue.FromBinary([first])), cancellationToken);
        cache.Set(b, new NodeCacheEntry<StoredValue>(StoredValue.FromBinary([second])), cancellationToken);
        for (var i = 0; i < 32; i++)
            cache.Set(CacheKey.Default(i.ToString(CultureInfo.InvariantCulture)), new NodeCacheEntry<StoredValue>(default), cancellationToken);

        var held = cache.GetValue(b, cancellationToken).Value;
        _ = await Assert.That(held.IsSlice).IsTrue();
        var large = new byte[32_768];
        large[0] = marker;
        cache.Set(b, new NodeCacheEntry<StoredValue>(StoredValue.FromBinary(large)), cancellationToken);
        var fallback = cache.GetValue(b, cancellationToken).Value;
        _ = await Assert.That(fallback.Memory.Length).IsEqualTo(large.Length);
        _ = await Assert.That(cache.GetValue(a, cancellationToken).Value.Memory.Span[0]).IsEqualTo(first);
        for (var i = 0; i < 128; i++)
            cache.Set(b, new NodeCacheEntry<StoredValue>(StoredValue.FromInteger(i)), cancellationToken);

        _ = await Assert.That(cache.Remove(b, cancellationToken)).IsTrue();
        var c = new CacheKey("gamma", "same");
        cache.Set(c, new NodeCacheEntry<StoredValue>(StoredValue.FromText("last")), cancellationToken);
        _ = await Assert.That(cache.GetValue(b, cancellationToken).Found).IsFalse();
        _ = await Assert.That(cache.GetValue(c, cancellationToken).Value.Memory.Span.SequenceEqual("last"u8)).IsTrue();
        _ = await Assert.That(cache.GetValue(a, cancellationToken).Value.Memory.Span[0]).IsEqualTo(first);
        _ = await Assert.That(held.Memory.Span[0]).IsEqualTo(second);
        _ = await Assert.That(fallback.Memory.Span[0]).IsEqualTo(marker);
    }
}
