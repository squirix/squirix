using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.Core;
using Squirix.Server.Node.Services;
using Squirix.Server.Runtime.Contracts;
using Squirix.Server.TestKit;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Node.Services;

/// <summary>Replicated mutation prepare/apply round-trips over an in-memory cache.</summary>
public sealed class ReplicaMutationTests : ServerUnitTestBase
{
    /// <summary>A second try-add for the same key reports false with a matching outcome.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task AddDuplicateFails(CancellationToken cancellationToken)
    {
        var cache = new MemoryCache();
        var factory = new ReplicaMutationFactory(cache, "g1", 1UL);

        var first = await factory.PrepareTryAddAsync("op-1", "cache", "k", new NodeCacheEntry<object?> { Value = "v1" }, 1UL, cancellationToken);
        _ = await Assert.That(await ReplicaCacheApplier.ApplyAsync(cache, await DecodeRecordAsync(first), cancellationToken)).IsTrue();
        _ = await Assert.That(ReplicaOutcomeCodec.TryDecode(first.OutcomePayload, out var firstApplied, out _)).IsTrue();
        _ = await Assert.That(firstApplied).IsTrue();

        var second = await factory.PrepareTryAddAsync("op-2", "cache", "k", new NodeCacheEntry<object?> { Value = "v2" }, 2UL, cancellationToken);
        _ = await Assert.That(await ReplicaCacheApplier.ApplyAsync(cache, await DecodeRecordAsync(second), cancellationToken)).IsFalse();
        _ = await Assert.That(ReplicaOutcomeCodec.TryDecode(second.OutcomePayload, out var secondApplied, out _)).IsTrue();
        _ = await Assert.That(secondApplied).IsFalse();
    }

    /// <summary>A preparing that faults while reading the local entry propagates the fault, so the reserved log index is left unconsumed for a retry.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task FailedPreparePropagatesFault(CancellationToken cancellationToken)
    {
        var fault = new InvalidOperationException("local read failed");
        var factory = new ReplicaMutationFactory(new FaultingCache(fault), "g1", 1UL);

        var thrown = await NodeAsyncAssert.ThrowsAsync<InvalidOperationException>(factory.PrepareRemoveAsync("op-1", "cache", "k", 42UL, cancellationToken));

        _ = await Assert.That(thrown).IsSameReferenceAs(fault);
    }

    /// <summary>Outcome bytes round-trip the applied flag and the previous entry.</summary>
    [Test]
    public async Task OutcomeCodecRoundTrips()
    {
        var previous = new byte[] { 1, 2, 3 };

        _ = await Assert.That(ReplicaOutcomeCodec.TryDecode(ReplicaOutcomeCodec.Encode(true, previous), out var applied, out var decoded)).IsTrue();
        _ = await Assert.That(applied).IsTrue();
        _ = await Assert.That(previous.AsSpan().SequenceEqual(decoded.Span)).IsTrue();

        _ = await Assert.That(ReplicaOutcomeCodec.TryDecode(ReplicaOutcomeCodec.Encode(false, ReadOnlyMemory<byte>.Empty), out var missed, out var empty)).IsTrue();
        _ = await Assert.That(missed).IsFalse();
        _ = await Assert.That(empty.IsEmpty).IsTrue();

        _ = await Assert.That(ReplicaOutcomeCodec.TryDecode(new byte[] { 1, 2 }, out _, out _)).IsFalse();
        _ = await Assert.That(ReplicaOutcomeCodec.TryDecode(new byte[] { 1, 9, 0, 0, 0 }, out _, out _)).IsFalse();
    }

    /// <summary>Expiration removal reports whether an expiration was present.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task RemoveExpirationReportsPresence(CancellationToken cancellationToken)
    {
        var cache = new MemoryCache();
        var factory = new ReplicaMutationFactory(cache, "g1", 1UL);
        var plain = factory.PrepareSet("op-1", "cache", "k", new NodeCacheEntry<object?> { Value = "v1" }, 1UL);
        _ = await Assert.That(await ReplicaCacheApplier.ApplyAsync(cache, await DecodeRecordAsync(plain), cancellationToken)).IsTrue();

        var absent = await factory.PrepareRemoveExpirationAsync("op-2", "cache", "k", 2UL, cancellationToken);
        _ = await Assert.That(await ReplicaCacheApplier.ApplyAsync(cache, await DecodeRecordAsync(absent), cancellationToken)).IsFalse();

        var timed = factory.PrepareSet("op-3", "cache", "timed", new NodeCacheEntry<object?> { Value = "v1", ExpiresUtc = DateTime.UtcNow.AddHours(1) }, 3UL);
        _ = await Assert.That(await ReplicaCacheApplier.ApplyAsync(cache, await DecodeRecordAsync(timed), cancellationToken)).IsTrue();

        var present = await factory.PrepareRemoveExpirationAsync("op-4", "cache", "timed", 4UL, cancellationToken);
        _ = await Assert.That(await ReplicaCacheApplier.ApplyAsync(cache, await DecodeRecordAsync(present), cancellationToken)).IsTrue();
    }

    /// <summary>Remove returns the observed previous value, then reports missing.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task RemoveReturnsPrevious(CancellationToken cancellationToken)
    {
        var cache = new MemoryCache();
        var factory = new ReplicaMutationFactory(cache, "g1", 1UL);
        var prepared = factory.PrepareSet("op-1", "cache", "k", new NodeCacheEntry<object?> { Value = "v1" }, 1UL);
        _ = await Assert.That(await ReplicaCacheApplier.ApplyAsync(cache, await DecodeRecordAsync(prepared), cancellationToken)).IsTrue();

        var mutation = await factory.PrepareRemoveAsync("op-2", "cache", "k", 2UL, cancellationToken);
        _ = await Assert.That(await ReplicaCacheApplier.ApplyAsync(cache, await DecodeRecordAsync(mutation), cancellationToken)).IsTrue();
        _ = await Assert.That(ReplicaOutcomeCodec.TryDecode(mutation.OutcomePayload, out var removed, out var previous)).IsTrue();
        _ = await Assert.That(removed).IsTrue();
        _ = await Assert.That(previous.IsEmpty).IsFalse();

        var missing = await factory.PrepareRemoveAsync("op-3", "cache", "k", 3UL, cancellationToken);
        _ = await Assert.That(await ReplicaCacheApplier.ApplyAsync(cache, await DecodeRecordAsync(missing), cancellationToken)).IsFalse();
        _ = await Assert.That(ReplicaOutcomeCodec.TryDecode(missing.OutcomePayload, out var removedAgain, out _)).IsTrue();
        _ = await Assert.That(removedAgain).IsFalse();
    }

    /// <summary>A prepared set applies and reads back through the record.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task SetAppliesThroughRecord(CancellationToken cancellationToken)
    {
        var cache = new MemoryCache();
        var factory = new ReplicaMutationFactory(cache, "g1", 1UL);
        var entry = new NodeCacheEntry<object?> { Value = "v1" };

        var mutation = factory.PrepareSet("op-1", "cache", "k", entry, 1UL);
        var record = await DecodeRecordAsync(mutation);

        _ = await Assert.That(await ReplicaCacheApplier.ApplyAsync(cache, record, cancellationToken)).IsTrue();
        var read = await cache.GetValueAsync("cache", "k", cancellationToken);
        _ = await Assert.That(read.Found).IsTrue();
        var firstValue = await Assert.That(read.Value).IsTypeOf<string>();
        _ = await Assert.That(firstValue).IsEqualTo("v1");
    }

    /// <summary>Touch succeeds only for present keys.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task TouchRequiresPresentKey(CancellationToken cancellationToken)
    {
        var cache = new MemoryCache();
        var factory = new ReplicaMutationFactory(cache, "g1", 1UL);

        var missing = await factory.PrepareTouchAsync("op-1", "cache", "k", TimeSpan.FromMinutes(5), 1UL, cancellationToken);
        _ = await Assert.That(await ReplicaCacheApplier.ApplyAsync(cache, await DecodeRecordAsync(missing), cancellationToken)).IsFalse();

        var prepared = factory.PrepareSet("op-2", "cache", "k", new NodeCacheEntry<object?> { Value = "v1" }, 2UL);
        _ = await Assert.That(await ReplicaCacheApplier.ApplyAsync(cache, await DecodeRecordAsync(prepared), cancellationToken)).IsTrue();

        var touch = await factory.PrepareTouchAsync("op-3", "cache", "k", TimeSpan.FromMinutes(5), 3UL, cancellationToken);
        _ = await Assert.That(await ReplicaCacheApplier.ApplyAsync(cache, await DecodeRecordAsync(touch), cancellationToken)).IsTrue();
    }

    /// <summary>Update replaces the value of a present key only.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task UpdateRequiresPresentKey(CancellationToken cancellationToken)
    {
        var cache = new MemoryCache();
        var factory = new ReplicaMutationFactory(cache, "g1", 1UL);

        var missing = await factory.PrepareUpdateAsync("op-1", "cache", "k", "v9", 1UL, cancellationToken);
        _ = await Assert.That(await ReplicaCacheApplier.ApplyAsync(cache, await DecodeRecordAsync(missing), cancellationToken)).IsFalse();

        var prepared = factory.PrepareSet("op-2", "cache", "k", new NodeCacheEntry<object?> { Value = "v1" }, 2UL);
        _ = await Assert.That(await ReplicaCacheApplier.ApplyAsync(cache, await DecodeRecordAsync(prepared), cancellationToken)).IsTrue();

        var update = await factory.PrepareUpdateAsync("op-3", "cache", "k", "v2", 3UL, cancellationToken);
        _ = await Assert.That(await ReplicaCacheApplier.ApplyAsync(cache, await DecodeRecordAsync(update), cancellationToken)).IsTrue();
        var read = await cache.GetValueAsync("cache", "k", cancellationToken);
        _ = await Assert.That(read.Found).IsTrue();
        var secondValue = await Assert.That(read.Value).IsTypeOf<string>();
        _ = await Assert.That(secondValue).IsEqualTo("v2");
    }

    private static async Task<ReplicaLogRecord> DecodeRecordAsync(PreparedReplicaMutation mutation)
    {
        var decoded = ReplicaLogCodec.Decode(mutation.CanonicalPayload);
        _ = await Assert.That(decoded).IsNotNull();
        return decoded.Value;
    }

    /// <summary>Logical cache whose entry reads always fault, modeling a prepare-time failure.</summary>
    private sealed class FaultingCache : ILogicalNamespacedCache<object?>
    {
        private readonly Exception _fault;

        internal FaultingCache(Exception fault)
        {
            _fault = fault;
        }

        public ValueTask<NodeCacheEntry<object?>?> GetEntryAsync(string cacheName, string key, CancellationToken cancellationToken) =>
            ValueTask.FromException<NodeCacheEntry<object?>?>(_fault);

        public ValueTask<NodeCacheValueResult<object?>> GetValueAsync(string cacheName, string key, CancellationToken cancellationToken) => throw new NotSupportedException();

        public ValueTask<CacheRemoveResult<object?>> RemoveAsync(string operationId, string cacheName, string key, CancellationToken cancellationToken) =>
            throw new NotSupportedException();

        public ValueTask<bool> RemoveExpirationAsync(string operationId, string cacheName, string key, CancellationToken cancellationToken) => throw new NotSupportedException();

        public ValueTask SetEntryAsync(string operationId, string cacheName, string key, NodeCacheEntry<object?> entry, CancellationToken cancellationToken) =>
            throw new NotSupportedException();

        public ValueTask<bool> TouchAsync(string operationId, string cacheName, string key, TimeSpan expiration, CancellationToken cancellationToken) =>
            throw new NotSupportedException();

        public ValueTask<bool> TryAddEntryAsync(string operationId, string cacheName, string key, NodeCacheEntry<object?> entry, CancellationToken cancellationToken) =>
            throw new NotSupportedException();

        public ValueTask<bool> UpdateAsync(string operationId, string cacheName, string key, object? value, CancellationToken cancellationToken) =>
            throw new NotSupportedException();
    }

    /// <summary>In-memory logical cache for prepare/apply round-trips.</summary>
    private sealed class MemoryCache : ILogicalNamespacedCache<object?>
    {
        private readonly Dictionary<string, NodeCacheEntry<object?>> _entries = [with(StringComparer.Ordinal)];

        public ValueTask<NodeCacheEntry<object?>?> GetEntryAsync(string cacheName, string key, CancellationToken cancellationToken) =>
            ValueTask.FromResult(_entries.TryGetValue(Key(cacheName, key), out var entry) ? entry : null);

        public ValueTask<NodeCacheValueResult<object?>> GetValueAsync(string cacheName, string key, CancellationToken cancellationToken)
        {
            return _entries.TryGetValue(Key(cacheName, key), out var entry) ? ValueTask.FromResult(new NodeCacheValueResult<object?>(true, entry.Value))
                : ValueTask.FromResult(new NodeCacheValueResult<object?>(false, null));
        }

        public ValueTask<CacheRemoveResult<object?>> RemoveAsync(string operationId, string cacheName, string key, CancellationToken cancellationToken)
        {
            return _entries.Remove(Key(cacheName, key), out var previous) ? ValueTask.FromResult(new CacheRemoveResult<object?>(true, previous.Value))
                : ValueTask.FromResult(new CacheRemoveResult<object?>(false, null));
        }

        public ValueTask<bool> RemoveExpirationAsync(string operationId, string cacheName, string key, CancellationToken cancellationToken)
        {
            return !_entries.TryGetValue(Key(cacheName, key), out var entry) || entry.ExpiresUtc == null ? ValueTask.FromResult(false)
                : ValueTask.FromResult(RemoveExpirationEntry(cacheName, key, entry));
        }

        public ValueTask SetEntryAsync(string operationId, string cacheName, string key, NodeCacheEntry<object?> entry, CancellationToken cancellationToken)
        {
            _entries[Key(cacheName, key)] = entry;
            return ValueTask.CompletedTask;
        }

        public ValueTask<bool> TouchAsync(string operationId, string cacheName, string key, TimeSpan expiration, CancellationToken cancellationToken)
        {
            if (!_entries.TryGetValue(Key(cacheName, key), out var entry))
                return ValueTask.FromResult(false);

            _entries[Key(cacheName, key)] = new NodeCacheEntry<object?> { Value = entry.Value, ExpiresUtc = DateTime.UtcNow.Add(expiration) };
            return ValueTask.FromResult(true);
        }

        public ValueTask<bool> TryAddEntryAsync(string operationId, string cacheName, string key, NodeCacheEntry<object?> entry, CancellationToken cancellationToken)
        {
            var cacheKey = Key(cacheName, key);
            return _entries.TryAdd(cacheKey, entry) ? ValueTask.FromResult(true) : ValueTask.FromResult(false);
        }

        public ValueTask<bool> UpdateAsync(string operationId, string cacheName, string key, object? value, CancellationToken cancellationToken)
        {
            var cacheKey = Key(cacheName, key);
            if (!_entries.TryGetValue(cacheKey, out var entry))
                return ValueTask.FromResult(false);

            _entries[cacheKey] = new NodeCacheEntry<object?> { Value = value, ExpiresUtc = entry.ExpiresUtc };
            return ValueTask.FromResult(true);
        }

        private static string Key(string cacheName, string key) => cacheName + "\x1F" + key;

        private bool RemoveExpirationEntry(string cacheName, string key, NodeCacheEntry<object?> entry)
        {
            _entries[Key(cacheName, key)] = new NodeCacheEntry<object?> { Value = entry.Value };
            return true;
        }
    }
}
