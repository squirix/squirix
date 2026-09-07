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
using Xunit;

namespace Squirix.Server.UnitTests.Node.Services;

/// <summary>Replicated mutation prepare/apply round-trips over an in-memory cache.</summary>
public sealed class ReplicaMutationTests : ServerUnitTestBase
{
    /// <summary>Outcome bytes round-trip the applied flag and the previous entry.</summary>
    [Fact]
    public void OutcomeCodecRoundTrips()
    {
        var previous = new byte[] { 1, 2, 3 };

        Assert.True(ReplicaOutcomeCodec.TryDecode(ReplicaOutcomeCodec.Encode(true, previous), out var applied, out var decoded));
        Assert.True(applied);
        Assert.True(previous.AsSpan().SequenceEqual(decoded.Span));

        Assert.True(ReplicaOutcomeCodec.TryDecode(ReplicaOutcomeCodec.Encode(false, ReadOnlyMemory<byte>.Empty), out var missed, out var empty));
        Assert.False(missed);
        Assert.True(empty.IsEmpty);

        Assert.False(ReplicaOutcomeCodec.TryDecode(new byte[] { 1, 2 }, out _, out _));
        Assert.False(ReplicaOutcomeCodec.TryDecode(new byte[] { 1, 9, 0, 0, 0 }, out _, out _));
    }

    /// <summary>Expiration removal reports whether an expiration was present.</summary>
    [Fact]
    public async Task RemoveExpirationReportsPresence()
    {
        var cache = new MemoryCache();
        var factory = new ReplicaMutationFactory(cache, "g1", 1UL);
        var plain = factory.PrepareSet("op-1", "cache", "k", new NodeCacheEntry<object?> { Value = "v1" }, 1UL);
        Assert.True(await ReplicaCacheApplier.ApplyAsync(cache, DecodeRecord(plain), DefaultCancellationToken));

        var absent = await factory.PrepareRemoveExpirationAsync("op-2", "cache", "k", 2UL, DefaultCancellationToken);
        Assert.False(await ReplicaCacheApplier.ApplyAsync(cache, DecodeRecord(absent), DefaultCancellationToken));

        var timed = factory.PrepareSet("op-3", "cache", "timed", new NodeCacheEntry<object?> { Value = "v1", ExpiresUtc = DateTime.UtcNow.AddHours(1) }, 3UL);
        Assert.True(await ReplicaCacheApplier.ApplyAsync(cache, DecodeRecord(timed), DefaultCancellationToken));

        var present = await factory.PrepareRemoveExpirationAsync("op-4", "cache", "timed", 4UL, DefaultCancellationToken);
        Assert.True(await ReplicaCacheApplier.ApplyAsync(cache, DecodeRecord(present), DefaultCancellationToken));
    }

    /// <summary>Remove returns the observed previous value, then reports missing.</summary>
    [Fact]
    public async Task RemoveReturnsPrevious()
    {
        var cache = new MemoryCache();
        var factory = new ReplicaMutationFactory(cache, "g1", 1UL);
        var prepared = factory.PrepareSet("op-1", "cache", "k", new NodeCacheEntry<object?> { Value = "v1" }, 1UL);
        Assert.True(await ReplicaCacheApplier.ApplyAsync(cache, DecodeRecord(prepared), DefaultCancellationToken));

        var mutation = await factory.PrepareRemoveAsync("op-2", "cache", "k", 2UL, DefaultCancellationToken);
        Assert.True(await ReplicaCacheApplier.ApplyAsync(cache, DecodeRecord(mutation), DefaultCancellationToken));
        Assert.True(ReplicaOutcomeCodec.TryDecode(mutation.OutcomePayload, out var removed, out var previous));
        Assert.True(removed);
        Assert.False(previous.IsEmpty);

        var missing = await factory.PrepareRemoveAsync("op-3", "cache", "k", 3UL, DefaultCancellationToken);
        Assert.False(await ReplicaCacheApplier.ApplyAsync(cache, DecodeRecord(missing), DefaultCancellationToken));
        Assert.True(ReplicaOutcomeCodec.TryDecode(missing.OutcomePayload, out var removedAgain, out _));
        Assert.False(removedAgain);
    }

    /// <summary>A prepared set applies and reads back through the record.</summary>
    [Fact]
    public async Task SetAppliesThroughRecord()
    {
        var cache = new MemoryCache();
        var factory = new ReplicaMutationFactory(cache, "g1", 1UL);
        var entry = new NodeCacheEntry<object?> { Value = "v1" };

        var mutation = factory.PrepareSet("op-1", "cache", "k", entry, 1UL);
        var record = DecodeRecord(mutation);

        Assert.True(await ReplicaCacheApplier.ApplyAsync(cache, record, DefaultCancellationToken));
        var read = await cache.GetValueAsync("cache", "k", DefaultCancellationToken);
        Assert.True(read.Found);
        Assert.Equal("v1", Assert.IsType<string>(read.Value));
    }

    /// <summary>Touch succeeds only for present keys.</summary>
    [Fact]
    public async Task TouchRequiresPresentKey()
    {
        var cache = new MemoryCache();
        var factory = new ReplicaMutationFactory(cache, "g1", 1UL);

        var missing = await factory.PrepareTouchAsync("op-1", "cache", "k", TimeSpan.FromMinutes(5), 1UL, DefaultCancellationToken);
        Assert.False(await ReplicaCacheApplier.ApplyAsync(cache, DecodeRecord(missing), DefaultCancellationToken));

        var prepared = factory.PrepareSet("op-2", "cache", "k", new NodeCacheEntry<object?> { Value = "v1" }, 2UL);
        Assert.True(await ReplicaCacheApplier.ApplyAsync(cache, DecodeRecord(prepared), DefaultCancellationToken));

        var touch = await factory.PrepareTouchAsync("op-3", "cache", "k", TimeSpan.FromMinutes(5), 3UL, DefaultCancellationToken);
        Assert.True(await ReplicaCacheApplier.ApplyAsync(cache, DecodeRecord(touch), DefaultCancellationToken));
    }

    /// <summary>A second try-add for the same key reports false with a matching outcome.</summary>
    [Fact]
    public async Task TryAddSecondFails()
    {
        var cache = new MemoryCache();
        var factory = new ReplicaMutationFactory(cache, "g1", 1UL);

        var first = await factory.PrepareTryAddAsync("op-1", "cache", "k", new NodeCacheEntry<object?> { Value = "v1" }, 1UL, DefaultCancellationToken);
        Assert.True(await ReplicaCacheApplier.ApplyAsync(cache, DecodeRecord(first), DefaultCancellationToken));
        Assert.True(ReplicaOutcomeCodec.TryDecode(first.OutcomePayload, out var firstApplied, out _));
        Assert.True(firstApplied);

        var second = await factory.PrepareTryAddAsync("op-2", "cache", "k", new NodeCacheEntry<object?> { Value = "v2" }, 2UL, DefaultCancellationToken);
        Assert.False(await ReplicaCacheApplier.ApplyAsync(cache, DecodeRecord(second), DefaultCancellationToken));
        Assert.True(ReplicaOutcomeCodec.TryDecode(second.OutcomePayload, out var secondApplied, out _));
        Assert.False(secondApplied);
    }

    /// <summary>Update replaces the value of a present key only.</summary>
    [Fact]
    public async Task UpdateRequiresPresentKey()
    {
        var cache = new MemoryCache();
        var factory = new ReplicaMutationFactory(cache, "g1", 1UL);

        var missing = await factory.PrepareUpdateAsync("op-1", "cache", "k", "v9", 1UL, DefaultCancellationToken);
        Assert.False(await ReplicaCacheApplier.ApplyAsync(cache, DecodeRecord(missing), DefaultCancellationToken));

        var prepared = factory.PrepareSet("op-2", "cache", "k", new NodeCacheEntry<object?> { Value = "v1" }, 2UL);
        Assert.True(await ReplicaCacheApplier.ApplyAsync(cache, DecodeRecord(prepared), DefaultCancellationToken));

        var update = await factory.PrepareUpdateAsync("op-3", "cache", "k", "v2", 3UL, DefaultCancellationToken);
        Assert.True(await ReplicaCacheApplier.ApplyAsync(cache, DecodeRecord(update), DefaultCancellationToken));
        var read = await cache.GetValueAsync("cache", "k", DefaultCancellationToken);
        Assert.True(read.Found);
        Assert.Equal("v2", Assert.IsType<string>(read.Value));
    }

    /// <summary>A prepare that faults while reading the local entry propagates the fault, so the reserved log index is left unconsumed for a retry.</summary>
    [Fact]
    public async Task FailedPreparePropagatesFault()
    {
        var fault = new InvalidOperationException("local read failed");
        var factory = new ReplicaMutationFactory(new FaultingCache(fault), "g1", 1UL);

        var thrown = await NodeAsyncAssert.ThrowsAsync<InvalidOperationException>(
            factory.PrepareRemoveAsync("op-1", "cache", "k", 42UL, DefaultCancellationToken));

        Assert.Same(fault, thrown);
    }

    private static ReplicaLogRecord DecodeRecord(PreparedReplicaMutation mutation)
    {
        var decoded = ReplicaLogCodec.Decode(mutation.CanonicalPayload);
        return Assert.NotNull(decoded);
    }

    /// <summary>In-memory logical cache for prepare/apply round-trips.</summary>
    private sealed class MemoryCache : ILogicalNamespacedCache<object?>
    {
        private readonly Dictionary<string, NodeCacheEntry<object?>> _entries = new(StringComparer.Ordinal);

        public ValueTask<NodeCacheEntry<object?>?> GetEntryAsync(string cacheName, string key, CancellationToken cancellationToken)
        {
            _ = cancellationToken;
            return ValueTask.FromResult(_entries.TryGetValue(Key(cacheName, key), out var entry) ? entry : null);
        }

        public ValueTask<NodeCacheValueResult<object?>> GetValueAsync(string cacheName, string key, CancellationToken cancellationToken)
        {
            _ = cancellationToken;
            return _entries.TryGetValue(Key(cacheName, key), out var entry) ? ValueTask.FromResult(new NodeCacheValueResult<object?>(true, entry.Value))
                : ValueTask.FromResult(new NodeCacheValueResult<object?>(false, null));
        }

        public ValueTask<CacheRemoveResult<object?>> RemoveAsync(string operationId, string cacheName, string key, CancellationToken cancellationToken)
        {
            _ = operationId;
            _ = cancellationToken;
            if (!_entries.Remove(Key(cacheName, key), out var previous))
                return ValueTask.FromResult(new CacheRemoveResult<object?>(false, null));

            return ValueTask.FromResult(new CacheRemoveResult<object?>(true, previous.Value));
        }

        public ValueTask<bool> RemoveExpirationAsync(string operationId, string cacheName, string key, CancellationToken cancellationToken)
        {
            _ = operationId;
            _ = cancellationToken;
            if (!_entries.TryGetValue(Key(cacheName, key), out var entry) || entry.ExpiresUtc == null)
                return ValueTask.FromResult(false);

            _entries[Key(cacheName, key)] = new NodeCacheEntry<object?> { Value = entry.Value };
            return ValueTask.FromResult(true);
        }

        public ValueTask SetEntryAsync(string operationId, string cacheName, string key, NodeCacheEntry<object?> entry, CancellationToken cancellationToken)
        {
            _ = operationId;
            _ = cancellationToken;
            _entries[Key(cacheName, key)] = entry;
            return ValueTask.CompletedTask;
        }

        public ValueTask<bool> TouchAsync(string operationId, string cacheName, string key, TimeSpan expiration, CancellationToken cancellationToken)
        {
            _ = operationId;
            _ = cancellationToken;
            if (!_entries.TryGetValue(Key(cacheName, key), out var entry))
                return ValueTask.FromResult(false);

            _entries[Key(cacheName, key)] = new NodeCacheEntry<object?> { Value = entry.Value, ExpiresUtc = DateTime.UtcNow.Add(expiration) };
            return ValueTask.FromResult(true);
        }

        public ValueTask<bool> TryAddEntryAsync(string operationId, string cacheName, string key, NodeCacheEntry<object?> entry, CancellationToken cancellationToken)
        {
            _ = operationId;
            _ = cancellationToken;
            var cacheKey = Key(cacheName, key);
            if (!_entries.TryAdd(cacheKey, entry))
                return ValueTask.FromResult(false);

            return ValueTask.FromResult(true);
        }

        public ValueTask<bool> UpdateAsync(string operationId, string cacheName, string key, object? value, CancellationToken cancellationToken)
        {
            _ = operationId;
            _ = cancellationToken;
            var cacheKey = Key(cacheName, key);
            if (!_entries.TryGetValue(cacheKey, out var entry))
                return ValueTask.FromResult(false);

            _entries[cacheKey] = new NodeCacheEntry<object?> { Value = value, ExpiresUtc = entry.ExpiresUtc };
            return ValueTask.FromResult(true);
        }

        private static string Key(string cacheName, string key) => cacheName + "\x1F" + key;
    }

    /// <summary>Logical cache whose entry reads always fault, modeling a prepare-time failure.</summary>
    private sealed class FaultingCache : ILogicalNamespacedCache<object?>
    {
        private readonly Exception _fault;

        internal FaultingCache(Exception fault)
        {
            _fault = fault;
        }

        public ValueTask<NodeCacheEntry<object?>?> GetEntryAsync(string cacheName, string key, CancellationToken cancellationToken)
            => ValueTask.FromException<NodeCacheEntry<object?>?>(_fault);

        public ValueTask<NodeCacheValueResult<object?>> GetValueAsync(string cacheName, string key, CancellationToken cancellationToken)
            => throw new NotSupportedException();

        public ValueTask<CacheRemoveResult<object?>> RemoveAsync(string operationId, string cacheName, string key, CancellationToken cancellationToken)
            => throw new NotSupportedException();

        public ValueTask<bool> RemoveExpirationAsync(string operationId, string cacheName, string key, CancellationToken cancellationToken)
            => throw new NotSupportedException();

        public ValueTask SetEntryAsync(string operationId, string cacheName, string key, NodeCacheEntry<object?> entry, CancellationToken cancellationToken)
            => throw new NotSupportedException();

        public ValueTask<bool> TouchAsync(string operationId, string cacheName, string key, TimeSpan expiration, CancellationToken cancellationToken)
            => throw new NotSupportedException();

        public ValueTask<bool> TryAddEntryAsync(string operationId, string cacheName, string key, NodeCacheEntry<object?> entry, CancellationToken cancellationToken)
            => throw new NotSupportedException();

        public ValueTask<bool> UpdateAsync(string operationId, string cacheName, string key, object? value, CancellationToken cancellationToken)
            => throw new NotSupportedException();
    }
}
