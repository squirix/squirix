using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.Core;
using Squirix.Server.Utils;

namespace Squirix.Server.LocalCache;

/// <summary>In-memory cache store (KV and expiration).</summary>
/// <typeparam name="T">The stored value type.</typeparam>
/// <remarks>
/// Mutations serialize under the cache lock. Reads use a sequence lock and retry under the cache lock on contention or local expiry.
/// A live read records access on the observed node. LRU publishes a timestamp, and LFU unlinks the first hit from the frequency-one
/// candidates under the scope's own gate. Raw reads and snapshot enumeration do not affect policy order.
/// Each namespace and original owner has its own index. Committed-record caches only apply removals selected by the leader.
/// </remarks>
[ThreadSafe]
internal sealed class PhysicalCache<T> : ILocalCache<T>, ILocalCacheSnapshotReader<T>
{
    private readonly DeadlineIndex _deadlines;
    private readonly CacheEviction<T> _eviction;
    private readonly CacheExpiryAuthority _expiry;
    private readonly Footprint _footprint;
    private readonly Lock _lock = new();
    private readonly PhysicalCacheReader _reader;
    private readonly SlotStore<T> _store = new();
    private readonly TimeProvider _timeProvider;

    /// <summary>Initializes a new instance of the <see cref="PhysicalCache{T}" /> class.</summary>
    /// <param name="timeProvider">The node clock.</param>
    /// <param name="estimator">Estimates the bytes of an entry; the cache records the estimate of every entry it stores.</param>
    /// <param name="eviction">The eviction options; unbounded LRU when <see langword="null" />.</param>
    /// <param name="expiry">
    /// Who decides expiry. <see cref="CacheExpiryAuthority.LocalClock" /> treats an entry past its deadline as absent;
    /// <see cref="CacheExpiryAuthority.CommittedRecords" /> keeps it present until a committed record removes it.
    /// </param>
    /// <param name="groupForKey">Original replication owner of a key for bounded caches.</param>
    internal PhysicalCache(
        TimeProvider timeProvider,
        ICacheEntrySizeEstimator<T> estimator,
        EvictionOptions? eviction = null,
        CacheExpiryAuthority expiry = CacheExpiryAuthority.LocalClock,
        Func<CacheKey, string>? groupForKey = null)
    {
        ArgumentNullException.ThrowIfNull(estimator);
        _timeProvider = timeProvider;
        var options = eviction ?? new EvictionOptions { Policy = EvictionPolicyType.Lru };
        _footprint = new Footprint(CacheEntrySizeEstimator<T>.ForEviction(estimator, options));
        _eviction = new CacheEviction<T>(options, expiry, groupForKey, _store);
        _expiry = expiry;
        _deadlines = new DeadlineIndex(this);
        _reader = new PhysicalCacheReader(this);
    }

    int ILocalCacheStats.EntryCount
    {
        get
        {
            lock (_lock)
                return _store.Count;
        }
    }

    long ILocalCacheStats.EstimatedBytes => _footprint.Read();

    /// <summary>Gets the keys past their deadline that no committed record removed yet, read from the deadline index.</summary>
    internal ILocalCacheExpiredKeys ExpiredKeys => _deadlines;

    internal ILocalCacheRawReader<T> RawReader => _reader;

    internal bool HasCapacity => _eviction.HasCapacity;

    private DateTime UtcNow => _timeProvider.GetUtcNow().UtcDateTime;

    public async IAsyncEnumerable<(CacheKey Key, NodeCacheEntry<T> Entry)> EnumerateLiveAsync([EnumeratorCancellation] CancellationToken cancellationToken)
    {
        const int yieldEvery = 256;

        // Snapshot the key set under the lock, then look up one key at a time, without the lock unless
        // a writer gets in the way. This never holds _lock across a `yield return` or an `await`, and the
        // lookup deliberately does not touch eviction order or frequency, nor remove an expired entry: a
        // snapshot read must not reorder LRU, inflate LFU counts or change the store for entries it merely enumerates.
        CacheKey[] keys;
        lock (_lock)
            keys = _store.CopyKeysLocked();

        var produced = 0;
        foreach (var key in keys)
        {
            cancellationToken.ThrowIfCancellationRequested();

            if (!_reader.TryRead(key, false, out var read) || IsExpired(read.ExpiresUtc))
                continue;

            yield return (key, read.ToEntry());
            produced++;
            if (produced % yieldEvery == 0)
                await Task.Yield();
        }
    }

    public NodeCacheEntry<T>? GetEntry(CacheKey key, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        return _reader.TryRead(key, true, out var read) ? read.ToEntry() : null;
    }

    async IAsyncEnumerable<CacheKey> ILocalCacheSnapshotReader<T>.EnumerateLiveKeysAsync([EnumeratorCancellation] CancellationToken cancellationToken)
    {
        const int yieldEvery = 256;
        CacheKey[] keys;
        lock (_lock)
            keys = _store.CopyKeysLocked();

        var produced = 0;
        foreach (var key in keys)
        {
            cancellationToken.ThrowIfCancellationRequested();

            if (!_reader.TryRead(key, false, out var read) || IsExpired(read.ExpiresUtc))
                continue;

            yield return key;
            produced++;
            if (produced % yieldEvery == 0)
                await Task.Yield();
        }
    }

    public NodeCacheValueResult<T> GetValue(CacheKey key, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        return _reader.TryRead(key, true, out var read) ? new NodeCacheValueResult<T>(true, read.Value) : new NodeCacheValueResult<T>(false, default);
    }

    NodeCacheValueRead<T> ILocalCacheReadOperations<T>.GetValueWithDeadline(CacheKey key, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        return _reader.TryRead(key, true, out var read) ? new NodeCacheValueRead<T>(true, read.Value, read.ExpiresUtc) : default;
    }

    public ValueTask InsertRecoveryAsync(CacheKey key, NodeCacheEntry<T> entry, CancellationToken cancellationToken, int? encodedLength = null)
    {
        Set(key, entry, cancellationToken, encodedLength);
        return ValueTask.CompletedTask;
    }

    public bool Remove(CacheKey key, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        lock (_lock)
            return RemoveLocked(key);
    }

    public bool RemoveExpiration(CacheKey key, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        lock (_lock)
        {
            _store.AdvanceGrowthLocked();
            if (!TryGetLiveLocked(key, false, out var node))
                return false;

            var previous = node.ExpiresUtc;
            if (previous != null)
            {
                _footprint.ResizeForDeadline(key, in node, null);
                node.WriteExpiresUtc(null);
                _deadlines.MovedLocked(in node, previous);
            }

            node.RecordAccess();
            return previous != null;
        }
    }

    public ValueTask<bool> RemoveRecoveryAsync(CacheKey key, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        lock (_lock)
            return ValueTask.FromResult(RemoveLocked(key));
    }

    public void Set(CacheKey key, NodeCacheEntry<T> entry, CancellationToken cancellationToken, int? encodedLength = null)
    {
        cancellationToken.ThrowIfCancellationRequested();
        var (normalized, bytes) = _footprint.PrepareInsert(key, entry, encodedLength, _timeProvider);
        lock (_lock)
            _ = UpsertLocked(key, normalized, bytes, false);
    }

    public bool Touch(CacheKey key, TimeSpan expiration, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        lock (_lock)
        {
            _store.AdvanceGrowthLocked();
            if (!TryGetLiveLocked(key, false, out var node))
                return false;

            var expiresUtc = UtcNow.SaturatedAdd(expiration);
            if (node.ExpiresUtc == null)
                _footprint.ResizeForDeadline(key, in node, expiresUtc);

            var previous = node.ExpiresUtc;
            node.WriteExpiresUtc(expiresUtc);
            _deadlines.MovedLocked(in node, previous);
            node.RecordAccess();
            return true;
        }
    }

    public bool TryAdd(CacheKey key, NodeCacheEntry<T> entry, CancellationToken cancellationToken, int? encodedLength = null)
    {
        cancellationToken.ThrowIfCancellationRequested();
        var (normalized, bytes) = _footprint.PrepareInsert(key, entry, encodedLength, _timeProvider);
        lock (_lock)
            return UpsertLocked(key, normalized, bytes, true);
    }

    public bool Update(CacheKey key, T? value, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        var measuredValue = _footprint.MeasureValue(value);
        lock (_lock)
        {
            _store.AdvanceGrowthLocked();
            if (!TryGetLiveLocked(key, false, out var node))
                return false;

            var bytes = _footprint.Estimate(key, new NodeCacheEntry<T>(value, node.Version, node.ExpiresUtc, tags: node.Tags), measuredValue);
            node.WriteValue(key, value);
            _footprint.Resize(in node, bytes);
            node.RecordAccess();
            return true;
        }
    }

    /// <summary>Checks whether an observed live entry already covers the owner's current sliding deadline.</summary>
    /// <param name="entry">The immutable entry observed by the read path.</param>
    /// <returns>Whether the access can return without renewing an unexpired entry.</returns>
    internal bool CanReadWithoutRenewal(NodeCacheEntry<T> entry)
    {
        var (_, renewTo, expired) = DistributedCacheAccess.Decide(entry, UtcNow);
        return !expired && renewTo == null;
    }

    /// <summary>Observes and renews an adapter value under the same lock as local writes and removals.</summary>
    /// <param name="key">The adapter key.</param>
    /// <param name="refresh">Whether to omit the value from the result.</param>
    /// <param name="cancellationToken">Cancellation token before taking the cache lock.</param>
    /// <returns>The observed payload when requested and present.</returns>
    internal ReadOnlyMemory<byte>? AccessDistributed(CacheKey key, bool refresh, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        lock (_lock)
        {
            if (!TryGetLiveLocked(key, false, out var node))
                return null;

            var (payload, renewTo, expired) = DistributedCacheAccess.Decide(new NodeCacheEntry<T>(node.Value, expiresUtc: node.ExpiresUtc), UtcNow);
            if (expired)
            {
                _ = RemoveLocked(key);
                return null;
            }

            if (renewTo is { } deadline)
            {
                _store.AdvanceGrowthLocked();
                var previous = node.ExpiresUtc;
                node.WriteExpiresUtc(deadline);
                _deadlines.MovedLocked(in node, previous);
            }

            node.RecordAccess();
            return refresh ? null : payload;
        }
    }

    internal CacheKey? SelectCapacityVictim(CacheKey key)
    {
        lock (_lock)
            return _eviction.SelectCapacityVictimLocked(key);
    }

    internal CacheKey? SelectPressureVictim(string group)
    {
        lock (_lock)
            return _eviction.SelectPressureVictimLocked(group);
    }

    /// <summary>Tells whether this node treats an entry as gone because its deadline passed on the node clock.</summary>
    /// <param name="expiresUtc">The deadline of the stored entry, or <see langword="null" /> for none.</param>
    /// <returns><see langword="false" /> under <see cref="CacheExpiryAuthority.CommittedRecords" />, where only a committed record removes an entry.</returns>
    private bool IsExpired(DateTime? expiresUtc) => _expiry == CacheExpiryAuthority.LocalClock && expiresUtc is { } expires && expires <= UtcNow;

    private bool RemoveLocked(CacheKey key)
    {
        _store.AdvanceGrowthLocked();
        ref var link = ref _store.FindLinkLocked(key, key.GetHashCode(), 0, out var node);
        if (!node.Exists)
            return false;

        var expired = IsExpired(node.ExpiresUtc);
        _store.UnlinkLocked(ref link, in node);
        UntrackLocked(in node);
        return !expired;
    }

    private bool TryGetLiveLocked(CacheKey key, bool touchOrder, out CacheSlot<T> node)
    {
        if (!_store.TryGetLocked(key, out node))
            return false;

        if (IsExpired(node.ExpiresUtc))
        {
            _store.RemoveLocked(in node);
            UntrackLocked(in node);
            node = default;
            return false;
        }

        if (touchOrder)
            node.RecordAccess();

        return true;
    }

    /// <summary>Releases an entry that left the store: its recorded bytes leave the total, it leaves the eviction order, and it drops its value and tags.</summary>
    /// <param name="node">The entry already removed from the store.</param>
    private void UntrackLocked(in CacheSlot<T> node)
    {
        _footprint.Release(in node);

        if (node.Policy is { } bounded)
            _eviction.Remove(in node, bounded);

        node.MarkRemoved();
    }

    private bool UpsertLocked(CacheKey key, NodeCacheEntry<T> normalized, long bytes, bool insertOnly)
    {
        _store.AdvanceGrowthLocked();
        var hash = key.GetHashCode();
        _ = _store.TryGetLocked(key, hash, out var node);
        if (node.Exists && insertOnly && IsExpired(node.ExpiresUtc))
        {
            _store.RemoveLocked(in node);
            UntrackLocked(in node);
            node = default;
        }

        if (!node.Exists)
        {
            if (_eviction.Enabled)
            {
                node = _eviction.Add(key, hash, normalized, bytes, out var victim);
                if (victim.Exists)
                    UntrackLocked(in victim);
            }
            else
            {
                node = _store.AddLocked(key, hash, normalized, null, bytes);
            }

            if (!node.Exists)
                return true;

            _footprint.Add(bytes);
            _deadlines.RecordLocked(in node);

            return true;
        }

        if (insertOnly)
            return false;

        var previous = node.ExpiresUtc;
        node.Write(key, normalized.Value, normalized.Version, normalized.ExpiresUtc, normalized.Tags);
        _footprint.Resize(in node, bytes);
        _deadlines.MovedLocked(in node, previous);
        node.RecordAccess();
        return false;
    }

    /// <summary>The recorded bytes of the stored entries and their running total.</summary>
    /// <remarks>
    /// <see cref="Resize" /> and <see cref="Release" /> run only under the cache lock, so the total always equals the sum of
    /// <see cref="CacheSlot{T}.Bytes" /> over the stored entries; <see cref="Read" /> needs no lock. Update measures its value before the lock and
    /// combines the measurement with the entry metadata under it. Deadline changes reuse the stored estimate when only the deadline share changes.
    /// </remarks>
    [ThreadSafe]
    private sealed class Footprint
    {
        private readonly ICacheEntrySizeEstimator<T> _estimator;
        private long _total;

        internal Footprint(ICacheEntrySizeEstimator<T> estimator)
        {
            _estimator = estimator;
        }

        internal long Estimate(CacheKey key, NodeCacheEntry<T> entry, long measuredValue) => _estimator.EstimateBytesForMeasuredValue(key, entry, measuredValue);

        internal long MeasureValue(T? value) => _estimator.MeasureValue(value);

        /// <summary>Resolves the deadline of an entry about to be stored and estimates its bytes, before the cache lock is taken.</summary>
        /// <param name="key">The key the entry is stored under.</param>
        /// <param name="candidate">The entry to store.</param>
        /// <param name="knownEncodedLength">
        /// The journal-encoded length of <paramref name="candidate" />, or <see langword="null" /> to measure. It is used only for an entry without a relative
        /// expiration: resolving a relative expiration adds a deadline the length may not cover, so that entry is measured.
        /// </param>
        /// <param name="clock">The node clock for relative deadlines.</param>
        /// <returns>The entry to store and its estimated bytes; the estimator throws before the cache is touched.</returns>
        internal (NodeCacheEntry<T> Entry, long Bytes) PrepareInsert(CacheKey key, NodeCacheEntry<T> candidate, int? knownEncodedLength, TimeProvider clock)
        {
            if (candidate.PendingWrite is { } write)
                candidate = write.Materialize<T>(clock.GetUtcNow().UtcDateTime);

            candidate = Normalize(candidate, clock);
            var bytes = knownEncodedLength is { } length && candidate.Expiration == null
                ? _estimator.EstimateBytesForEncodedLength(key, candidate, length)
                : _estimator.EstimateBytes(key, candidate, false);
            return (candidate, bytes);
        }

        internal long Read() => Volatile.Read(ref _total);

        internal void Release(in CacheSlot<T> node) => Add(-node.Bytes);

        internal void Add(long bytes) => Volatile.Write(ref _total, _total + bytes);

        internal void ResizeForDeadline(CacheKey key, in CacheSlot<T> node, DateTime? expiresUtc)
        {
            if (_estimator is CacheEntrySizeEstimator<T>)
            {
                const int deadlineBytes = CacheEntryDeadlineSize.IndexBytes;
                Resize(in node, node.Bytes + (expiresUtc != null ? deadlineBytes : -deadlineBytes));
                return;
            }

            Resize(in node, _estimator.EstimateBytes(key, new NodeCacheEntry<T>(node.Value, node.Version, expiresUtc, tags: node.Tags), false));
        }

        internal void Resize(in CacheSlot<T> node, long bytes)
        {
            var previous = node.Bytes;
            node.Bytes = bytes;
            Add(bytes - previous);
        }

        private static NodeCacheEntry<T> Normalize(NodeCacheEntry<T> entry, TimeProvider clock) =>
            entry is { Version: > 0, Expiration: null }
                ? entry
                : new NodeCacheEntry<T>(entry.Value, Math.Max(1, entry.Version), ResolveDeadline(entry, clock), entry.Expiration, entry.Tags);

        private static DateTime? ResolveDeadline(NodeCacheEntry<T> entry, TimeProvider clock)
        {
            if (entry.Expiration is not { } ttl)
                return entry.ExpiresUtc;
            var relative = clock.GetUtcNow().UtcDateTime.SaturatedAdd(ttl);
            return entry.ExpiresUtc is { } absolute && absolute <= relative ? absolute : relative;
        }
    }

    /// <summary>The deadline index: a heap of deadlines, earliest first, apart from skipped keys, with one canonical item per entry, at or before its deadline.</summary>
    /// <remarks>
    /// Under <see cref="CacheExpiryAuthority.CommittedRecords" /> an entry past its deadline stays until a committed record removes it, so the
    /// index only lists it. Under <see cref="CacheExpiryAuthority.LocalClock" /> the local expiry sweep removes it. The heap is lazy: the item of
    /// an entry, canonical when its generation equals <see cref="CacheSlot{T}.IndexedGeneration" />, is pushed when the entry is stored and when its deadline appears
    /// or moves before that, never when it moves later. When an item reaches the top, it is dropped when its entry left the store, was replaced or
    /// has another canonical item; it is dropped when the entry lost its deadline; it is put back with the later deadline of its entry; otherwise
    /// the entry is due. The heap is rebuilt from the store once it holds <c language="csharp">2 * store.Count + 1024</c> items. Every member but <see cref="ListExpired" /> and <see cref="RemoveExpired" /> runs under the cache lock.
    /// </remarks>
    [ThreadSafe]
    private sealed class DeadlineIndex : ILocalCacheExpiredKeys
    {
        private const int RemoveBatchSize = 1024;

        private readonly PriorityQueue<Deadline, long> _heap = new();
        private readonly PhysicalCache<T> _owner;

        internal DeadlineIndex(PhysicalCache<T> owner)
        {
            _owner = owner;
        }

        /// <inheritdoc />
        /// <remarks>
        /// Pops the heap while its top is due, at most four items per key asked for, so the work under the cache lock and the allocations follow
        /// the expired entries, not the stored ones. A stale item is dropped, and one whose entry has a later deadline is put back with it; both
        /// count toward the four. Every current item is pushed back, listed or not, since only a
        /// committed record removes its entry. A listed item keeps its place. A skipped one goes behind every item due before this call, so
        /// keys the caller skips, such as those of a group whose leader is away, cannot hold back the keys it can expire.
        /// </remarks>
        public IReadOnlyList<CacheKey> ListExpired(DateTime nowUtc, int max, Func<CacheKey, bool> include)
        {
            ArgumentOutOfRangeException.ThrowIfNegativeOrZero(max);
            ArgumentNullException.ThrowIfNull(include);
            if (_owner._expiry != CacheExpiryAuthority.CommittedRecords)
                return [];

            lock (_owner._lock)
                return ListDueLocked(_heap, nowUtc.Ticks, max, include);
        }

        /// <inheritdoc />
        /// <remarks>Pops the due items in batches of <see cref="RemoveBatchSize" /> per cache lock hold, so a large backlog cannot stall writers.</remarks>
        public int RemoveExpired()
        {
            if (_owner._expiry != CacheExpiryAuthority.LocalClock)
                return 0;

            var removed = 0;
            int examined;
            do
            {
                lock (_owner._lock)
                {
                    (examined, var batchRemoved) = RemoveDueLocked(_owner.UtcNow.Ticks, RemoveBatchSize);
                    removed += batchRemoved;
                }
            }
            while (examined == RemoveBatchSize);

            return removed;
        }

        /// <summary>Records the deadline just set on a stored entry only when it appeared or moved earlier: a later deadline needs no new item.</summary>
        /// <param name="node">The entry, carrying its new deadline.</param>
        /// <param name="previous">The deadline the entry had before, or <see langword="null" /> for none.</param>
        internal void MovedLocked(in CacheSlot<T> node, DateTime? previous)
        {
            var expiresUtc = node.ExpiresUtc;
            if (previous == expiresUtc)
                return;

            // Until a deadline diverges, the previous entry deadline is also the date in the heap.
            var saved = _owner._store.DeferredDeadline(node.Slot);
            var scheduled = saved ?? previous?.Ticks;
            if (expiresUtc is { } moved && (scheduled == null || moved.Ticks < scheduled))
                RecordLocked(in node);
            else if (saved == null && scheduled != null)
                _owner._store.SetDeferredDeadline(node.Slot, scheduled);
        }

        /// <summary>Records the deadline just set on an entry, rebuilding the heap once stale items outnumber the entries.</summary>
        /// <param name="node">The entry; one without a deadline is not recorded.</param>
        internal void RecordLocked(in CacheSlot<T> node)
        {
            if (node.ExpiresUtc is not { } deadline)
                return;

            var store = _owner._store;
            if (_heap.Count < (2 * store.Count) + 1024)
            {
                Index(in node, deadline.Ticks);
                return;
            }

            // The rebuild reads the current deadline of every entry, this one included.
            _heap.Clear();
            _heap.EnqueueRange(CurrentDeadlines());
        }

        /// <summary>Pops the top item when it is due.</summary>
        /// <param name="heap">The heap.</param>
        /// <param name="nowTicks">The current time, in UTC ticks.</param>
        /// <param name="item">The item popped.</param>
        /// <param name="priority">The priority it had.</param>
        /// <returns><see langword="false" /> when the heap is empty or its top is not due.</returns>
        private static bool TryPopDue(PriorityQueue<Deadline, long> heap, long nowTicks, out Deadline item, out long priority)
        {
            if (!heap.TryPeek(out item, out priority) || priority > nowTicks)
                return false;

            _ = heap.Dequeue();
            return true;
        }

        private IEnumerable<(Deadline Element, long Priority)> CurrentDeadlines()
        {
            var nodes = _owner._store.GetEnumerator();
            while (nodes.MoveNext())
            {
                var stored = nodes.Current;
                stored.IndexedGeneration++;
                _owner._store.SetDeferredDeadline(stored.Slot, null);
                if (stored.ExpiresUtc is { } expires)
                    yield return (new Deadline(stored.Identity, expires.Ticks, stored.Slot, stored.IndexedGeneration), expires.Ticks);
            }
        }

        /// <summary>Pushes the canonical item of an entry, which makes every other item of the entry stale.</summary>
        /// <param name="node">The entry.</param>
        /// <param name="ticks">The deadline of the item, in UTC ticks.</param>
        private void Index(in CacheSlot<T> node, long ticks)
        {
            _owner._store.SetDeferredDeadline(node.Slot, null);
            node.IndexedGeneration++;
            _heap.Enqueue(new Deadline(node.Identity, ticks, node.Slot, node.IndexedGeneration), ticks);
        }

        /// <summary>Tells what a popped item stands for, putting it back when its entry has a later deadline.</summary>
        /// <param name="item">The item popped.</param>
        /// <param name="node">The current occupant when the item is due.</param>
        /// <returns>
        /// <see langword="true" /> when the item names a stored entry with the deadline it recorded; <see langword="false" /> when it was dropped as
        /// stale or put back with the later deadline of its entry.
        /// </returns>
        private bool TryResolve(in Deadline item, out CacheSlot<T> node)
        {
            node = _owner._store.ResolveSlotLocked(item.Slot, item.Identity);
            if (!node.Exists || node.IndexedGeneration != item.Generation)
                return false;

            if (node.ExpiresUtc is not { } expires)
            {
                _owner._store.SetDeferredDeadline(node.Slot, null);
                node.IndexedGeneration++;
                return false;
            }

            if (expires.Ticks <= item.Ticks)
                return true;

            Index(in node, expires.Ticks);
            return false;
        }

        /// <summary>Removes the entries whose deadline passed, examining at most a batch of heap items.</summary>
        /// <param name="nowTicks">The current time, in UTC ticks.</param>
        /// <param name="max">The most heap items to examine.</param>
        /// <returns>The number of heap items examined and the number of entries removed.</returns>
        private (int Examined, int Removed) RemoveDueLocked(long nowTicks, int max)
        {
            var examined = 0;
            var removed = 0;
            while (examined < max && TryPopDue(_heap, nowTicks, out var item, out _))
            {
                examined++;
                if (!TryResolve(in item, out var node))
                    continue;

                _owner._store.RemoveLocked(in node);
                _owner.UntrackLocked(in node);
                removed++;
            }

            return (examined, removed);
        }

        /// <summary>Lists the due keys the caller includes and pushes every due item that is still current back, as <see cref="ListExpired" /> tells.</summary>
        /// <param name="heap">The heap.</param>
        /// <param name="nowTicks">The current time, in UTC ticks.</param>
        /// <param name="max">The most keys to list.</param>
        /// <param name="include">Tells whether the caller can expire a key.</param>
        /// <returns>The keys listed.</returns>
        private List<CacheKey> ListDueLocked(PriorityQueue<Deadline, long> heap, long nowTicks, int max, Func<CacheKey, bool> include)
        {
            var budget = Math.Min(max, int.MaxValue / 4) * 4;
            var listed = new List<CacheKey>();
            List<(Deadline Item, long Priority)>? due = null;
            for (var popped = 0; popped < budget && listed.Count < max && TryPopDue(heap, nowTicks, out var item, out var priority); popped++)
            {
                if (!TryResolve(in item, out var node))
                    continue;

                var key = node.Key;
                var included = include(key);
                if (included)
                    listed.Add(key);

                // A skipped item goes behind every item due before this listing.
                (due ??= []).Add((item, included ? priority : nowTicks));
            }

            if (due == null)
                return listed;

            foreach (var (item, priority) in due)
                heap.Enqueue(item, priority);

            return listed;
        }

        /// <summary>An item of the heap: the entry a deadline was recorded for, and the deadline then.</summary>
        /// <param name="Identity">Identity allocated once per insertion across all pages in this store.</param>
        /// <param name="Ticks">The deadline recorded, in UTC ticks; a later deadline on the entry moves the item.</param>
        /// <param name="Slot">Logical slot number. A stale deadline must not keep its former page alive.</param>
        /// <param name="Generation">The generation of the entry when the item was pushed; the item is stale unless it equals <see cref="CacheSlot{T}.IndexedGeneration" />.</param>
        [Immutable]
        [StructLayout(LayoutKind.Auto)]
        private readonly record struct Deadline(long Identity, long Ticks, int Slot, int Generation);
    }

    [Immutable]
    private sealed class PhysicalCacheReader : ILocalCacheRawReader<T>
    {
        private readonly PhysicalCache<T> _owner;

        internal PhysicalCacheReader(PhysicalCache<T> owner)
        {
            _owner = owner;
        }

        public NodeCacheEntry<T>? GetEntryRaw(CacheKey key, CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();
            return TryRead(key, false, out var read) ? read.ToEntry() : null;
        }

        /// <summary>Reads the entry of a key, without the cache lock unless a writer gets in the way or the read must change the cache.</summary>
        /// <param name="key">The key to read.</param>
        /// <param name="live">
        /// <see langword="true" /> to read only a live entry, which counts as an access for the eviction policy and removes an entry past its deadline;
        /// <see langword="false" /> to read the stored entry, past its deadline or not, without changing the cache.
        /// </param>
        /// <param name="read">The fields of the entry.</param>
        /// <returns><see langword="true" /> when the key holds an entry to return.</returns>
        /// <remarks>Policy tracking uses the observed node. Reinserting the same key creates a different node.</remarks>
        internal bool TryRead(CacheKey key, bool live, out CacheEntryRead<T> read)
        {
            var outcome = _owner._store.Read(key, out read, out var observed);
            if (outcome == CacheReadOutcome.Absent)
                return false;

            if (outcome != CacheReadOutcome.Present || (live && _owner.IsExpired(read.ExpiresUtc)))
                return TryReadWithLock(key, live, out read);

            if (live && _owner._eviction.Enabled)
                observed.RecordAccess();
            return true;
        }

        private bool TryReadWithLock(CacheKey key, bool live, out CacheEntryRead<T> read)
        {
            lock (_owner._lock)
            {
                if (live ? _owner.TryGetLiveLocked(key, true, out var node) : _owner._store.TryGetLocked(key, out node))
                {
                    read = node.ReadLocked();
                    return true;
                }
            }

            read = default;
            return false;
        }
    }
}
