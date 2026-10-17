using System;
using System.Collections.Frozen;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.Core;
using Squirix.Server.Utils;

namespace Squirix.Server.LocalCache;

/// <summary>In-memory cache store (KV and expiration).</summary>
/// <typeparam name="T">The stored value type.</typeparam>
/// <remarks>
/// Every mutation and lookup runs under a single <see cref="_lock" />. The value store and the
/// eviction-order bookkeeping used to be two independently synchronized structures (a lock-free
/// <c language="csharp">ConcurrentDictionary</c> plus a separately locked index). Any interleaving of concurrent
/// inserts, removals, expirations, and evictions across those two structures could leave them
/// out of sync - a key present in one but not the other. Merging both into one <see cref="Node" /> per key, mutated only
/// inside <see cref="_lock" />, makes that class of bug structurally impossible: there is no
/// second structure left to diverge from. A deadline index finds the entries whose deadline passed without walking the store: under
/// <see cref="CacheExpiryAuthority.CommittedRecords" /> it lists them, under <see cref="CacheExpiryAuthority.LocalClock" /> the local expiry
/// sweep removes them.
/// </remarks>
[Immutable]
internal sealed class PhysicalCache<T> : ILocalCache<T>, ILocalCacheSnapshotReader<T>
{
    private readonly EvictionOptions _eviction;
    private readonly DeadlineIndex _deadlines;
    private readonly CacheExpiryAuthority _expiry;
    private readonly Footprint _footprint;
    private readonly Lock _lock = new();
    private readonly LinkedList<CacheKey> _order = new();
    private readonly Dictionary<CacheKey, Node> _store = [];
    private readonly TimeProvider _timeProvider;

    /// <summary>Initializes a new instance of the <see cref="PhysicalCache{T}" /> class.</summary>
    /// <param name="timeProvider">The node clock.</param>
    /// <param name="estimator">Estimates the bytes of an entry; the cache records the estimate of every entry it stores.</param>
    /// <param name="eviction">The eviction options; unbounded LRU when <see langword="null" />.</param>
    /// <param name="expiry">
    /// Who decides expiry. <see cref="CacheExpiryAuthority.LocalClock" /> treats an entry past its deadline as absent;
    /// <see cref="CacheExpiryAuthority.CommittedRecords" /> keeps it present until a committed record removes it.
    /// </param>
    internal PhysicalCache(
        TimeProvider timeProvider,
        ICacheEntrySizeEstimator<T> estimator,
        EvictionOptions? eviction = null,
        CacheExpiryAuthority expiry = CacheExpiryAuthority.LocalClock)
    {
        ArgumentNullException.ThrowIfNull(estimator);
        _timeProvider = timeProvider;
        _footprint = new Footprint(estimator);
        _eviction = eviction ?? new EvictionOptions { Policy = EvictionPolicyType.Lru };
        _expiry = expiry;
        _deadlines = new DeadlineIndex(this);
        RawReader = new PhysicalCacheRawReader(this);
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

    internal ILocalCacheRawReader<T> RawReader { get; }

    private DateTime UtcNow => _timeProvider.GetUtcNow().UtcDateTime;

    public async IAsyncEnumerable<(CacheKey Key, NodeCacheEntry<T> Entry)> EnumerateLiveAsync([EnumeratorCancellation] CancellationToken cancellationToken)
    {
        const int yieldEvery = 256;

        // Snapshot the key set under the lock, then look up one key at a time under its own short
        // lock acquisition. This never holds _lock across a `yield return` or an `await`, and the
        // lookup deliberately does not touch eviction order or frequency: a snapshot read must not
        // reorder LRU or inflate LFU counts for entries it merely enumerates.
        CacheKey[] keys;
        lock (_lock)
        {
            keys = new CacheKey[_store.Count];
            _store.Keys.CopyTo(keys, 0);
        }

        var produced = 0;
        foreach (var key in keys)
        {
            cancellationToken.ThrowIfCancellationRequested();

            NodeCacheEntry<T>? entry = null;
            lock (_lock)
            {
                if (_store.TryGetValue(key, out var node) && !IsExpiredLocked(node))
                    entry = new NodeCacheEntry<T>(node.Value, node.Version, node.ExpiresUtc, tags: node.Tags);
            }

            if (entry == null)
                continue;

            yield return (key, entry);
            produced++;
            if (produced % yieldEvery == 0)
                await Task.Yield();
        }
    }

    public ValueTask<NodeCacheEntry<T>?> GetEntryAsync(CacheKey key, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        lock (_lock)
            return ValueTask.FromResult(TryGetLiveLocked(key, true, out var node) ? new NodeCacheEntry<T>(node.Value, node.Version, node.ExpiresUtc, tags: node.Tags) : null);
    }

    public ValueTask<NodeCacheValueResult<T>> GetValueAsync(CacheKey key, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        lock (_lock)
            return ValueTask.FromResult(TryGetLiveLocked(key, true, out var node) ? new NodeCacheValueResult<T>(true, node.Value) : new NodeCacheValueResult<T>(false, default));
    }

    public ValueTask InsertRecoveryAsync(CacheKey key, NodeCacheEntry<T> entry, CancellationToken cancellationToken) => SetAsync(key, entry, cancellationToken);

    public ValueTask<CacheRemoveResult<T>> RemoveAsync(CacheKey key, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        lock (_lock)
        {
            var (removed, value) = RemoveLocked(key);
            return ValueTask.FromResult(new CacheRemoveResult<T>(removed, value));
        }
    }

    public ValueTask<bool> RemoveExpirationAsync(CacheKey key, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        lock (_lock)
        {
            if (!TryGetLiveLocked(key, false, out var node))
                return ValueTask.FromResult(false);

            var hadDeadline = node.ExpiresUtc != null;
            if (hadDeadline)
                _footprint.Resize(node, _footprint.Estimate(key, new NodeCacheEntry<T>(node.Value, node.Version, tags: node.Tags)));

            node.ExpiresUtc = null;
            TouchOrderLocked(key, node);
            return ValueTask.FromResult(hadDeadline);
        }
    }

    public ValueTask<bool> RemoveRecoveryAsync(CacheKey key, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        lock (_lock)
            return ValueTask.FromResult(RemoveLocked(key).Removed);
    }

    public ValueTask SetAsync(CacheKey key, NodeCacheEntry<T> entry, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        var (normalized, bytes) = PrepareInsert(key, entry);
        lock (_lock)
            _ = UpsertLocked(key, normalized, bytes, false);
        return ValueTask.CompletedTask;
    }

    public ValueTask<bool> TouchAsync(CacheKey key, TimeSpan expiration, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        lock (_lock)
        {
            if (!TryGetLiveLocked(key, false, out var node))
                return ValueTask.FromResult(false);

            var expiresUtc = UtcNow.SaturatedAdd(expiration);
            if (node.ExpiresUtc == null)
                _footprint.Resize(node, _footprint.Estimate(key, new NodeCacheEntry<T>(node.Value, node.Version, expiresUtc, tags: node.Tags)));

            _deadlines.MoveLocked(key, node, expiresUtc);
            TouchOrderLocked(key, node);
            return ValueTask.FromResult(true);
        }
    }

    public ValueTask<bool> TryAddAsync(CacheKey key, NodeCacheEntry<T> entry, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        var (normalized, bytes) = PrepareInsert(key, entry);
        lock (_lock)
            return ValueTask.FromResult(UpsertLocked(key, normalized, bytes, true));
    }

    public ValueTask<bool> UpdateAsync(CacheKey key, T? value, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        lock (_lock)
        {
            if (!TryGetLiveLocked(key, false, out var node))
                return ValueTask.FromResult(false);

            _footprint.Resize(node, _footprint.Estimate(key, new NodeCacheEntry<T>(value, node.Version, node.ExpiresUtc, tags: node.Tags)));
            node.Value = value;
            TouchOrderLocked(key, node);
            return ValueTask.FromResult(true);
        }
    }

    private void EnforceCapacityLocked()
    {
        if (_eviction.Capacity is not { } cap)
            return;

        while (_store.Count > cap)
        {
            var candidate = _eviction.Policy switch
            {
                EvictionPolicyType.Fifo => _order.Last?.Value,
                EvictionPolicyType.Lru => _order.Last?.Value,
                EvictionPolicyType.Lfu => GetLeastFrequentlyUsedKeyLocked(),
                _ => throw new InvalidOperationException("Unsupported eviction policy."),
            };

            if (candidate == null)
                break;

            if (!_store.Remove(candidate, out var node))
                break; // Shouldn't happen: order and store are updated together now.

            UntrackLocked(node);
        }
    }

    private CacheKey? GetLeastFrequentlyUsedKeyLocked()
    {
        CacheKey? chosen = null;
        var minFrequency = long.MaxValue;

        for (var node = _order.Last; node != null; node = node.Previous)
        {
            if (!_store.TryGetValue(node.Value, out var entry) || entry.Frequency >= minFrequency)
                continue;

            minFrequency = entry.Frequency;
            chosen = node.Value;
        }

        return chosen;
    }

    /// <summary>Tells whether this node treats an entry as gone because its deadline passed on the node clock.</summary>
    /// <param name="node">The stored entry.</param>
    /// <returns><see langword="false" /> under <see cref="CacheExpiryAuthority.CommittedRecords" />, where only a committed record removes an entry.</returns>
    private bool IsExpiredLocked(Node node) => _expiry == CacheExpiryAuthority.LocalClock && node.ExpiresUtc is { } expires && expires <= UtcNow;

    /// <summary>Resolves the deadline of an entry about to be stored and estimates its bytes, before the cache lock is taken.</summary>
    /// <param name="key">The key the entry is stored under.</param>
    /// <param name="candidate">The entry to store.</param>
    /// <returns>The entry to store and its estimated bytes; the estimator throws before the cache is touched.</returns>
    private (NodeCacheEntry<T> Entry, long Bytes) PrepareInsert(CacheKey key, NodeCacheEntry<T> candidate)
    {
        var version = candidate.Version > 0 ? candidate.Version : 1;
        var expires = candidate.ExpiresUtc;
        if (candidate.Expiration is { } expiration)
        {
            var relativeDeadline = UtcNow.SaturatedAdd(expiration);
            if (expires == null || relativeDeadline < expires)
                expires = relativeDeadline;
        }

        var normalized = new NodeCacheEntry<T>(candidate.Value, version, expires, candidate.Expiration, candidate.Tags);
        return (normalized, _footprint.Estimate(key, normalized));
    }

    private (bool Removed, T? Value) RemoveLocked(CacheKey key)
    {
        if (!_store.Remove(key, out var node))
            return (false, default);

        var value = node.Value;
        UntrackLocked(node);
        return IsExpiredLocked(node) ? (false, default) : (true, value);
    }

    private void TouchOrderLocked(CacheKey key, Node node)
    {
        if (_eviction.Capacity == null || _eviction.Policy is EvictionPolicyType.Fifo)
            return;

        if (_eviction.Policy is EvictionPolicyType.Lfu)
        {
            node.Frequency++;
            return;
        }

        if (node.OrderNode != null)
            _order.Remove(node.OrderNode);
        node.OrderNode = _order.AddFirst(key);
        node.Frequency++;
    }

    private bool TryGetLiveLocked(CacheKey key, bool touchOrder, [NotNullWhen(true)] out Node? node)
    {
        if (!_store.TryGetValue(key, out node))
            return false;

        if (IsExpiredLocked(node))
        {
            _ = _store.Remove(key);
            UntrackLocked(node);
            node = null;
            return false;
        }

        if (touchOrder)
            TouchOrderLocked(key, node);

        return true;
    }

    /// <summary>Releases an entry that left the store: its recorded bytes leave the total, it leaves the eviction order, and it drops its value and tags.</summary>
    /// <param name="node">The entry already removed from, or about to be overwritten in, the store.</param>
    private void UntrackLocked(Node node)
    {
        _footprint.Release(node);

        // A stale item of the deadline index may keep the node reachable until its old deadline: do not let it keep the value alive.
        node.Value = default;
        node.Tags = null;
        if (node.OrderNode == null)
            return;

        _order.Remove(node.OrderNode);
        node.OrderNode = null;
    }

    private bool UpsertLocked(CacheKey key, NodeCacheEntry<T> normalized, long bytes, bool insertOnly)
    {
        _ = _store.TryGetValue(key, out var node);
        if (node != null && insertOnly && IsExpiredLocked(node))
        {
            UntrackLocked(node);
            node = null;
        }

        if (node == null)
        {
            node = new Node(normalized.Value, normalized.ExpiresUtc, normalized.Version, normalized.Tags);
            _store[key] = node;
            _footprint.Resize(node, bytes);
            _deadlines.RecordLocked(key, node);

            if (_eviction.Capacity != null)
            {
                node.OrderNode = _order.AddFirst(key);
                node.Frequency = 1;
            }

            EnforceCapacityLocked();
            return true;
        }

        if (insertOnly)
            return false;

        _footprint.Resize(node, bytes);
        node.Value = normalized.Value;
        _deadlines.MoveLocked(key, node, normalized.ExpiresUtc);
        node.Version = normalized.Version;
        node.Tags = normalized.Tags;
        TouchOrderLocked(key, node);
        return false;
    }

    /// <summary>The recorded bytes of the stored entries and their running total.</summary>
    /// <remarks>
    /// <see cref="Resize" /> and <see cref="Release" /> run only under the cache lock, so the total always equals the sum of
    /// <see cref="Node.Bytes" /> over the stored entries; <see cref="Read" /> needs no lock. Update, touch and remove-expiration estimate the
    /// stored entry under the lock, so on hosts without persistence their estimator cost is paid while the lock is held.
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

        internal long Estimate(CacheKey key, NodeCacheEntry<T> entry) => _estimator.EstimateBytes(key, entry, false);

        internal long Read() => Volatile.Read(ref _total);

        internal void Release(Node node) => Resize(node, 0);

        internal void Resize(Node node, long bytes)
        {
            Volatile.Write(ref _total, _total - node.Bytes + bytes);
            node.Bytes = bytes;
        }
    }

    /// <summary>
    /// A single live entry plus its eviction-order bookkeeping. Merging value and order state
    /// into one object per key - instead of a value in one structure and metadata in another -
    /// is what removes the divergence race: there's nothing left to fall out of sync.
    /// </summary>
    private sealed class Node
    {
        /// <summary>Initializes a new instance of the <see cref="Node" /> class.</summary>
        /// <param name="value">The cached value.</param>
        /// <param name="expiresUtc">The expiration time.</param>
        /// <param name="version">The version.</param>
        /// <param name="tags">The tags.</param>
        internal Node(T? value, DateTime? expiresUtc, long version, FrozenDictionary<string, string>? tags)
        {
            Value = value;
            ExpiresUtc = expiresUtc;
            Version = version;
            Tags = tags;
        }

        /// <summary>Gets or sets the estimated bytes recorded for this entry; zero once it left the store.</summary>
        internal long Bytes { get; set; }
        internal DateTime? ExpiresUtc { get; set; }

        /// <summary>Gets or sets the access frequency, maintained only for the LFU policy.</summary>
        internal long Frequency { get; set; } = 1;

        /// <summary>Gets or sets the deadline, in UTC ticks, of the one canonical item the deadline index holds for this entry; <see cref="long.MaxValue" /> when it holds none.</summary>
        internal long IndexedTicks { get; set; } = long.MaxValue;

        /// <summary>Gets or sets the generation of the canonical item: another item of this entry with the same deadline is stale unless it carries this generation.</summary>
        internal int IndexedGeneration { get; set; }

        /// <summary>Gets or sets the node in the shared eviction-order list, or <see langword="null" /> when eviction is unbounded.</summary>
        internal LinkedListNode<CacheKey>? OrderNode { get; set; }

        internal FrozenDictionary<string, string>? Tags { get; set; }

        internal T? Value { get; set; }

        internal long Version { get; set; }
    }

    /// <summary>The deadline index: a heap of deadlines, earliest first, apart from skipped keys, with one canonical item per entry, at or before its deadline.</summary>
    /// <remarks>
    /// Under <see cref="CacheExpiryAuthority.CommittedRecords" /> an entry past its deadline stays until a committed record removes it, so the
    /// index only lists it. Under <see cref="CacheExpiryAuthority.LocalClock" /> the local expiry sweep removes it. The heap is lazy: the item of
    /// an entry, canonical when its generation equals <see cref="Node.IndexedGeneration" />, is pushed when the entry is stored and when its deadline appears
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

        /// <summary>Sets the deadline of a stored entry, recording it only when it appeared or moved earlier: a later deadline needs no new item.</summary>
        /// <param name="key">The key of the entry.</param>
        /// <param name="node">The entry.</param>
        /// <param name="expiresUtc">The new deadline, or <see langword="null" /> for none.</param>
        internal void MoveLocked(CacheKey key, Node node, DateTime? expiresUtc)
        {
            var previous = node.ExpiresUtc;
            if (previous == expiresUtc)
                return;

            node.ExpiresUtc = expiresUtc;
            if (expiresUtc is { } moved && moved.Ticks < node.IndexedTicks)
                RecordLocked(key, node);
        }

        /// <summary>Records the deadline just set on an entry, rebuilding the heap once stale items outnumber the entries.</summary>
        /// <param name="key">The key of the entry.</param>
        /// <param name="node">The entry; one without a deadline is not recorded.</param>
        internal void RecordLocked(CacheKey key, Node node)
        {
            if (node.ExpiresUtc is not { } deadline)
                return;

            var store = _owner._store;
            if (_heap.Count < (2 * store.Count) + 1024)
            {
                Index(key, node, deadline.Ticks);
                return;
            }

            // The rebuild reads the current deadline of every entry, this one included.
            _heap.Clear();
            foreach (var (storedKey, stored) in store)
            {
                if (stored.ExpiresUtc is { } expires)
                {
                    Index(storedKey, stored, expires.Ticks);
                }
                else
                {
                    stored.IndexedTicks = long.MaxValue;
                    stored.IndexedGeneration++;
                }
            }
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

        /// <summary>Pushes the canonical item of an entry, which makes every other item of the entry stale.</summary>
        /// <param name="key">The key of the entry.</param>
        /// <param name="node">The entry.</param>
        /// <param name="ticks">The deadline of the item, in UTC ticks.</param>
        private void Index(CacheKey key, Node node, long ticks)
        {
            node.IndexedTicks = ticks;
            node.IndexedGeneration++;
            _heap.Enqueue(new Deadline(key, node, ticks, node.IndexedGeneration), ticks);
        }

        /// <summary>Tells what a popped item stands for, putting it back when its entry has a later deadline.</summary>
        /// <param name="item">The item popped.</param>
        /// <returns>
        /// <see langword="true" /> when the item names a stored entry with the deadline it recorded; <see langword="false" /> when it was dropped as
        /// stale or put back with the later deadline of its entry.
        /// </returns>
        private bool TryResolve(in Deadline item)
        {
            if (!_owner._store.TryGetValue(item.Key, out var node) || !ReferenceEquals(node, item.Node) || node.IndexedGeneration != item.Generation)
                return false;

            if (node.ExpiresUtc is not { } expires)
            {
                node.IndexedTicks = long.MaxValue;
                node.IndexedGeneration++;
                return false;
            }

            if (expires.Ticks <= item.Ticks)
                return true;

            Index(item.Key, node, expires.Ticks);
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
                if (!TryResolve(in item))
                    continue;

                _ = _owner._store.Remove(item.Key);
                _owner.UntrackLocked(item.Node);
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
                if (!TryResolve(in item))
                    continue;

                var included = include(item.Key);
                if (included)
                    listed.Add(item.Key);

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
        /// <param name="Key">The key of the entry.</param>
        /// <param name="Node">The entry; a later entry under the same key makes the item stale.</param>
        /// <param name="Ticks">The deadline recorded, in UTC ticks; a later deadline on the entry moves the item.</param>
        /// <param name="Generation">The generation of the entry when the item was pushed; the item is stale unless it equals <see cref="Node.IndexedGeneration" />.</param>
        private readonly record struct Deadline(CacheKey Key, Node Node, long Ticks, int Generation);
    }

    [Immutable]
    private sealed class PhysicalCacheRawReader : ILocalCacheRawReader<T>
    {
        private readonly PhysicalCache<T> _owner;

        internal PhysicalCacheRawReader(PhysicalCache<T> owner)
        {
            _owner = owner;
        }

        public ValueTask<NodeCacheEntry<T>?> GetEntryRawAsync(CacheKey key, CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();
            lock (_owner._lock)
            {
                return ValueTask.FromResult(
                    _owner._store.TryGetValue(key, out var node) ? new NodeCacheEntry<T>(node.Value, node.Version, node.ExpiresUtc, tags: node.Tags) : null);
            }
        }
    }
}
