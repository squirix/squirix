using System;
using System.Collections.Frozen;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Numerics;
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
/// <para>Every mutation runs under a single <see cref="_lock" />, so writers are serialized. A non-live read, as <see cref="EnumerateLiveAsync" /> and
/// <see cref="RawReader" /> make, and a live lookup on a cache without a capacity take no lock: the <see cref="Store" /> finds the node of a key
/// without it, and the <see cref="Node" /> hands out its fields under a sequence lock, so a reader sees the fields of one write, never of two. A
/// live lookup takes the lock when a capacity is set, since the read counts as an access for the eviction policy, and when the entry is past its
/// deadline, which it re-checks and removes under the lock. Any read takes the lock when a writer kept the node or the store busy while it
/// looked.</para>
/// <para>The value store and the
/// eviction-order bookkeeping used to be two independently synchronized structures (a lock-free
/// <c language="csharp">ConcurrentDictionary</c> plus a separately locked index). Any interleaving of concurrent
/// inserts, removals, expirations, and evictions across those two structures could leave them
/// out of sync - a key present in one but not the other. Merging both into one <see cref="Node" /> per key, mutated only
/// inside <see cref="_lock" />, makes that class of bug structurally impossible: there is no
/// second structure left to diverge from. A deadline index finds the entries whose deadline passed without walking the store: under
/// <see cref="CacheExpiryAuthority.CommittedRecords" /> it lists them, under <see cref="CacheExpiryAuthority.LocalClock" /> the local expiry
/// sweep removes them.</para>
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
    private readonly Store _store = new();
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

    /// <summary>What a read without the cache lock found.</summary>
    private enum ReadOutcome
    {
        /// <summary>The key holds no entry.</summary>
        Absent = 0,

        /// <summary>The key holds an entry.</summary>
        Present = 1,

        /// <summary>A writer got in the way; read again under the cache lock.</summary>
        Unsure = 2,
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

            if (!TryRead(key, false, out var read) || IsExpired(read.ExpiresUtc))
                continue;

            yield return (key, read.ToEntry());
            produced++;
            if (produced % yieldEvery == 0)
                await Task.Yield();
        }
    }

    public ValueTask<NodeCacheEntry<T>?> GetEntryAsync(CacheKey key, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        return ValueTask.FromResult(TryRead(key, true, out var read) ? read.ToEntry() : null);
    }

    public ValueTask<NodeCacheValueResult<T>> GetValueAsync(CacheKey key, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        return ValueTask.FromResult(TryRead(key, true, out var read) ? new NodeCacheValueResult<T>(true, read.Value) : new NodeCacheValueResult<T>(false, default));
    }

    ValueTask<NodeCacheValueRead<T>> ILocalCacheReadOperations<T>.GetValueWithDeadlineAsync(CacheKey key, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        return ValueTask.FromResult(TryRead(key, true, out var read) ? new NodeCacheValueRead<T>(true, read.Value, read.ExpiresUtc) : default);
    }

    public ValueTask InsertRecoveryAsync(CacheKey key, NodeCacheEntry<T> entry, CancellationToken cancellationToken, int? encodedLength = null) =>
        SetAsync(key, entry, cancellationToken, encodedLength);

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
            {
                _footprint.Resize(node, _footprint.Estimate(key, new NodeCacheEntry<T>(node.Value, node.Version, tags: node.Tags)));
                node.WriteExpiresUtc(null);
            }

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

    public ValueTask SetAsync(CacheKey key, NodeCacheEntry<T> entry, CancellationToken cancellationToken, int? encodedLength = null)
    {
        cancellationToken.ThrowIfCancellationRequested();
        var (normalized, bytes) = PrepareInsert(key, entry, encodedLength);
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

            var previous = node.ExpiresUtc;
            node.WriteExpiresUtc(expiresUtc);
            _deadlines.MovedLocked(node, previous);
            TouchOrderLocked(key, node);
            return ValueTask.FromResult(true);
        }
    }

    public ValueTask<bool> TryAddAsync(CacheKey key, NodeCacheEntry<T> entry, CancellationToken cancellationToken, int? encodedLength = null)
    {
        cancellationToken.ThrowIfCancellationRequested();
        var (normalized, bytes) = PrepareInsert(key, entry, encodedLength);
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
            node.WriteValue(value);
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
                EvictionPolicyType.Lfu => _store.LeastFrequentLocked(_order),
                _ => throw new InvalidOperationException("Unsupported eviction policy."),
            };

            if (candidate is not { } victim)
                break;

            if (!_store.RemoveLocked(victim, out var node))
                break; // Shouldn't happen: order and store are updated together now.

            UntrackLocked(node);
        }
    }

    /// <summary>Tells whether this node treats an entry as gone because its deadline passed on the node clock.</summary>
    /// <param name="expiresUtc">The deadline of the stored entry, or <see langword="null" /> for none.</param>
    /// <returns><see langword="false" /> under <see cref="CacheExpiryAuthority.CommittedRecords" />, where only a committed record removes an entry.</returns>
    private bool IsExpired(DateTime? expiresUtc) => _expiry == CacheExpiryAuthority.LocalClock && expiresUtc is { } expires && expires <= UtcNow;

    /// <summary>Resolves the deadline of an entry about to be stored and estimates its bytes, before the cache lock is taken.</summary>
    /// <param name="key">The key the entry is stored under.</param>
    /// <param name="candidate">The entry to store.</param>
    /// <param name="knownEncodedLength">
    /// The journal-encoded length of <paramref name="candidate" />, or <see langword="null" /> to measure. It is used only for an entry without a relative
    /// expiration: resolving a relative expiration adds a deadline the length may not cover, so that entry is measured.
    /// </param>
    /// <returns>The entry to store and its estimated bytes; the estimator throws before the cache is touched.</returns>
    private (NodeCacheEntry<T> Entry, long Bytes) PrepareInsert(CacheKey key, NodeCacheEntry<T> candidate, int? knownEncodedLength)
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
        var bytes = knownEncodedLength is { } length && candidate.Expiration == null ? _footprint.Estimate(key, normalized, length) : _footprint.Estimate(key, normalized);
        return (normalized, bytes);
    }

    private (bool Removed, T? Value) RemoveLocked(CacheKey key)
    {
        if (!_store.RemoveLocked(key, out var node))
            return (false, default);

        var value = node.Value;
        var expired = IsExpired(node.ExpiresUtc);
        UntrackLocked(node);
        return expired ? (false, default) : (true, value);
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

        // Moving the list node of the entry, rather than linking a new one, keeps an access from allocating.
        if (node.OrderNode is { } orderNode)
        {
            _order.Remove(orderNode);
            _order.AddFirst(orderNode);
        }
        else
        {
            node.OrderNode = _order.AddFirst(key);
        }

        node.Frequency++;
    }

    private bool TryGetLiveLocked(CacheKey key, bool touchOrder, [NotNullWhen(true)] out Node? node)
    {
        if (!_store.TryGetLocked(key, out node))
            return false;

        if (IsExpired(node.ExpiresUtc))
        {
            _store.RemoveLocked(node);
            UntrackLocked(node);
            node = null;
            return false;
        }

        if (touchOrder)
            TouchOrderLocked(key, node);

        return true;
    }

    /// <summary>Reads the entry of a key, without the cache lock unless a writer gets in the way or the read must change the cache.</summary>
    /// <param name="key">The key to read.</param>
    /// <param name="live">
    /// <see langword="true" /> to read only a live entry, which counts as an access for the eviction policy and removes an entry past its deadline;
    /// <see langword="false" /> to read the stored entry, past its deadline or not, without changing the cache.
    /// </param>
    /// <param name="read">The fields of the entry.</param>
    /// <returns><see langword="true" /> when the key holds an entry to return.</returns>
    /// <remarks>A live read with a capacity set runs under the lock, since it touches eviction order.</remarks>
    private bool TryRead(CacheKey key, bool live, out EntryRead read)
    {
        if (!live || _eviction.Capacity == null)
        {
            var outcome = _store.Read(key, out read);
            if (outcome == ReadOutcome.Absent)
                return false;

            if (outcome == ReadOutcome.Present && !(live && IsExpired(read.ExpiresUtc)))
                return true;
        }

        lock (_lock)
        {
            if (live ? TryGetLiveLocked(key, true, out var node) : _store.TryGetLocked(key, out node))
            {
                read = node.ReadLocked();
                return true;
            }
        }

        read = default;
        return false;
    }

    /// <summary>Releases an entry that left the store: its recorded bytes leave the total, it leaves the eviction order, and it drops its value and tags.</summary>
    /// <param name="node">The entry already removed from the store.</param>
    private void UntrackLocked(Node node)
    {
        _footprint.Release(node);

        // A reader that found the node before it left the store sees it removed. A stale item of the deadline index may keep the node
        // reachable until its old deadline: do not let it keep the value alive.
        node.MarkRemoved();
        if (node.OrderNode == null)
            return;

        _order.Remove(node.OrderNode);
        node.OrderNode = null;
    }

    private bool UpsertLocked(CacheKey key, NodeCacheEntry<T> normalized, long bytes, bool insertOnly)
    {
        _ = _store.TryGetLocked(key, out var node);
        if (node != null && insertOnly && IsExpired(node.ExpiresUtc))
        {
            _store.RemoveLocked(node);
            UntrackLocked(node);
            node = null;
        }

        if (node == null)
        {
            node = new Node(key, normalized.Value, normalized.ExpiresUtc, normalized.Version, normalized.Tags);
            _store.AddLocked(node);
            _footprint.Resize(node, bytes);
            _deadlines.RecordLocked(node);

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
        var previous = node.ExpiresUtc;
        node.Write(normalized.Value, normalized.Version, normalized.ExpiresUtc, normalized.Tags);
        _deadlines.MovedLocked(node, previous);
        TouchOrderLocked(key, node);
        return false;
    }

    /// <summary>The fields of an entry as one read saw them.</summary>
    /// <param name="Value">The cached value.</param>
    /// <param name="Version">The version.</param>
    /// <param name="ExpiresUtc">The deadline, or <see langword="null" /> for none.</param>
    /// <param name="Tags">The tags.</param>
    [Immutable]
    private readonly record struct EntryRead(T? Value, long Version, DateTime? ExpiresUtc, FrozenDictionary<string, string>? Tags)
    {
        /// <summary>Builds the entry a read returns.</summary>
        /// <returns>A new entry with these fields.</returns>
        internal NodeCacheEntry<T> ToEntry() => new(Value, Version, ExpiresUtc, tags: Tags);
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

        internal long Estimate(CacheKey key, NodeCacheEntry<T> entry, int knownEncodedLength) => _estimator.EstimateBytesForEncodedLength(key, entry, knownEncodedLength);

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
    /// <remarks>
    /// <para>What a read returns - value, version, deadline and tags - and whether the node left the store are guarded by a sequence lock. A writer,
    /// always under the cache lock, makes <see cref="_sequence" /> odd, changes the fields in place with release writes, which keep the odd sequence
    /// before them, and makes it even again with a release write, which keeps them before it. <see cref="Read" /> reads the sequence, the fields
    /// and the sequence again, each with an acquire read so none of them moves past the next, and keeps the fields only when both reads saw the
    /// same even number: no write overlapped them, so they are the fields of one write.</para>
    /// <para>A node never changes key. A removed node is marked removed, its value and tags dropped, in the same write, and is never stored again; a
    /// key stored again gets a new node. The rest is writer-only bookkeeping, changed under the cache lock.</para>
    /// </remarks>
    private sealed class Node
    {
        /// <summary>
        /// The bits <see cref="_expires" /> holds for no deadline: no <see cref="DateTime" /> has them, since its ticks would exceed
        /// <see cref="DateTime.MaxValue" />.
        /// </summary>
        private const long NoDeadline = -1;

        private long _expires;
        private Node? _next;
        private bool _removed;
        private int _sequence;
        private FrozenDictionary<string, string>? _tags;
        private T? _value;
        private long _version;

        /// <summary>Initializes a new instance of the <see cref="Node" /> class.</summary>
        /// <param name="key">The key the entry is stored under.</param>
        /// <param name="value">The cached value.</param>
        /// <param name="expiresUtc">The expiration time.</param>
        /// <param name="version">The version.</param>
        /// <param name="tags">The tags.</param>
        internal Node(CacheKey key, T? value, DateTime? expiresUtc, long version, FrozenDictionary<string, string>? tags)
        {
            Key = key;
            Hash = key.GetHashCode();
            _value = value;
            _expires = Encode(expiresUtc);
            _version = version;
            _tags = tags;
        }

        /// <summary>Gets or sets the estimated bytes recorded for this entry; zero once it left the store.</summary>
        internal long Bytes { get; set; }

        /// <summary>Gets the deadline; read under the cache lock.</summary>
        internal DateTime? ExpiresUtc => Decode(_expires);

        /// <summary>Gets or sets the access frequency, maintained only for the LFU policy.</summary>
        internal long Frequency { get; set; } = 1;

        /// <summary>Gets the hash code of <see cref="Key" />.</summary>
        internal int Hash { get; }

        /// <summary>Gets a value indicating whether the node is in the store; read under the cache lock.</summary>
        /// <remarks>A node enters the store when it is built and leaves it once, through <see cref="MarkRemoved" />, so this needs no lookup.</remarks>
        internal bool IsStored => !_removed;

        /// <summary>Gets or sets the deadline, in UTC ticks, of the one canonical item the deadline index holds for this entry; <see cref="long.MaxValue" /> when it holds none.</summary>
        internal long IndexedTicks { get; set; } = long.MaxValue;

        /// <summary>Gets or sets the generation of the canonical item: another item of this entry with the same deadline is stale unless it carries this generation.</summary>
        internal int IndexedGeneration { get; set; }

        /// <summary>Gets the key the entry is stored under.</summary>
        internal CacheKey Key { get; }

        /// <summary>Gets the link to the next node of the same bucket of the <see cref="Store" />.</summary>
        internal ref Node? Next => ref _next;

        /// <summary>Gets or sets the node in the shared eviction-order list, or <see langword="null" /> when eviction is unbounded.</summary>
        internal LinkedListNode<CacheKey>? OrderNode { get; set; }

        /// <summary>Gets the tags; read under the cache lock.</summary>
        internal FrozenDictionary<string, string>? Tags => _tags;

        /// <summary>Gets the value; read under the cache lock.</summary>
        internal T? Value => _value;

        /// <summary>Gets the version; read under the cache lock.</summary>
        internal long Version => _version;

        /// <summary>Marks the node removed from the store and drops its value and tags; call under the cache lock.</summary>
        internal void MarkRemoved()
        {
            var odd = BeginWrite();
            StoreValue(default);
            Volatile.Write(ref _tags, null);
            Volatile.Write(ref _removed, true);
            EndWrite(odd);
        }

        /// <summary>Reads the fields under the cache lock.</summary>
        /// <returns>The fields.</returns>
        internal EntryRead ReadLocked() => new(_value, _version, ExpiresUtc, _tags);

        /// <summary>Reads the fields without the cache lock, retrying while a write overlaps the read.</summary>
        /// <param name="read">The fields of one write when the outcome is <see cref="ReadOutcome.Present" />.</param>
        /// <returns>
        /// <see cref="ReadOutcome.Present" /> for a stored node, <see cref="ReadOutcome.Absent" /> for a removed one, or <see cref="ReadOutcome.Unsure" />
        /// when writes kept overlapping the read and the caller must read under the cache lock.
        /// </returns>
        internal ReadOutcome Read(out EntryRead read)
        {
            var spinner = default(SpinWait);
            bool removed;
            while (!TryReadOnce(out read, out removed))
            {
                if (spinner.NextSpinWillYield)
                    return ReadOutcome.Unsure;

                spinner.SpinOnce();
            }

            return removed ? ReadOutcome.Absent : ReadOutcome.Present;
        }

        /// <summary>Replaces what a read returns in one write; call under the cache lock.</summary>
        /// <param name="value">The cached value.</param>
        /// <param name="version">The version.</param>
        /// <param name="expiresUtc">The deadline, or <see langword="null" /> for none.</param>
        /// <param name="tags">The tags.</param>
        internal void Write(T? value, long version, DateTime? expiresUtc, FrozenDictionary<string, string>? tags)
        {
            var odd = BeginWrite();
            StoreValue(value);
            Volatile.Write(ref _version, version);
            Volatile.Write(ref _expires, Encode(expiresUtc));
            Volatile.Write(ref _tags, tags);
            EndWrite(odd);
        }

        /// <summary>Replaces the deadline; call under the cache lock.</summary>
        /// <param name="expiresUtc">The deadline, or <see langword="null" /> for none.</param>
        internal void WriteExpiresUtc(DateTime? expiresUtc)
        {
            var odd = BeginWrite();
            Volatile.Write(ref _expires, Encode(expiresUtc));
            EndWrite(odd);
        }

        /// <summary>Replaces the value; call under the cache lock.</summary>
        /// <param name="value">The cached value.</param>
        internal void WriteValue(T? value)
        {
            var odd = BeginWrite();
            StoreValue(value);
            EndWrite(odd);
        }

        private static DateTime? Decode(long expires) => expires == NoDeadline ? null : Unsafe.BitCast<long, DateTime>(expires);

        private static long Encode(DateTime? expiresUtc) => expiresUtc is { } deadline ? Unsafe.BitCast<DateTime, long>(deadline) : NoDeadline;

        /// <summary>Makes the sequence odd.</summary>
        /// <returns>The odd sequence.</returns>
        /// <remarks>
        /// A reader that sees a field write must then see the odd sequence. Every field write that follows is a release write, which keeps this write
        /// before it; a value-type value is written plainly, so a full fence keeps it after this write.
        /// </remarks>
        private int BeginWrite()
        {
            var odd = _sequence + 1;
            Volatile.Write(ref _sequence, odd);
            if (typeof(T).IsValueType)
                Interlocked.MemoryBarrier();

            return odd;
        }

        /// <summary>Makes the sequence even; the release write keeps every field write before it.</summary>
        /// <param name="odd">The odd sequence <see cref="BeginWrite" /> returned.</param>
        private void EndWrite(int odd) => Volatile.Write(ref _sequence, odd + 1);

        /// <summary>Reads the fields once: the sequence, the fields and the sequence again.</summary>
        /// <param name="read">The fields of one write, or the default when the read failed or the node was removed.</param>
        /// <param name="removed">Whether the node left the store.</param>
        /// <returns><see langword="false" /> when a write overlapped the read.</returns>
        /// <remarks>Each read is an acquire read, so none moves past the next; a value-type value is read plainly and fenced before the second sequence read.</remarks>
        private bool TryReadOnce(out EntryRead read, out bool removed)
        {
            read = default;
            var sequence = Volatile.Read(ref _sequence);
            removed = Volatile.Read(ref _removed);
            if ((sequence & 1) != 0)
                return false;

            var value = ReadValue();
            var version = Volatile.Read(ref _version);
            var expires = Volatile.Read(ref _expires);
            var tags = Volatile.Read(ref _tags);
            if (typeof(T).IsValueType)
                Interlocked.MemoryBarrier();

            if (Volatile.Read(ref _sequence) != sequence)
                return false;

            if (!removed)
                read = new EntryRead(value, version, Decode(expires), tags);

            return true;
        }

        /// <summary>Writes the value with a release write when it is a reference; a value type is written plainly and fenced by <see cref="BeginWrite" />.</summary>
        /// <param name="value">The value.</param>
        private void StoreValue(T? value)
        {
            if (typeof(T).IsValueType)
            {
                _value = value;
                return;
            }

            Volatile.Write(ref Unsafe.As<T?, object?>(ref _value), Unsafe.As<T?, object?>(ref value));
        }

        /// <summary>Reads the value with an acquire read when it is a reference; a value type is read plainly and fenced by <see cref="TryReadOnce" />.</summary>
        /// <returns>The value.</returns>
        private T? ReadValue()
        {
            if (typeof(T).IsValueType)
                return _value;

            var value = Volatile.Read(ref Unsafe.As<T?, object?>(ref _value));
            return Unsafe.As<object?, T?>(ref value);
        }
    }

    /// <summary>The nodes by key: a chained hash table that writers change under the cache lock and readers search without it.</summary>
    /// <remarks>
    /// <para><see cref="AddLocked" /> links a fully built node at the head of its bucket with a release write. <see cref="RemoveLocked(Node)" /> points the
    /// link before a node past it and leaves the link of the removed node as it was, so a reader standing on the removed node still reaches the rest
    /// of the bucket. A search without the lock therefore finds every node stored for the whole search, and any node it finds was stored at some point
    /// during the search.</para>
    /// <para>Growing relinks every stored node into a bucket array twice as large, which may lead a reader on the old array to a bucket of the new one
    /// and miss its key. Growing keeps <see cref="_resizes" /> odd while it relinks, so a search that missed while the count was odd or changed reports
    /// <see cref="ReadOutcome.Unsure" />. A hit needs no such check: there is at most one node per key. Links move only to nodes relinked before, so a
    /// reader never walks a cycle.</para>
    /// <para>An insert-only write that replaces an entry past its deadline removes the old node and adds a new one in one lock hold, so a non-live
    /// read that found the removed node and missed the added one reports the key absent.</para>
    /// </remarks>
    [ThreadSafe]
    private sealed class Store
    {
        private const int InitialBucketCount = 16;

        /// <summary>
        /// The multiplier of Fibonacci hashing, 2^32 divided by the golden ratio (0x9E3779B9) as a signed number, which spreads the hash bits into the
        /// top bits a bucket index takes.
        /// </summary>
        private const int FibonacciMultiplier = -0x61C88647;

        private const int HashBits = 32;

        /// <summary>The share of buckets kept free, as a right shift of the bucket count: a quarter, so the store grows at three nodes per four buckets.</summary>
        private const int FreeBucketShift = 2;

        private Node?[] _buckets = new Node?[InitialBucketCount];
        private int _resizes;

        /// <summary>Gets the number of stored nodes; read under the cache lock.</summary>
        internal int Count { get; private set; }

        /// <summary>Gets an enumerator over the stored nodes; use under the cache lock.</summary>
        /// <returns>The enumerator.</returns>
        public Enumerator GetEnumerator() => new(_buckets);

        /// <summary>Links a node whose key is not stored; call under the cache lock.</summary>
        /// <param name="node">The node.</param>
        internal void AddLocked(Node node)
        {
            if (Count >= GrowThreshold(_buckets.Length))
                Grow();

            ref var head = ref _buckets[IndexOf(node.Hash, _buckets.Length)];
            node.Next = head;
            Volatile.Write(ref head, node);
            Count++;
        }

        /// <summary>Copies the stored keys; call under the cache lock.</summary>
        /// <returns>The keys.</returns>
        internal CacheKey[] CopyKeysLocked()
        {
            var keys = new CacheKey[Count];
            var index = 0;
            var nodes = GetEnumerator();
            while (nodes.MoveNext())
                keys[index++] = nodes.Current.Key;

            return keys;
        }

        /// <summary>Picks the key with the lowest access count, the least recently used among equals; call under the cache lock.</summary>
        /// <param name="order">The eviction order, most recently used first.</param>
        /// <returns>The key, or <see langword="null" /> when the order is empty.</returns>
        internal CacheKey? LeastFrequentLocked(LinkedList<CacheKey> order)
        {
            CacheKey? chosen = null;
            var minFrequency = long.MaxValue;

            for (var node = order.Last; node != null; node = node.Previous)
            {
                if (!TryGetLocked(node.Value, out var entry) || entry.Frequency >= minFrequency)
                    continue;

                minFrequency = entry.Frequency;
                chosen = node.Value;
            }

            return chosen;
        }

        /// <summary>Reads the stored entry of a key without the cache lock.</summary>
        /// <param name="key">The key to read.</param>
        /// <param name="read">The fields of the entry when the outcome is <see cref="ReadOutcome.Present" />.</param>
        /// <returns>Whether the key holds an entry, or <see cref="ReadOutcome.Unsure" /> when a writer got in the way and the caller must read under the lock.</returns>
        internal ReadOutcome Read(CacheKey key, out EntryRead read)
        {
            var outcome = Find(key, out var node);
            if (node != null)
                return node.Read(out read);

            read = default;
            return outcome;
        }

        /// <summary>Unlinks the node of a key; call under the cache lock.</summary>
        /// <param name="key">The key.</param>
        /// <param name="node">The node unlinked.</param>
        /// <returns><see langword="true" /> when the key was stored.</returns>
        internal bool RemoveLocked(CacheKey key, [NotNullWhen(true)] out Node? node)
        {
            var hash = key.GetHashCode();
            ref var link = ref _buckets[IndexOf(hash, _buckets.Length)];
            while ((node = link) != null)
            {
                if (node.Hash == hash && node.Key.Equals(key))
                {
                    Volatile.Write(ref link, node.Next);
                    Count--;
                    return true;
                }

                link = ref node.Next;
            }

            return false;
        }

        /// <summary>Unlinks a stored node; call under the cache lock.</summary>
        /// <param name="node">The node, found in the store under the same lock hold.</param>
        /// <exception cref="InvalidOperationException">Thrown when the node is not stored.</exception>
        internal void RemoveLocked(Node node)
        {
            ref var link = ref _buckets[IndexOf(node.Hash, _buckets.Length)];
            while (link is { } current)
            {
                if (ReferenceEquals(current, node))
                {
                    Volatile.Write(ref link, node.Next);
                    Count--;
                    return;
                }

                link = ref current.Next;
            }

            throw new InvalidOperationException("The node is not stored.");
        }

        /// <summary>Finds the node of a key; call under the cache lock.</summary>
        /// <param name="key">The key.</param>
        /// <param name="node">The node found.</param>
        /// <returns><see langword="true" /> when the key is stored.</returns>
        internal bool TryGetLocked(CacheKey key, [NotNullWhen(true)] out Node? node)
        {
            var hash = key.GetHashCode();
            for (node = _buckets[IndexOf(hash, _buckets.Length)]; node != null; node = node.Next)
            {
                if (node.Hash == hash && node.Key.Equals(key))
                    return true;
            }

            return false;
        }

        /// <summary>Gets the node count at which the store grows: chains stay short, which keeps lookups under the lock cheap.</summary>
        /// <param name="bucketCount">The number of buckets.</param>
        /// <returns>Three quarters of <paramref name="bucketCount" />.</returns>
        private static int GrowThreshold(int bucketCount) => bucketCount - (bucketCount >> FreeBucketShift);

        /// <summary>Gets the bucket of a hash: the top bits of its Fibonacci hash, as many as the power of two <paramref name="bucketCount" /> takes.</summary>
        /// <param name="hash">The hash code.</param>
        /// <param name="bucketCount">The number of buckets, a power of two.</param>
        /// <returns>The bucket index.</returns>
        private static int IndexOf(int hash, int bucketCount) => unchecked(hash * FibonacciMultiplier) >>> (HashBits - BitOperations.TrailingZeroCount(bucketCount));

        /// <summary>Searches for the node of a key without the cache lock.</summary>
        /// <param name="key">The key.</param>
        /// <param name="node">The node found, or <see langword="null" />.</param>
        /// <returns>
        /// <see cref="ReadOutcome.Present" /> when found, <see cref="ReadOutcome.Absent" /> when not, or <see cref="ReadOutcome.Unsure" /> when the
        /// store grew during a search that missed.
        /// </returns>
        private ReadOutcome Find(CacheKey key, out Node? node)
        {
            var hash = key.GetHashCode();
            var resizes = Volatile.Read(ref _resizes);
            var buckets = Volatile.Read(ref _buckets);
            for (node = Volatile.Read(ref buckets[IndexOf(hash, buckets.Length)]); node != null; node = Volatile.Read(ref node.Next))
            {
                if (node.Hash == hash && node.Key.Equals(key))
                    return ReadOutcome.Present;
            }

            return (resizes & 1) == 0 && Volatile.Read(ref _resizes) == resizes ? ReadOutcome.Absent : ReadOutcome.Unsure;
        }

        /// <summary>Relinks every node into twice as many buckets; call under the cache lock.</summary>
        private void Grow()
        {
            var buckets = _buckets;
            var grown = new Node?[buckets.Length * 2];

            // Odd while relinking; the full fence keeps every relinking write after the odd count.
            var resizing = Interlocked.Increment(ref _resizes);
            foreach (var head in buckets)
            {
                var node = head;
                while (node != null)
                {
                    var next = node.Next;
                    ref var slot = ref grown[IndexOf(node.Hash, grown.Length)];
                    Volatile.Write(ref node.Next, slot);
                    slot = node;
                    node = next;
                }
            }

            Volatile.Write(ref _buckets, grown);
            Volatile.Write(ref _resizes, resizing + 1);
        }

        /// <summary>Walks the stored nodes bucket by bucket; use under the cache lock.</summary>
        internal sealed class Enumerator
        {
            private readonly Node?[] _buckets;
            private int _bucket;
            private Node? _current;

            internal Enumerator(Node?[] buckets)
            {
                _buckets = buckets;
                _bucket = -1;
            }

            /// <summary>Gets the current node.</summary>
            public Node Current => _current!;

            /// <summary>Moves to the next node.</summary>
            /// <returns><see langword="false" /> when every node was visited.</returns>
            public bool MoveNext()
            {
                if (_current?.Next is { } next)
                {
                    _current = next;
                    return true;
                }

                while (++_bucket < _buckets.Length)
                {
                    if (_buckets[_bucket] is not { } head)
                        continue;

                    _current = head;
                    return true;
                }

                _current = null;
                return false;
            }
        }
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

        /// <summary>Records the deadline just set on a stored entry only when it appeared or moved earlier: a later deadline needs no new item.</summary>
        /// <param name="node">The entry, carrying its new deadline.</param>
        /// <param name="previous">The deadline the entry had before, or <see langword="null" /> for none.</param>
        internal void MovedLocked(Node node, DateTime? previous)
        {
            var expiresUtc = node.ExpiresUtc;
            if (previous == expiresUtc)
                return;

            if (expiresUtc is { } moved && moved.Ticks < node.IndexedTicks)
                RecordLocked(node);
        }

        /// <summary>Records the deadline just set on an entry, rebuilding the heap once stale items outnumber the entries.</summary>
        /// <param name="node">The entry; one without a deadline is not recorded.</param>
        internal void RecordLocked(Node node)
        {
            if (node.ExpiresUtc is not { } deadline)
                return;

            var store = _owner._store;
            if (_heap.Count < (2 * store.Count) + 1024)
            {
                Index(node, deadline.Ticks);
                return;
            }

            // The rebuild reads the current deadline of every entry, this one included.
            _heap.Clear();
            var nodes = store.GetEnumerator();
            while (nodes.MoveNext())
            {
                var stored = nodes.Current;
                if (stored.ExpiresUtc is { } expires)
                {
                    Index(stored, expires.Ticks);
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
        /// <param name="node">The entry.</param>
        /// <param name="ticks">The deadline of the item, in UTC ticks.</param>
        private void Index(Node node, long ticks)
        {
            node.IndexedTicks = ticks;
            node.IndexedGeneration++;
            _heap.Enqueue(new Deadline(node, ticks, node.IndexedGeneration), ticks);
        }

        /// <summary>Tells what a popped item stands for, putting it back when its entry has a later deadline.</summary>
        /// <param name="item">The item popped.</param>
        /// <returns>
        /// <see langword="true" /> when the item names a stored entry with the deadline it recorded; <see langword="false" /> when it was dropped as
        /// stale or put back with the later deadline of its entry.
        /// </returns>
        private bool TryResolve(in Deadline item)
        {
            var node = item.Node;
            if (!node.IsStored || node.IndexedGeneration != item.Generation)
                return false;

            if (node.ExpiresUtc is not { } expires)
            {
                node.IndexedTicks = long.MaxValue;
                node.IndexedGeneration++;
                return false;
            }

            if (expires.Ticks <= item.Ticks)
                return true;

            Index(node, expires.Ticks);
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

                _owner._store.RemoveLocked(item.Node);
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

                var key = item.Node.Key;
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
        /// <param name="Node">The entry, which holds its key; a later entry under the same key makes the item stale.</param>
        /// <param name="Ticks">The deadline recorded, in UTC ticks; a later deadline on the entry moves the item.</param>
        /// <param name="Generation">The generation of the entry when the item was pushed; the item is stale unless it equals <see cref="Node.IndexedGeneration" />.</param>
        [Immutable]
        private readonly record struct Deadline(Node Node, long Ticks, int Generation);
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
            return ValueTask.FromResult(_owner.TryRead(key, false, out var read) ? read.ToEntry() : null);
        }
    }
}
