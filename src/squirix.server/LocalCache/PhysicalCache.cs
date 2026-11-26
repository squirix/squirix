using System;
using System.Collections.Frozen;
using System.Collections.Generic;
using System.Numerics;
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
[Immutable]
internal sealed partial class PhysicalCache<T> : ILocalCache<T>, ILocalCacheSnapshotReader<T>
{
    private readonly EvictionOptions _eviction;
    private readonly DeadlineIndex _deadlines;
    private readonly CacheExpiryAuthority _expiry;
    private readonly Footprint _footprint;
    private readonly Lock _lock = new();
    private readonly Dictionary<(string Namespace, string Group), EvictionScope>? _scopes;
    private readonly Func<CacheKey, string>? _groupForKey;
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
        _eviction = eviction ?? new EvictionOptions { Policy = EvictionPolicyType.Lru };
        _footprint = new Footprint(CacheEntrySizeEstimator<T>.ForEviction(estimator, _eviction));
        _groupForKey = groupForKey;
        if (_eviction.Enabled)
        {
            if (_eviction.Capacity <= 0 || (_eviction.EvictOnMemoryPressure && (_eviction.Capacity != null || _eviction.Policy != EvictionPolicyType.Lru)))
                throw new ArgumentOutOfRangeException(nameof(eviction));
            _scopes = [];
        }

        _expiry = expiry;
        _deadlines = new DeadlineIndex(this);
        RawReader = new PhysicalCacheRawReader(this);
        DistributedAccess = new DistributedAccessOperations(this);
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

    internal bool HasCapacity => _eviction.Capacity != null;

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

    public NodeCacheEntry<T>? GetEntry(CacheKey key, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        return TryRead(key, true, out var read) ? read.ToEntry() : null;
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

            if (!TryRead(key, false, out var read) || IsExpired(read.ExpiresUtc))
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
        return TryRead(key, true, out var read) ? new NodeCacheValueResult<T>(true, read.Value) : new NodeCacheValueResult<T>(false, default);
    }

    NodeCacheValueRead<T> ILocalCacheReadOperations<T>.GetValueWithDeadline(CacheKey key, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        return TryRead(key, true, out var read) ? new NodeCacheValueRead<T>(true, read.Value, read.ExpiresUtc) : default;
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

            BoundedNode.RecordAccess(in node);
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
        var (normalized, bytes) = PrepareInsert(key, entry, encodedLength);
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
            BoundedNode.RecordAccess(in node);
            return true;
        }
    }

    public bool TryAdd(CacheKey key, NodeCacheEntry<T> entry, CancellationToken cancellationToken, int? encodedLength = null)
    {
        cancellationToken.ThrowIfCancellationRequested();
        var (normalized, bytes) = PrepareInsert(key, entry, encodedLength);
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
            var raw = _store.Pack(node.Slot, ref key, value);
            _footprint.Resize(in node, bytes);
            node.WriteValue(key, raw);
            BoundedNode.RecordAccess(in node);
            return true;
        }
    }

    /// <summary>Returns the key to evict for an insertion under the leader's serialized commit gate.</summary>
    /// <param name="key">The key being inserted.</param>
    /// <returns>The victim, or null if this write needs no eviction.</returns>
    internal CacheKey? SelectCapacityVictim(CacheKey key)
    {
        lock (_lock)
        {
            if (!HasCapacity || _scopes == null || _store.TryGetLocked(key, out _) ||
                !_scopes.TryGetValue(BoundedNode.ScopeOf(this, key), out var scope) || scope.Count < _eviction.Capacity)
                return null;

            return scope.Victim()?.Key ?? key;
        }
    }

    internal CacheKey? SelectPressureVictim(string group)
    {
        lock (_lock)
            return _eviction.EvictOnMemoryPressure && _scopes!.TryGetValue((string.Empty, group), out var scope)
                ? scope.Victim()?.Key
                : null;
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
        if (candidate.PendingWrite is { } write)
            candidate = write.Materialize<T>(UtcNow);

        if (candidate is not { Version: > 0, Expiration: null })
        {
            var version = candidate.Version > 0 ? candidate.Version : 1;
            var expires = candidate.ExpiresUtc;
            if (candidate.Expiration is { } expiration)
            {
                var relativeDeadline = UtcNow.SaturatedAdd(expiration);
                if (expires == null || relativeDeadline < expires)
                    expires = relativeDeadline;
            }

            candidate = new NodeCacheEntry<T>(candidate.Value, version, expires, candidate.Expiration, candidate.Tags);
        }

        var bytes = knownEncodedLength is { } length && candidate.Expiration == null ? _footprint.Estimate(key, candidate, length) : _footprint.Estimate(key, candidate);
        return (candidate, bytes);
    }

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

    private bool TryGetLiveLocked(CacheKey key, bool touchOrder, out Node node)
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
            BoundedNode.RecordAccess(in node);

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
    /// <remarks>Policy tracking uses the observed node. Reinserting the same key creates a different node.</remarks>
    private bool TryRead(CacheKey key, bool live, out EntryRead read)
    {
        var outcome = _store.Read(key, out read, out var observed);
        if (outcome == ReadOutcome.Absent)
            return false;

        if (outcome == ReadOutcome.Present && !(live && IsExpired(read.ExpiresUtc)))
        {
            if (live && _scopes != null)
                BoundedNode.RecordAccess(in observed);
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
    private void UntrackLocked(in Node node)
    {
        _footprint.Release(in node);

        if (node.Policy is { } bounded)
        {
            bounded.Scope.Remove(bounded);
            if (bounded.Scope.Count == 0 && _scopes != null)
                _ = _scopes.Remove(BoundedNode.ScopeOf(this, node.Key));
        }

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
            node = _scopes == null
                ? _store.AddLocked(key, hash, normalized, null)
                : BoundedNode.Create(this, key, hash, normalized);
            if (!node.Exists)
                return true;

            _footprint.Resize(in node, bytes);
            _deadlines.RecordLocked(in node);

            return true;
        }

        if (insertOnly)
            return false;

        var raw = _store.Pack(node.Slot, ref key, normalized.Value);
        _footprint.Resize(in node, bytes);
        var previous = node.ExpiresUtc;
        node.Write(key, raw, normalized.Version, normalized.ExpiresUtc, normalized.Tags);
        _deadlines.MovedLocked(in node, previous);
        BoundedNode.RecordAccess(in node);
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

    /// <summary>The fields of an entry stored inline in a page.</summary>
    /// <remarks>
    /// <para>What a read returns - value, version, deadline and tags - is guarded by a sequence lock. A writer,
    /// always under the cache lock, makes <see cref="_sequence" /> odd, changes the fields in place with release writes, which keep the odd sequence
    /// before them, and makes it even again with a release write, which keeps them before it. <see cref="Read" /> reads the sequence, the fields
    /// and the sequence again, each with an acquire read so none of them moves past the next, and keeps the fields only when both reads saw the
    /// same even number: no write overlapped them, so they are the fields of one write.</para>
    /// <para>A key is stable for one positive lifetime. Removal invalidates that lifetime before clearing the references.
    /// Reuse publishes a fresh lifetime after initialization. Readers validate the observed lifetime after the sequence-locked read.</para>
    /// </remarks>
    private struct Cell
    {
        private CacheKey _key;
        private int _sequence;
        private T? _value;
        private long _version;

        /// <summary>Initializes a new instance of the <see cref="Cell" /> struct.</summary>
        /// <param name="key">The key the entry is stored under.</param>
        /// <param name="value">The cached value.</param>
        /// <param name="version">The version.</param>
        internal Cell(CacheKey key, T? value, long version)
        {
            _key = key;
            _value = value;
            _version = version;
        }

        /// <summary>Gets or sets the estimated bytes recorded for this entry; zero once it left the store.</summary>
        internal long Bytes { get; set; }

        /// <summary>Gets the key the entry is stored under.</summary>
        internal readonly CacheKey Key => !IsPacked(_value) ? _key : new CacheKey(_key.Namespace, PackedSlotPage.KeyText(_value));

        internal readonly T? RawValue => _value;

        /// <summary>Gets the value; read under the cache lock.</summary>
        internal readonly T? Value => PackedSlotPage.Unpack(_value);

        /// <summary>Gets the version; read under the cache lock.</summary>
        internal readonly long Version => _version;

        internal readonly bool MatchesLocked(CacheKey key) =>
            !IsPacked(_value) ? _key.Equals(key) : string.Equals(_key.Namespace, key.Namespace, StringComparison.Ordinal) && PackedSlotPage.Matches(_value, key.Key);

        internal bool MatchesPublished(CacheKey key, ref long lifetime, long identity, ref bool unsure)
        {
            if (!PackedSlotPage.Typed)
                return _key.Equals(key);

            var sequence = Volatile.Read(ref _sequence);
            var storedKey = _key;
            var raw = ReadValue();
            Interlocked.MemoryBarrier();
            if ((sequence & 1) != 0 || Volatile.Read(ref _sequence) != sequence || Volatile.Read(ref lifetime) != identity)
            {
                unsure = true;
                return false;
            }

            return !IsPacked(raw) ? storedKey.Equals(key) : string.Equals(storedKey.Namespace, key.Namespace, StringComparison.Ordinal) && PackedSlotPage.Matches(raw, key.Key);
        }

        /// <summary>Clears the key, value and tags after the lifetime has been invalidated; call under the cache lock.</summary>
        internal void MarkRemoved()
        {
            var odd = BeginWrite();
            _key = default;
            StoreValue(default);
            EndWrite(odd);
        }

        /// <summary>Reads the fields under the cache lock.</summary>
        /// <returns>The fields.</returns>
        /// <param name="node">The slot holding optional metadata.</param>
        internal readonly EntryRead ReadLocked(in Node node) => new(Value, _version, node.ExpiresUtc, node.Tags);

        /// <summary>Reads the fields without the cache lock, retrying while a write overlaps the read.</summary>
        /// <param name="node">The slot whose optional fields participate in this read or write.</param>
        /// <param name="read">The fields of one write when the outcome is <see cref="ReadOutcome.Present" />.</param>
        /// <returns>
        /// <see cref="ReadOutcome.Present" /> when the snapshot is consistent, or <see cref="ReadOutcome.Unsure" />
        /// when writes kept overlapping the read and the caller must read under the cache lock.
        /// </returns>
        internal ReadOutcome Read(in Node node, out EntryRead read)
        {
            var spinner = default(SpinWait);
            while (!TryReadOnce(in node, out read))
            {
                if (spinner.NextSpinWillYield)
                    return ReadOutcome.Unsure;

                spinner.SpinOnce();
            }

            return ReadOutcome.Present;
        }

        /// <summary>Replaces what a read returns in one write; call under the cache lock.</summary>
        /// <param name="node">The slot whose optional fields participate in this read or write.</param>
        /// <param name="key">The storage key representation for this value.</param>
        /// <param name="value">The cached value.</param>
        /// <param name="version">The version.</param>
        /// <param name="expiresUtc">The deadline, or <see langword="null" /> for none.</param>
        /// <param name="tags">The tags.</param>
        internal void Write(in Node node, CacheKey key, T? value, long version, DateTime? expiresUtc, FrozenDictionary<string, string>? tags)
        {
            var odd = BeginWrite();
            _key = key;
            StoreValue(value);
            Volatile.Write(ref _version, version);
            node.SetDeadline(expiresUtc);
            node.SetTags(tags);
            EndWrite(odd);
        }

        /// <summary>Replaces the deadline; call under the cache lock.</summary>
        /// <param name="node">The slot whose optional fields participate in this read or write.</param>
        /// <param name="expiresUtc">The deadline, or <see langword="null" /> for none.</param>
        internal void WriteExpiresUtc(in Node node, DateTime? expiresUtc)
        {
            var odd = BeginWrite();
            node.SetDeadline(expiresUtc);
            EndWrite(odd);
        }

        /// <summary>Replaces the value; call under the cache lock.</summary>
        /// <param name="key">The storage key representation for this value.</param>
        /// <param name="value">The cached value.</param>
        internal void WriteValue(CacheKey key, T? value)
        {
            var odd = BeginWrite();
            _key = key;
            StoreValue(value);
            EndWrite(odd);
        }

        internal void Relocate(T? value)
        {
            var odd = BeginWrite();
            StoreValue(value);
            EndWrite(odd);
        }

        private static bool IsPacked(T? raw) => PackedSlotPage.Typed && PackedSlotPage.AsStored(raw).IsPageRecord;

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
        /// <param name="node">The slot whose optional fields participate in this read or write.</param>
        /// <param name="read">The fields of one write, or the default when the read must be retried.</param>
        /// <returns><see langword="false" /> when a write overlapped the read.</returns>
        /// <remarks>Each read is an acquire read, so none moves past the next; a value-type value is read plainly and fenced before the second sequence read.</remarks>
        private bool TryReadOnce(in Node node, out EntryRead read)
        {
            read = default;
            var sequence = Volatile.Read(ref _sequence);
            if ((sequence & 1) != 0)
                return false;

            var value = ReadValue();
            var version = Volatile.Read(ref _version);
            var expires = node.ExpiresUtc;
            var tags = node.Tags;
            if (typeof(T).IsValueType)
                Interlocked.MemoryBarrier();

            if (Volatile.Read(ref _sequence) != sequence)
                return false;

            read = new EntryRead(value, version, expires, tags);

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

    /// <summary>Stores the lifetime, bucket link, and cached hash alongside the entry. Migration can relink a slot without reading its payload.</summary>
    private struct SlotRecord
    {
        internal long Lifetime;
        internal int Hash;
        internal int Next;
        internal Cell Entry;
    }

    /// <summary>Identifies one occupant of a page position, including after the position is recycled.</summary>
    private readonly struct Node
    {
        private readonly SlotPage _cells;

        internal Node(SlotPage cells, int slot, long identity)
        {
            _cells = cells;
            Identity = identity;
            Slot = slot;
        }

        internal int Slot { get; }

        internal long Identity { get; }

        internal bool Exists => _cells != null;

        internal bool IsStored => Volatile.Read(ref Record.Lifetime) == Identity;

        internal CacheKey Key => Entry.Key;

        internal BoundedNode? Policy => _cells.Policy(Offset);

        internal int Hash => Record.Hash;

        internal long Bytes
        {
            get => Entry.Bytes;
            set => Entry.Bytes = value;
        }

        internal int IndexedGeneration
        {
            get => _cells.Generation(Offset);
            set => _cells.SetGeneration(Offset, value);
        }

        internal DateTime? ExpiresUtc => _cells.Deadline(Offset);

        internal T? Value => Entry.Value;

        internal long Version => Entry.Version;

        internal FrozenDictionary<string, string>? Tags => _cells.Tags(Offset);

        private int Offset => Store.SlotOffset(Slot);

        private ref SlotRecord Record => ref _cells[Offset];

        private ref Cell Entry => ref Record.Entry;

        internal EntryRead ReadLocked() => Entry.ReadLocked(in this);

        internal ReadOutcome Read(out EntryRead read)
        {
            var result = Entry.Read(in this, out read);
            if (!IsStored)
                return ReadOutcome.Unsure;
            if (PackedSlotPage.Typed && result == ReadOutcome.Present)
                read = read with { Value = PackedSlotPage.Unpack(read.Value) };
            return result;
        }

        internal void Write(CacheKey key, T? value, long version, DateTime? expires, FrozenDictionary<string, string>? tags) =>
            Entry.Write(in this, key, value, version, expires, tags);

        internal void WriteValue(CacheKey key, T? value) => Entry.WriteValue(key, value);

        internal void WriteExpiresUtc(DateTime? expires) => Entry.WriteExpiresUtc(in this, expires);

        internal void SetDeadline(DateTime? deadline) => _cells.SetDeadline(Offset, deadline);

        internal void SetTags(FrozenDictionary<string, string>? tags) => _cells.SetTags(Offset, tags);

        internal void MarkRemoved()
        {
            _ = Interlocked.Exchange(ref Record.Lifetime, -Identity);
            Entry.MarkRemoved();
            _cells.Clear(Offset);
        }
    }

    /// <summary>The recorded bytes of the stored entries and their running total.</summary>
    /// <remarks>
    /// <see cref="Resize" /> and <see cref="Release" /> run only under the cache lock, so the total always equals the sum of
    /// <see cref="Node.Bytes" /> over the stored entries; <see cref="Read" /> needs no lock. Update measures its value before the lock and
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

        internal long Estimate(CacheKey key, NodeCacheEntry<T> entry) => _estimator.EstimateBytes(key, entry, false);

        internal long Estimate(CacheKey key, NodeCacheEntry<T> entry, int knownEncodedLength) => _estimator.EstimateBytesForEncodedLength(key, entry, knownEncodedLength);

        internal long Estimate(CacheKey key, NodeCacheEntry<T> entry, long measuredValue) => _estimator.EstimateBytesForMeasuredValue(key, entry, measuredValue);

        internal long MeasureValue(T? value) => _estimator.MeasureValue(value);

        internal long Read() => Volatile.Read(ref _total);

        internal void Release(in Node node) => Resize(in node, 0);

        internal void ResizeForDeadline(CacheKey key, in Node node, DateTime? expiresUtc)
        {
            if (_estimator is CacheEntrySizeEstimator<T>)
            {
                const int deadlineBytes = CacheEntryDeadlineSize.IndexBytes;
                Resize(in node, node.Bytes + (expiresUtc != null ? deadlineBytes : -deadlineBytes));
                return;
            }

            Resize(in node, Estimate(key, new NodeCacheEntry<T>(node.Value, node.Version, expiresUtc, tags: node.Tags)));
        }

        internal void Resize(in Node node, long bytes)
        {
            Volatile.Write(ref _total, _total - node.Bytes + bytes);
            node.Bytes = bytes;
        }
    }

    /// <summary>The nodes by key: a chained hash table that writers change under the cache lock and readers search without it.</summary>
    /// <remarks>
    /// <para>Bucket heads and links are slot numbers. Entries stay in their pages while migration rewrites links.
    /// Readers retry under the lock when a miss overlaps removal or migration. A hit also validates the occupant lifetime.</para>
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

        private const int TransferBudget = 8;
        private const int SlotPageBits = 8;
        private const int SlotsPerPage = 1 << SlotPageBits;
        private readonly SlotMap _slots = new();
        private BucketTable _table = new(InitialBucketCount);
        private BucketTable? _previous;
        private int _cursor;
        private long _topologyVersion;

        /// <summary>Gets the number of stored nodes; read under the cache lock.</summary>
        internal int Count { get; private set; }

        internal static int SlotOffset(int slot) => (slot - 1) & (SlotsPerPage - 1);

        /// <summary>Gets an enumerator over the stored nodes; use under the cache lock.</summary>
        /// <returns>The enumerator.</returns>
        internal Enumerator GetEnumerator() => _slots.GetEnumerator();

        /// <summary>Links a node whose key is not stored; call under the cache lock.</summary>
        /// <param name="key">The key.</param>
        /// <param name="hash">The hash used to look for the key.</param>
        /// <param name="entry">The entry to publish.</param>
        /// <param name="policy">Optional eviction metadata.</param>
        /// <returns>The published occupant.</returns>
        internal Node AddLocked(CacheKey key, int hash, NodeCacheEntry<T> entry, BoundedNode? policy)
        {
            if (Count >= GrowThreshold(_table.Length) && _previous == null && _table.Length < 1 << 30)
            {
                var grown = new BucketTable(_table.Length * 2);
                Volatile.Write(ref _previous, _table);
                Volatile.Write(ref _table, grown);
            }

            ref var head = ref _table.WritableBucket(_table.IndexOf(hash));
            var node = _slots.Add(key, hash, entry, policy, head, out var slot);
            Volatile.Write(ref head, slot);
            Count++;
            return node;
        }

        /// <summary>Copies the stored keys; call under the cache lock.</summary>
        /// <returns>The keys.</returns>
        internal CacheKey[] CopyKeysLocked()
        {
            if (Count == 0)
                return [];

            var keys = new CacheKey[Count];
            var index = 0;
            var nodes = GetEnumerator();
            while (nodes.MoveNext())
                keys[index++] = nodes.Current.Key;

            return keys;
        }

        /// <summary>Reads the stored entry of a key without the cache lock.</summary>
        /// <param name="key">The key to read.</param>
        /// <param name="read">The fields of the entry when the outcome is <see cref="ReadOutcome.Present" />.</param>
        /// <param name="node">The node observed by the lookup.</param>
        /// <returns>Whether the key holds an entry, or <see cref="ReadOutcome.Unsure" /> when a writer got in the way and the caller must read under the lock.</returns>
        internal ReadOutcome Read(CacheKey key, out EntryRead read, out Node node)
        {
            var outcome = Find(key, out node);
            if (node.Exists)
                return node.Read(out read);

            read = default;
            return outcome;
        }

        /// <summary>Finds the link to an occupant under the cache lock. Only use the link when the node exists.</summary>
        /// <param name="key">The requested key.</param>
        /// <param name="hash">Its previously computed hash.</param>
        /// <param name="identity">The exact occupant lifetime, or zero to match the key.</param>
        /// <param name="node">The occupant, or the default on a miss.</param>
        /// <returns>The link under the writer lock, or a null reference on a miss.</returns>
        internal ref int FindLinkLocked(CacheKey key, int hash, long identity, out Node node)
        {
            ref var link = ref FindLink(_table, key, hash, identity, out node);
            if (!node.Exists && _previous is { } previous)
                return ref FindLink(previous, key, hash, identity, out node);

            return ref link;
        }

        /// <summary>Unlinks the exact stored node under the cache lock.</summary>
        /// <param name="node">The stored node.</param>
        /// <exception cref="InvalidOperationException">The node has already left the store.</exception>
        internal void RemoveLocked(in Node node)
        {
            ref var link = ref FindLinkLocked(node.Key, node.Hash, node.Identity, out var stored);
            if (!stored.Exists)
                throw new InvalidOperationException("The node is not stored.");

            UnlinkLocked(ref link, in stored);
        }

        /// <summary>Detaches the occupant after all potentially throwing expiry checks have completed.</summary>
        /// <param name="link">The link returned by the lookup under the same lock.</param>
        /// <param name="node">The occupant reached through the link.</param>
        internal void UnlinkLocked(ref int link, in Node node)
        {
            var slots = _slots.PageFor(node.Slot);
            var offset = SlotOffset(node.Slot);
            _slots.ReleasePacked(slots, offset, node.Slot);
            _ = Interlocked.Increment(ref _topologyVersion);
            Volatile.Write(ref link, slots[offset].Next);
            _slots.Release(slots, offset, node.Slot);
            _ = Interlocked.Increment(ref _topologyVersion);
            Count--;
        }

        /// <summary>Finds the node in either generation under the cache lock.</summary>
        /// <param name="key">The requested key.</param>
        /// <param name="node">The matching stored node, if present.</param>
        /// <returns>Whether the key is stored.</returns>
        internal bool TryGetLocked(CacheKey key, out Node node) => TryGetLocked(key, key.GetHashCode(), out node);

        internal T? Pack(int slot, ref CacheKey key, T? value) => _slots.Pack(slot, ref key, value);

        internal long? DeferredDeadline(int slot) => _slots.DeferredDeadline(slot);

        internal void SetDeferredDeadline(int slot, long? ticks) => _slots.SetDeferredDeadline(slot, ticks);

        internal Node ResolveSlotLocked(int slot, long identity)
        {
            var page = _slots.ReadPage(slot);
            return page != null && page[SlotOffset(slot)].Lifetime == identity ? new Node(page, slot, identity) : default;
        }

        internal bool TryGetLocked(CacheKey key, int hash, out Node node)
        {
            var version = _topologyVersion;
            var unsure = false;
            node = Search(_table, key, hash, version, ref unsure);
            if (!node.Exists && _previous is { } previous)
                node = Search(previous, key, hash, version, ref unsure);
            return node.Exists;
        }

        /// <summary>Charges one unit per node moved or empty bucket passed, including a partially drained collision chain.</summary>
        internal void AdvanceGrowthLocked()
        {
            var table = _table;
            if (_previous is not { } previous)
                return;

            var resizing = Interlocked.Increment(ref _topologyVersion);
            try
            {
                for (var work = 0; work < TransferBudget; work++)
                {
                    var source = previous.Page(_cursor);
                    var index = BucketTable.Offset(_cursor);
                    var slot = source?[index] ?? 0;
                    if (slot != 0)
                    {
                        // Obtain the destination page before unlinking, so allocation failure leaves the node reachable.
                        var page = _slots.PageFor(slot);
                        var offset = SlotOffset(slot);
                        ref var record = ref page[offset];
                        ref var destination = ref table.WritableBucket(table.IndexOf(record.Hash));
                        Volatile.Write(ref source![index], record.Next);
                        if (record.Next != destination)
                            Volatile.Write(ref record.Next, destination);
                        Volatile.Write(ref destination, slot);
                    }
                    else if (++_cursor == previous.Length)
                    {
                        Volatile.Write(ref _previous, null);
                        _cursor = 0;
                        break;
                    }
                }
            }
            finally
            {
                Volatile.Write(ref _topologyVersion, resizing + 1);
            }
        }

        /// <summary>Gets the node count at which the store grows: chains stay short, which keeps lookups under the lock cheap.</summary>
        /// <param name="bucketCount">The number of buckets.</param>
        /// <returns>Three quarters of <paramref name="bucketCount" />.</returns>
        private static int GrowThreshold(int bucketCount) => bucketCount - (bucketCount >> FreeBucketShift);

        private ref int FindLink(BucketTable table, CacheKey key, int hash, long identity, out Node node)
        {
            var index = table.IndexOf(hash);
            if (table.Page(index) is { } page)
            {
                ref var link = ref page[BucketTable.Offset(index)];
                while (link != 0)
                {
                    var slot = link;
                    var slots = _slots.PageFor(slot);
                    var offset = SlotOffset(slot);
                    ref var record = ref slots[offset];
                    if (identity != 0 ? identity == record.Lifetime : record.Hash == hash && record.Entry.MatchesLocked(key))
                    {
                        node = new Node(slots, slot, record.Lifetime);
                        return ref link;
                    }

                    link = ref record.Next;
                }
            }

            node = default;
            return ref Unsafe.NullRef<int>();
        }

        private Node Search(BucketTable table, CacheKey key, int hash, long version, ref bool unsure)
        {
            var slot = table.Head(table.IndexOf(hash));
            while (slot > 0)
            {
                var page = _slots.ReadPage(slot);
                if (page == null)
                    return default;

                var offset = SlotOffset(slot);
                ref var record = ref page[offset];
                var identity = Volatile.Read(ref record.Lifetime);
                if (identity <= 0)
                    return default;
                if (record.Hash == hash && record.Entry.MatchesPublished(key, ref record.Lifetime, identity, ref unsure))
                    return new Node(page, slot, identity);
                slot = Volatile.Read(ref record.Next);

                // A reader crossing reuse may enter another chain. Stop before following it indefinitely.
                if (slot > 0 && Volatile.Read(ref _topologyVersion) != version)
                    return default;
            }

            return default;
        }

        /// <summary>Searches for the node of a key without the cache lock.</summary>
        /// <param name="key">The key.</param>
        /// <param name="node">The node found, or <see langword="null" />.</param>
        /// <returns>
        /// <see cref="ReadOutcome.Present" /> when found, <see cref="ReadOutcome.Absent" /> when not, or <see cref="ReadOutcome.Unsure" /> when the
        /// store grew during a search that missed.
        /// </returns>
        private ReadOutcome Find(CacheKey key, out Node node)
        {
            var hash = key.GetHashCode();
            var version = Volatile.Read(ref _topologyVersion);
            var table = Volatile.Read(ref _table);
            var unsure = false;
            node = Search(table, key, hash, version, ref unsure);
            if (!node.Exists && Volatile.Read(ref _previous) is { } previous)
                node = Search(previous, key, hash, version, ref unsure);
            if (!node.Exists)
            {
                var stable = !unsure && (version & 1) == 0 && Volatile.Read(ref _topologyVersion) == version;
                return stable ? ReadOutcome.Absent : ReadOutcome.Unsure;
            }

            return ReadOutcome.Present;
        }

        /// <summary>Keeps small indexes in one array and allocates pages on demand for larger indexes.</summary>
        internal sealed class BucketTable
        {
            private const int PageBits = 13;
            private const int PageSize = 1 << PageBits;
            private readonly int[]?[] _pages;
            private readonly int[]? _buckets;
            private readonly int _hashShift;

            internal BucketTable(int length)
            {
                Length = length;
                _hashShift = HashBits - BitOperations.TrailingZeroCount(length);
                if (length <= PageSize)
                {
                    _buckets = new int[length];
                    _pages = [];
                }
                else
                {
                    _pages = new int[length >> PageBits][];
                }
            }

            internal int Length { get; }

            internal static int Offset(int index) => index & (PageSize - 1);

            internal int IndexOf(int hash) => unchecked(hash * FibonacciMultiplier) >>> _hashShift;

            internal int[]? Page(int index) => _buckets ?? Volatile.Read(ref _pages[index >> PageBits]);

            internal int Head(int index) => Page(index) is { } page ? Volatile.Read(ref page[Offset(index)]) : 0;

            internal ref int WritableBucket(int index)
            {
                if (_buckets != null)
                    return ref _buckets[index];

                ref var slot = ref _pages[index >> PageBits];
                var page = slot;
                if (page == null)
                {
                    page = new int[PageSize];
                    Volatile.Write(ref slot, page);
                }

                return ref page[Offset(index)];
            }
        }

        /// <summary>Visits occupied slots in page order under the cache lock, including while buckets are migrating.</summary>
        internal sealed class Enumerator
        {
            private readonly SlotPage?[] _pages;
            private readonly int _pageCount;
            private SlotPage? _page;
            private int _pageIndex = -1;
            private int _offset;

            internal Enumerator(SlotPage?[] pages, int pageCount)
            {
                _pages = pages;
                _pageCount = pageCount;
            }

            internal Node Current { get; private set; }

            internal bool MoveNext()
            {
                while (true)
                {
                    while (_page != null && _offset < _page.Length)
                    {
                        var offset = _offset++;
                        var identity = _page[offset].Lifetime;
                        if (identity > 0)
                        {
                            Current = new Node(_page, (_pageIndex * SlotsPerPage) + offset + 1, identity);
                            return true;
                        }
                    }

                    if (++_pageIndex >= _pageCount)
                        return false;

                    _page = _pages[_pageIndex];
                    _offset = 0;
                }
            }
        }

        /// <summary>Owns stable slot numbers. Zero terminates a bucket chain; released slots are reused under the writer lock.</summary>
        private sealed class SlotMap
        {
            private const int DeadlineBlockSize = 64;
            private SlotPage?[] _pages = new SlotPage[1];
            private PageSpace[] _space = new PageSpace[1];
            private int _pageCount;
            private int _available = -1;
            private int _spare = -1;
            private long _lastIdentity;

            internal Enumerator GetEnumerator() => new(_pages, _pageCount);

            internal long? DeferredDeadline(int slot)
            {
                var offset = SlotOffset(slot);
                var encoded = _space[(slot - 1) >> SlotPageBits].Deadlines?[offset / DeadlineBlockSize]?[offset % DeadlineBlockSize] ?? 0;
                return encoded == 0 ? null : encoded - 1;
            }

            internal void SetDeferredDeadline(int slot, long? ticks)
            {
                var index = (slot - 1) >> SlotPageBits;
                var offset = SlotOffset(slot);
                ref var space = ref _space[index];
                if (ticks is not { } deadline)
                {
                    space.ClearDeferredDeadline(offset);
                    return;
                }

                var blocks = space.Deadlines ??= new long[index == 0 ? 1 : SlotsPerPage / DeadlineBlockSize][];
                var block = blocks[offset / DeadlineBlockSize] ??= new long[index == 0 ? InitialBucketCount : DeadlineBlockSize];

                // Zero means no saved deadline, including in a newly allocated block. UTC tick zero is valid.
                block[offset % DeadlineBlockSize] = deadline + 1;
            }

            internal SlotPage? ReadPage(int slot) => Volatile.Read(ref Volatile.Read(ref _pages)[(slot - 1) >> SlotPageBits]);

            internal SlotPage PageFor(int slot) => ReadPage(slot)!;

            internal T? Pack(int slot, ref CacheKey key, T? value)
            {
                if (!PackedSlotPage.Typed)
                    return value;
                var index = (slot - 1) >> SlotPageBits;
                var packed = _space[index].Packed;
                return packed != null ? packed.Pack(_pages[index]!.Records, SlotOffset(slot), ref key, value) : value;
            }

            internal Node Add(CacheKey key, int hash, NodeCacheEntry<T> entry, BoundedNode? policy, int next, out int slot)
            {
                if (_available < 0)
                    AppendPage();

                var index = _available;
                ref var space = ref _space[index];
                var page = _pages[index];
                if (page == null)
                {
                    page = new SlotPage(index == 0 ? InitialBucketCount : SlotsPerPage);
                    Volatile.Write(ref _pages[index], page);
                }

                var offset = space.Free == 0 ? space.Created : space.Free - 1;
                slot = checked((index * SlotsPerPage) + offset + 1);
                var identity = checked(++_lastIdentity);
                ref var record = ref page[offset];
                if (PackedSlotPage.Typed && space.Packed == null && space.Used + 1 == page.Length)
                    space.Packed = PackedSlotPage.Seal(page.Records);
                var raw = Pack(slot, ref key, entry.Value);
                if (space.Free == 0)
                    space.Created++;
                else
                    space.Free = -record.Next;
                if (index == _spare)
                    _spare = -1;
                if (++space.Used == page.Length)
                    _available = space.Next;

                record.Hash = hash;
                Volatile.Write(ref record.Next, next);
                record.Entry = new Cell(key, raw, entry.Version);
                page.SetDeadline(offset, entry.ExpiresUtc);
                page.SetTags(offset, entry.Tags);
                page.SetPolicy(offset, policy);
                Volatile.Write(ref record.Lifetime, identity);
                return new Node(page, slot, identity);
            }

            internal void ReleasePacked(SlotPage page, int offset, int slot) => _space[(slot - 1) >> SlotPageBits].Packed?.Release(page.Records, offset);

            internal void Release(SlotPage page, int offset, int slot)
            {
                var index = (slot - 1) >> SlotPageBits;
                ref var space = ref _space[index];
                space.ClearDeferredDeadline(offset);
                if (space.Used-- == page.Length)
                {
                    space.Next = _available;
                    _available = index;
                }

                Volatile.Write(ref page[offset].Next, -space.Free);
                space.Free = offset + 1;
                if (space.Used != 0 || index == 0)
                    return;

                // Keep one full-size empty page for churn. Other empty pages need only their directory entry.
                if (_spare < 0)
                {
                    _spare = index;
                    return;
                }

                space.Packed = null;
                space.Deadlines = null;
                Volatile.Write(ref _pages[index], null);
                space.Created = 0;
                space.Free = 0;
            }

            private void AppendPage()
            {
                if (_pageCount == _pages.Length)
                {
                    var grown = new SlotPage[_pages.Length * 2];
                    _pages.CopyTo(grown, 0);
                    Array.Resize(ref _space, grown.Length);
                    Volatile.Write(ref _pages, grown);
                }

                _available = _pageCount++;
                _space[_available].Next = -1;
            }

            /// <summary>Writer-owned links between non-full pages and free offsets within a page.</summary>
            [StructLayout(LayoutKind.Auto)]
            private struct PageSpace
            {
                internal int Next;
                internal int Free;
                internal int Created;
                internal int Used;
                internal long[]?[]? Deadlines;
                internal PackedSlotPage? Packed;

                internal readonly void ClearDeferredDeadline(int offset)
                {
                    if (Deadlines?[offset / DeadlineBlockSize] is { } block)
                        block[offset % DeadlineBlockSize] = 0;
                }
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
        internal void MovedLocked(in Node node, DateTime? previous)
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
        internal void RecordLocked(in Node node)
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
        private void Index(in Node node, long ticks)
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
        private bool TryResolve(in Deadline item, out Node node)
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
        /// <param name="Generation">The generation of the entry when the item was pushed; the item is stale unless it equals <see cref="Node.IndexedGeneration" />.</param>
        [Immutable]
        [StructLayout(LayoutKind.Auto)]
        private readonly record struct Deadline(long Identity, long Ticks, int Slot, int Generation);
    }

    [Immutable]
    private sealed class PhysicalCacheRawReader : ILocalCacheRawReader<T>
    {
        private readonly PhysicalCache<T> _owner;

        internal PhysicalCacheRawReader(PhysicalCache<T> owner)
        {
            _owner = owner;
        }

        public NodeCacheEntry<T>? GetEntryRaw(CacheKey key, CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();
            return _owner.TryRead(key, false, out var read) ? read.ToEntry() : null;
        }
    }
}
