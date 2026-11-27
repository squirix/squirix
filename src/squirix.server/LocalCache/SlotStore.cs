using System;
using System.Numerics;
using System.Runtime.InteropServices;
using System.Threading;
using Squirix.Server.Attributes;
using Squirix.Server.Core;

namespace Squirix.Server.LocalCache;

/// <summary>The nodes by key: an open-addressed hash table that writers change under the cache lock and readers search without it.</summary>
/// <typeparam name="T">The stored value type.</typeparam>
/// <remarks>
/// <para>A hit follows bucket → stable slot → occupant. Zero terminates a probe and a negative bucket marks a deletion.
/// Migration moves slot numbers between indexes; it never moves an occupant.
/// Readers retry under the lock when a miss overlaps removal or migration. A hit also validates the occupant lifetime.</para>
/// <para>An insert-only write that replaces an entry past its deadline removes the old node and adds a new one in one lock hold, so a non-live
/// read that found the removed node and missed the added one reports the key absent.</para>
/// </remarks>
[ThreadSafe]
internal sealed class SlotStore<T>
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

    private const long MaximumBuckets = 1L << 31;
    private const int TransferBudget = 8;
    private readonly SlotMap _slots = new();
    private BucketTable _table = new(InitialBucketCount);
    private BucketTable? _previous;
    private int _cursor;
    private long _topologyVersion;

    /// <summary>Gets the number of stored nodes; read under the cache lock.</summary>
    internal int Count { get; private set; }

    /// <summary>Links a node whose key is not stored; call under the cache lock.</summary>
    /// <param name="key">The key.</param>
    /// <param name="hash">The hash used to look for the key.</param>
    /// <param name="entry">The entry to publish.</param>
    /// <param name="policy">Optional eviction metadata.</param>
    /// <param name="bytes">The charge for the new occupant, or <see langword="null" /> when memory accounting is disabled.</param>
    /// <returns>The published occupant.</returns>
    internal CacheSlot<T> AddLocked(CacheKey key, int hash, NodeCacheEntry<T> entry, EvictionNode<T>? policy, long? bytes)
    {
        if (_previous == null && _table.NeedsRebuild(Count))
        {
            var length = Count >= _table.Length / 2 && _table.Length < MaximumBuckets ? _table.Length * 2 : _table.Length;
            var grown = new BucketTable(length);
            Volatile.Write(ref _previous, _table);
            Volatile.Write(ref _table, grown);
        }

        ref var head = ref _table.InsertPosition(hash);
        var node = _slots.Add(key, hash, entry, policy, bytes, out var slot);
        if (head == 0)
            _table.Used++;
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
        var nodes = _slots.GetEnumerator();
        while (nodes.MoveNext())
            keys[index++] = nodes.Current.Key;

        return keys;
    }

    /// <summary>Reads the stored entry of a key without the cache lock.</summary>
    /// <param name="key">The key to read.</param>
    /// <param name="read">The fields of the entry when the outcome is <see cref="CacheReadOutcome.Present" />.</param>
    /// <param name="node">The node observed by the lookup.</param>
    /// <returns>Whether the key holds an entry, or <see cref="CacheReadOutcome.Unsure" /> when a writer got in the way and the caller must read under the lock.</returns>
    internal CacheReadOutcome Read(CacheKey key, out CacheEntryRead<T> read, out CacheSlot<T> node)
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
    /// <returns>The bucket and table generation under the writer lock.</returns>
    internal (int Index, bool Previous) FindLinkLocked(CacheKey key, int hash, long identity, out CacheSlot<T> node)
    {
        var index = FindLink(_table, key, hash, identity, out node);
        if (!node.Exists && _previous is { } previous)
            return (FindLink(previous, key, hash, identity, out node), true);

        return (index, false);
    }

    /// <summary>Unlinks the exact stored node under the cache lock.</summary>
    /// <param name="node">The stored node.</param>
    /// <exception cref="InvalidOperationException">The node has already left the store.</exception>
    internal void RemoveLocked(in CacheSlot<T> node)
    {
        var link = FindLinkLocked(default, node.Hash, node.Identity, out var stored);
        if (!stored.Exists)
            throw new InvalidOperationException("The node is not stored.");

        UnlinkLocked(link, in stored);
    }

    /// <summary>Detaches the occupant after all potentially throwing expiry checks have completed.</summary>
    /// <param name="link">The link returned by the lookup under the same lock.</param>
    /// <param name="node">The occupant reached through the link.</param>
    internal void UnlinkLocked((int Index, bool Previous) link, in CacheSlot<T> node)
    {
        var slots = _slots.PageFor(node.Slot);
        var offset = SlotPageBase.OffsetOf(node.Slot);
        slots.PrepareRemove(offset);
        _ = Interlocked.Increment(ref _topologyVersion);
        if (link.Previous)
            Volatile.Write(ref _previous!.WritableBucket(link.Index), -1);
        else
            _table.Remove(link.Index, _slots);
        _slots.Release(slots, offset, node.Slot);
        _ = Interlocked.Increment(ref _topologyVersion);
        Count--;
    }

    /// <summary>Finds the node in either generation under the cache lock.</summary>
    /// <param name="key">The requested key.</param>
    /// <param name="node">The matching stored node, if present.</param>
    /// <returns>Whether the key is stored.</returns>
    internal bool TryGetLocked(CacheKey key, out CacheSlot<T> node) => TryGetLocked(key, key.GetHashCode(), out node);

    internal long? DeferredDeadline(int slot) => _slots.DeferredDeadline(slot);

    internal void SetDeferredDeadline(int slot, long? ticks) => _slots.SetDeferredDeadline(slot, ticks);

    internal CacheSlot<T> ResolveSlotLocked(int slot, long identity)
    {
        var page = _slots.ReadPage(slot);
        return page != null && page.ReadIdentity(SlotPageBase.OffsetOf(slot)) == identity ? new CacheSlot<T>(page, slot, identity) : default;
    }

    internal bool TryGetLocked(CacheKey key, int hash, out CacheSlot<T> node)
    {
        var unsure = false;
        node = Search(_table, key, hash, ref unsure);
        if (!node.Exists && _previous is { } previous)
            node = Search(previous, key, hash, ref unsure);
        return node.Exists;
    }

    /// <summary>Scans at most eight source buckets per mutation while growing, shrinking, or reclaiming deleted buckets.</summary>
    internal void AdvanceGrowthLocked()
    {
        var table = _table;
        if (_previous == null && table.Length > InitialBucketCount && Count < table.Length / 4)
        {
            var smaller = new BucketTable(table.Length / 2);
            Volatile.Write(ref _previous, table);
            Volatile.Write(ref _table, smaller);
            table = smaller;
        }

        if (_previous is not { } previous)
            return;

        // An odd version makes readers retry misses until both bucket arrays agree.
        var resizing = Interlocked.Increment(ref _topologyVersion);
        try
        {
            for (var work = 0; work < TransferBudget; work++)
            {
                var source = previous.Page(_cursor);
                var index = BucketTable.Offset(_cursor);
                var slot = source?[index] ?? 0;
                if (slot > 0)
                    TransferSlot(table, source!, index, slot);

                if (_cursor + 1L == previous.Length)
                {
                    Volatile.Write(ref _previous, null);
                    _cursor = 0;
                    break;
                }

                _cursor++;
            }
        }
        finally
        {
            Volatile.Write(ref _topologyVersion, resizing + 1);
        }
    }

    private void TransferSlot(BucketTable table, int[] source, int index, int slot)
    {
        // Obtain the destination page before unlinking, so allocation failure leaves the node reachable.
        var page = _slots.PageFor(slot);
        var offset = SlotPageBase.OffsetOf(slot);
        ref var storedHash = ref page[offset];
        ref var destination = ref table.InsertPosition(storedHash);
        if (destination == 0)
            table.Used++;
        Volatile.Write(ref destination, slot);
        Volatile.Write(ref source[index], -1);
    }

    private int FindLink(BucketTable table, CacheKey key, int hash, long identity, out CacheSlot<T> node)
    {
        var index = table.IndexOf(hash);
        for (long visited = 0; visited < table.Length; visited++)
        {
            var slot = table.Head(index);
            if (slot == 0)
                break;

            if (slot > 0)
            {
                var slots = _slots.PageFor(slot);
                var offset = SlotPageBase.OffsetOf(slot);
                ref var storedHash = ref slots[offset];
                if (identity != 0 ? identity == slots.ReadIdentity(offset) : storedHash == hash && slots.ReadPayload(offset).Matches(key))
                {
                    node = new CacheSlot<T>(slots, slot, slots.ReadIdentity(offset));
                    return index;
                }
            }

            index = table.NextIndex(index);
        }

        node = default;
        return -1;
    }

    private CacheSlot<T> Search(BucketTable table, CacheKey key, int hash, ref bool unsure)
    {
        var index = table.IndexOf(hash);
        for (long visited = 0; visited < table.Length; visited++)
        {
            var slot = table.Head(index);
            if (slot == 0)
                break;

            if (slot > 0 && _slots.ReadPage(slot) is { } page)
            {
                ref var storedHash = ref page[SlotPageBase.OffsetOf(slot)];
                var identity = page.ReadIdentity(SlotPageBase.OffsetOf(slot));
                if (identity > 0 && storedHash == hash)
                {
                    var node = new CacheSlot<T>(page, slot, identity);
                    if (node.MatchesPublished(key, ref unsure))
                        return node;
                }
            }

            index = table.NextIndex(index);
        }

        return default;
    }

    /// <summary>Searches for the node of a key without the cache lock.</summary>
    /// <param name="key">The key.</param>
    /// <param name="node">The node found, or <see langword="null" />.</param>
    /// <returns>
    /// <see cref="CacheReadOutcome.Present" /> when found, <see cref="CacheReadOutcome.Absent" /> when not, or <see cref="CacheReadOutcome.Unsure" /> when the
    /// store grew during a search that missed.
    /// </returns>
    private CacheReadOutcome Find(CacheKey key, out CacheSlot<T> node)
    {
        var hash = key.GetHashCode();
        var version = Volatile.Read(ref _topologyVersion);
        var table = Volatile.Read(ref _table);
        var unsure = false;
        node = Search(table, key, hash, ref unsure);
        if (!node.Exists && Volatile.Read(ref _previous) is { } previous)
            node = Search(previous, key, hash, ref unsure);
        if (node.Exists)
            return CacheReadOutcome.Present;
        var stable = !unsure && (version & 1) == 0 && Volatile.Read(ref _topologyVersion) == version;
        return stable ? CacheReadOutcome.Absent : CacheReadOutcome.Unsure;
    }

    /// <summary>Keeps small indexes in one array and allocates pages on demand for larger indexes.</summary>
    private sealed class BucketTable
    {
        private const int PageBits = 13;
        private const int PageSize = 1 << PageBits;
        private readonly int[]?[] _pages;
        private readonly int[]? _buckets;
        private readonly int _hashShift;

        internal BucketTable(long length)
        {
            Length = length;
            _hashShift = HashBits - BitOperations.Log2(uint.CreateChecked(length));
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

        internal long Length { get; }

        internal long Used { get; set; }

        internal static int Offset(int index) => index & (PageSize - 1);

        internal int NextIndex(int index) => unchecked(index + 1) & int.CreateTruncating(Length - 1);

        internal bool NeedsRebuild(int count) => Used >= Length - (Length >> FreeBucketShift) && (Length < MaximumBuckets || Used - count >= Length / 4);

        internal void Remove(int index, SlotMap slots)
        {
            // Close short probe chains in place. A long chain keeps a tombstone for the
            // incremental rebuild so a single removal has bounded work under the writer lock.
            var hole = index;
            var mask = int.CreateTruncating(Length - 1);
            for (var i = 0; i < TransferBudget; i++)
            {
                index = NextIndex(index);
                var slot = Head(index);
                if (slot == 0)
                {
                    Volatile.Write(ref WritableBucket(hole), 0);
                    Used--;
                    return;
                }

                if (slot < 0)
                    continue;
                var hash = slots.PageFor(slot)[SlotPageBase.OffsetOf(slot)];
                var home = IndexOf(hash);
                if (((hole - home) & mask) >= ((index - home) & mask))
                    continue;
                Volatile.Write(ref WritableBucket(hole), slot);
                hole = index;
            }

            Volatile.Write(ref WritableBucket(hole), -1);
        }

        internal ref int InsertPosition(int hash)
        {
            var index = IndexOf(hash);
            for (long visited = 0; visited < Length; visited++)
            {
                if (Head(index) <= 0)
                    return ref WritableBucket(index);
                index = NextIndex(index);
            }

            throw new InvalidOperationException("The slot index is full.");
        }

        internal int IndexOf(int hash) => unchecked(hash * FibonacciMultiplier) >>> _hashShift;

        internal int[]? Page(int index) => _buckets ?? Volatile.Read(ref _pages[index >> PageBits]);

        internal int Head(int index) => Page(index) is { } page ? Volatile.Read(ref page[Offset(index)]) : 0;

        internal ref int WritableBucket(int index)
        {
            if (_buckets != null)
                return ref _buckets[index];

            ref var slot = ref _pages[index >> PageBits];
            var page = slot;
            if (page != null)
                return ref page[Offset(index)];
            page = new int[PageSize];
            Volatile.Write(ref slot, page);

            return ref page[Offset(index)];
        }
    }

    /// <summary>Owns stable slot numbers. Released slots use the cached hash field as the free-list link under the writer lock.</summary>
    private sealed class SlotMap
    {
        private const int DeadlineBlockSize = 64;
        private SlotPage<T>?[] _pages = new SlotPage<T>[1];
        private PageSpace[] _space = new PageSpace[1];
        private int _pageCount;
        private int _available = -1;
        private int _spare = -1;
        private long _lastIdentity;

        internal SlotEnumerator<T> GetEnumerator() => new(_pages, _pageCount);

        internal long? DeferredDeadline(int slot)
        {
            var offset = SlotPageBase.OffsetOf(slot);
            var encoded = _space[(slot - 1) >> SlotPageBase.IndexBits].Deadlines?[offset / DeadlineBlockSize]?[offset % DeadlineBlockSize] ?? 0;
            return encoded == 0 ? null : encoded - 1;
        }

        internal void SetDeferredDeadline(int slot, long? ticks)
        {
            var index = (slot - 1) >> SlotPageBase.IndexBits;
            var offset = SlotPageBase.OffsetOf(slot);
            ref var space = ref _space[index];
            if (ticks is not { } deadline)
            {
                space.ClearDeferredDeadline(offset);
                return;
            }

            var blocks = space.Deadlines;
            if (blocks == null)
            {
                blocks = new long[index == 0 ? 1 : SlotPageBase.Capacity / DeadlineBlockSize][];
                space = space with { Deadlines = blocks };
            }

            var block = blocks[offset / DeadlineBlockSize] ??= new long[index == 0 ? InitialBucketCount : DeadlineBlockSize];

            // Zero means no saved deadline, including in a newly allocated block. UTC tick zero is valid.
            block[offset % DeadlineBlockSize] = deadline + 1;
        }

        internal SlotPage<T>? ReadPage(int slot) => Volatile.Read(ref Volatile.Read(ref _pages)[(slot - 1) >> SlotPageBase.IndexBits]);

        internal SlotPage<T> PageFor(int slot) => ReadPage(slot)!;

        internal CacheSlot<T> Add(CacheKey key, int hash, NodeCacheEntry<T> entry, EvictionNode<T>? policy, long? bytes, out int slot)
        {
            if (_available < 0)
                AppendPage();

            var index = _available;
            ref var space = ref _space[index];
            var page = _pages[index];
            if (page == null)
            {
                var length = index == 0 ? InitialBucketCount : SlotPageBase.Capacity;
                page = new SlotPage<T>(length, _lastIdentity);
                _lastIdentity = checked(_lastIdentity + (2L * length));
                Volatile.Write(ref _pages[index], page);
            }

            // A recycled slot keeps the free-list link in its hash until the payload is ready.
            var offset = space.Free == 0 ? space.Created : space.Free - 1;
            slot = checked((index * SlotPageBase.Capacity) + offset + 1);
            var identity = page.NextIdentity(offset, ref _lastIdentity);
            ref var storedHash = ref page[offset];
            page.PrepareVersion(entry.Version);
            if (bytes is { } charge)
                page.SetCharge(offset, charge);
            page.PrepareMetadata(entry.ExpiresUtc, entry.Tags, policy);
            page.StorePayload(offset, new SlotPayload<T>(key, entry.Value), space.Used + 1 == page.Length);
            space = space.Free == 0
                ? space with { Created = space.Created + 1, Used = space.Used + 1 }
                : space with { Free = storedHash, Used = space.Used + 1 };
            if (index == _spare)
                _spare = -1;
            if (space.Used == page.Length)
                _available = space.Next;

            storedHash = hash;
            page.SetVersion(offset, entry.Version);
            page.SetDeadline(offset, entry.ExpiresUtc);
            page.SetTags(offset, entry.Tags);
            page.SetPolicy(offset, policy);

            // Publish the identity last so readers cannot see a partially initialized occupant.
            page.PublishIdentity(offset, identity);
            return new CacheSlot<T>(page, slot, identity);
        }

        internal void Release(SlotPage<T> page, int offset, int slot)
        {
            var index = (slot - 1) >> SlotPageBase.IndexBits;
            ref var space = ref _space[index];
            space.ClearDeferredDeadline(offset);
            if (space.Used == page.Length)
            {
                space = space with { Next = _available };
                _available = index;
            }

            page[offset] = space.Free;
            space = space with { Free = offset + 1, Used = space.Used - 1 };
            if (space.Used != 0 || index == 0)
                return;

            // Keep one full-size empty page for churn. Other empty pages need only their directory entry.
            if (_spare < 0)
            {
                _spare = index;
                return;
            }

            Volatile.Write(ref _pages[index], null);
            space = new PageSpace(space.Next, 0, 0, 0, null);
        }

        private void AppendPage()
        {
            if (_pageCount == _pages.Length)
            {
                var grown = new SlotPage<T>[_pages.Length * 2];
                _pages.CopyTo(grown, 0);
                Array.Resize(ref _space, grown.Length);
                Volatile.Write(ref _pages, grown);
            }

            _available = _pageCount++;
            _space[_available] = new PageSpace(-1, 0, 0, 0, null);
        }

        /// <summary>Writer-owned links between non-full pages and free offsets within a page.</summary>
        /// <param name="Next">Next non-full page.</param>
        /// <param name="Free">First recycled slot, encoded as offset plus one.</param>
        /// <param name="Created">Number of offsets used at least once on this page.</param>
        /// <param name="Used">Number of current occupants.</param>
        /// <param name="Deadlines">Optional deferred deadlines, allocated in blocks.</param>
        [Immutable]
        [StructLayout(LayoutKind.Auto)]
        private readonly record struct PageSpace(int Next, int Free, int Created, int Used, long[]?[]? Deadlines)
        {
            internal void ClearDeferredDeadline(int offset)
            {
                if (Deadlines?[offset / DeadlineBlockSize] is { } block)
                    block[offset % DeadlineBlockSize] = 0;
            }
        }
    }
}
