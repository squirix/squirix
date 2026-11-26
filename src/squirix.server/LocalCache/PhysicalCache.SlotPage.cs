using System;
using System.Collections.Frozen;
using System.Threading;
using Squirix.Server.Core;

namespace Squirix.Server.LocalCache;

internal sealed partial class PhysicalCache<T>
{
    private readonly record struct Payload(CacheKey Key, T? Value)
    {
        internal CacheKey FullKey => IsPacked ? new CacheKey(Key.Namespace, PackedSlotPage.KeyText(Value)) : Key;

        private bool IsPacked => PackedSlotPage.Typed && PackedSlotPage.AsStored(Value).IsPageRecord;

        internal bool Matches(CacheKey key) => IsPacked
            ? string.Equals(Key.Namespace, key.Namespace, StringComparison.Ordinal) && PackedSlotPage.Matches(Value, key.Key)
            : Key.Equals(key);
    }

    /// <summary>First occupants use the page's reserved identity range. A recycled page records every subsequent occupant explicitly.</summary>
    private struct PageLifetimes
    {
        private readonly long _identityBase;
        private volatile long[]? _identities;
        private volatile int _published;

        internal PageLifetimes(long identityBase)
        {
            _identityBase = identityBase;
        }

        internal readonly long Read(int offset)
        {
            var identities = _identities;
            return identities != null ? Volatile.Read(ref identities[offset]) : InitialIdentity(offset);
        }

        internal readonly long Next(int offset, ref long lastIdentity) =>
            _identities == null ? _identityBase + offset + 1 : checked(++lastIdentity);

        internal void Publish(int offset, long identity)
        {
            if (_identities is { } identities)
                Volatile.Write(ref identities[offset], identity);
            else
                _published = offset + 1;
        }

        internal void PrepareReuse(int length)
        {
            if (_identities != null)
                return;

            var identities = new long[length];
            for (var i = 0; i < _published; i++)
                identities[i] = _identityBase + i + 1;
            _identities = identities;
        }

        internal readonly void Retire(int offset) => Interlocked.Exchange(ref _identities![offset], 0);

        private readonly long InitialIdentity(int offset) => offset < _published ? _identityBase + offset + 1 : 0;
    }

    private struct PageCharges
    {
        private long[]? _charges;
        private long _commonCharge;
        private bool _hasCharge;

        internal readonly long Get(int offset) => _charges is { } charges ? charges[offset] : _commonCharge;

        internal void Set(int offset, long bytes, int length)
        {
            if (!_hasCharge)
            {
                _commonCharge = bytes;
                _hasCharge = true;
            }

            if (_charges == null)
            {
                if (bytes == _commonCharge)
                    return;

                _charges = new long[length];
                Array.Fill(_charges, _commonCharge);
            }

            _charges[offset] = bytes;
        }
    }

    /// <summary>Stable page shared by readers. Each optional column is allocated by the writer on its first use.</summary>
    private sealed class SlotPage
    {
        private readonly int[] _hashes;
        private Cell _entry;
        private PageCharges _charges;
        private PageLifetimes _lifetimes;
        private Payload[]? _plain;
        private PackedSlotPage? _packed;
        private long[]? _versions;
        private long[]? _deadlines;
        private int[]? _generations;
        private BoundedNode?[]? _policies;
        private FrozenDictionary<string, string>?[]? _tags;

        internal SlotPage(int length, long identityBase)
        {
            _lifetimes = new PageLifetimes(identityBase);
            _hashes = new int[length];
            _plain = new Payload[length];
        }

        internal ref PageCharges Charges => ref _charges;

        internal ref PageLifetimes Lifetimes => ref _lifetimes;

        internal ref Cell Entry => ref _entry;

        internal int Length => _hashes.Length;

        internal ref int this[int offset] => ref _hashes[offset];

        internal Payload ReadPayload(int offset)
        {
            // Capture the plain array before observing the sealed representation. A reader that
            // started before sealing can finish on the original, immutable array.
            var plain = Volatile.Read(ref _plain);
            return Volatile.Read(ref _packed) is { } packed ? packed.Read(offset) : plain![offset];
        }

        internal void StorePayload(int offset, Payload payload, bool full = false)
        {
            if (_packed is { } packed)
            {
                packed.Write(offset, payload);
            }
            else if (full && PackedSlotPage.Typed)
            {
                var sealedPage = PackedSlotPage.Seal(_plain!, offset, payload);
                Volatile.Write(ref _packed, sealedPage);
                Volatile.Write(ref _plain, null);
            }
            else
            {
                _plain![offset] = payload;
            }
        }

        internal void PrepareRemove(int offset)
        {
            Lifetimes.PrepareReuse(Length);
            _packed?.PrepareRemove(offset);
        }

        internal void RemovePayload(int offset)
        {
            if (_packed is { } packed)
                packed.Remove(offset);
            else
                _plain![offset] = default;
        }

        internal long Version(int offset)
        {
            var versions = Volatile.Read(ref _versions);
            return versions == null ? 1 : Volatile.Read(ref versions[offset]);
        }

        internal void PrepareVersion(long version)
        {
            if (version == 1 || _versions != null)
                return;

            var versions = new long[Length];
            Array.Fill(versions, 1L);
            Volatile.Write(ref _versions, versions);
        }

        internal void SetVersion(int offset, long version)
        {
            if (_versions is { } versions)
                Volatile.Write(ref versions[offset], version);
        }

        internal DateTime? Deadline(int offset) => Volatile.Read(ref _deadlines) is { } values
            ? EntryDeadline.Decode(~Volatile.Read(ref values[offset]))
            : null;

        internal void PrepareMetadata(DateTime? deadline, FrozenDictionary<string, string>? tags, BoundedNode? policy = null)
        {
            _ = Column(ref _deadlines, deadline != null);
            _ = Column(ref _tags, tags != null);
            _ = Column(ref _policies, policy != null);
            _ = Column(ref _generations, deadline != null);
        }

        internal void SetDeadline(int offset, DateTime? value)
        {
            if (Column(ref _deadlines, value != null) is { } values)
                Volatile.Write(ref values[offset], ~EntryDeadline.Encode(value));
        }

        internal int Generation(int offset) => _generations?[offset] ?? 0;

        internal void SetGeneration(int offset, int value)
        {
            if (Column(ref _generations, value != 0) is { } values)
                values[offset] = value;
        }

        internal FrozenDictionary<string, string>? Tags(int offset) => Volatile.Read(ref _tags) is { } values ? Volatile.Read(ref values[offset]) : null;

        internal void SetTags(int offset, FrozenDictionary<string, string>? tags)
        {
            if (Column(ref _tags, tags != null) is { } values)
                Volatile.Write(ref values[offset], tags);
        }

        internal BoundedNode? Policy(int offset) => Volatile.Read(ref _policies) is { } values ? Volatile.Read(ref values[offset]) : null;

        internal void SetPolicy(int offset, BoundedNode? policy)
        {
            if (Column(ref _policies, policy != null) is { } values)
                Volatile.Write(ref values[offset], policy);
        }

        internal void Clear(int offset)
        {
            SetDeadline(offset, null);
            SetGeneration(offset, 0);
            SetTags(offset, null);
            SetPolicy(offset, null);
        }

        private TField[]? Column<TField>(ref TField[]? column, bool needed)
        {
            if (needed && column == null)
                Volatile.Write(ref column, new TField[Length]);
            return column;
        }
    }
}
