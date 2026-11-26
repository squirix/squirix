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

    /// <summary>Stable page shared by readers. Each optional column is allocated by the writer on its first use.</summary>
    private sealed class SlotPage
    {
        private Payload[]? _plain;
        private PackedSlotPage? _packed;
        private long[]? _deadlines;
        private int[]? _generations;
        private BoundedNode?[]? _policies;
        private FrozenDictionary<string, string>?[]? _tags;

        internal SlotPage(int length)
        {
            Records = new SlotRecord[length];
            _plain = new Payload[length];
        }

        internal SlotRecord[] Records { get; }

        internal int Length => Records.Length;

        internal ref SlotRecord this[int offset] => ref Records[offset];

        internal Payload ReadPayload(int offset)
        {
            // Capture the plain array before observing the sealed representation. A reader that
            // started before sealing can finish on the original, immutable array.
            var plain = Volatile.Read(ref _plain);
            return Volatile.Read(ref _packed) is { } packed ? packed.Read(offset) : plain![offset];
        }

        internal void StorePayload(int offset, Payload payload)
        {
            if (_packed is { } packed)
                packed.Write(offset, payload);
            else
                _plain![offset] = payload;
        }

        internal void PrepareRemove(int offset) => _packed?.PrepareRemove(offset);

        internal void RemovePayload(int offset)
        {
            if (_packed is { } packed)
                packed.Remove(offset);
            else
                _plain![offset] = default;
        }

        internal void Seal()
        {
            if (!PackedSlotPage.Typed || _packed != null)
                return;
            var packed = PackedSlotPage.Seal(this, _plain!);
            Volatile.Write(ref _packed, packed);
            Volatile.Write(ref _plain, null);
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
