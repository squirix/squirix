using System;
using System.Collections.Frozen;
using System.Threading;
using Squirix.Server.Attributes;
using Squirix.Server.Core;

namespace Squirix.Server.LocalCache;

/// <summary>Pairs a page position with its occupant’s identity so a recycled position invalidates the old handle.</summary>
/// <typeparam name="T">The stored value type.</typeparam>
[Immutable]
internal readonly record struct CacheSlot<T>
{
    private readonly SlotPage<T> _cells;

    internal CacheSlot(SlotPage<T> cells, int slot, long identity)
    {
        _cells = cells;
        Identity = identity;
        Slot = slot;
    }

    internal int Slot { get; }

    internal long Identity { get; }

    internal bool Exists => Slot != 0;

    internal CacheKey Key => RawPayload.FullKey;

    internal EvictionNode<T>? Policy => _cells.Policy(Offset);

    internal int Hash => _cells[Offset];

    internal long Bytes
    {
        get => _cells.Charge(Offset);
        set => _cells.SetCharge(Offset, value);
    }

    internal int IndexedGeneration
    {
        get => _cells.Generation(Offset);
        set => _cells.SetGeneration(Offset, value);
    }

    internal DateTime? ExpiresUtc => _cells.Deadline(Offset);

    internal T? Value => SlotPayload<T>.Unpack(RawPayload.Value);

    internal long Version => _cells.Version(Offset);

    internal FrozenDictionary<string, string>? Tags => _cells.Tags(Offset);

    private bool IsStored => _cells.ReadIdentity(Offset) == Identity;

    private SlotPayload<T> RawPayload => _cells.ReadPayload(Offset);

    private int Offset => SlotPageBase.OffsetOf(Slot);

    internal CacheEntryRead<T> ReadLocked() => new(Value, Version, ExpiresUtc, Tags);

    internal bool MatchesPublished(CacheKey key, ref bool unsure)
    {
        var sequence = _cells.Sequence;
        var payload = RawPayload;
        Interlocked.MemoryBarrier();
        if ((sequence & 1) == 0 && _cells.Sequence == sequence && IsStored)
            return payload.Matches(key);

        unsure = true;
        return false;
    }

    internal CacheReadOutcome Read(out CacheEntryRead<T> read)
    {
        var spinner = default(SpinWait);
        while (!TryReadOnce(out read))
        {
            if (spinner.NextSpinWillYield)
                return CacheReadOutcome.Unsure;
            spinner.SpinOnce();
        }

        if (!IsStored)
            return CacheReadOutcome.Unsure;
        if (typeof(T) == typeof(StoredValue))
            read = read with { Value = SlotPayload<T>.Unpack(read.Value) };
        return CacheReadOutcome.Present;
    }

    internal void Write(CacheKey key, T? value, long version, DateTime? expires, FrozenDictionary<string, string>? tags)
    {
        _cells.PrepareVersion(version);
        _cells.PrepareMetadata(expires, tags);
        _cells.Write(Offset, key, value, version, expires, tags);
    }

    internal void WriteValue(CacheKey key, T? value) => _cells.WriteValue(Offset, key, value);

    internal void WriteExpiresUtc(DateTime? expires)
    {
        _cells.PrepareMetadata(expires, null);
        _cells.WriteExpiresUtc(Offset, expires);
    }

    internal void RecordAccess()
    {
        if (Policy is { } bounded && IsStored)
            bounded.Scope.Touch(bounded);
    }

    internal void MarkRemoved()
    {
        _cells.RetireIdentity(Offset);
        _cells.Clear(Offset);
    }

    /// <summary>Reads the fields once: the sequence, the fields and the sequence again.</summary>
    /// <param name="read">The fields of one write, or the default when the read must be retried.</param>
    /// <returns><see langword="false" /> when a write overlapped the read.</returns>
    /// <remarks>Each read is an acquire read, so none moves past the next; a value-type value is read plainly and fenced before the second sequence read.</remarks>
    private bool TryReadOnce(out CacheEntryRead<T> read)
    {
        read = default;
        var sequence = _cells.Sequence;
        if ((sequence & 1) != 0)
            return false;

        var value = RawPayload.Value;
        var version = Version;
        var expires = ExpiresUtc;
        var tags = Tags;
        Interlocked.MemoryBarrier();

        if (_cells.Sequence != sequence)
            return false;

        read = new CacheEntryRead<T>(value, version, expires, tags);

        return true;
    }
}
