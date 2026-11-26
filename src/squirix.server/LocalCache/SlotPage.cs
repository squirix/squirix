using System;
using System.Collections.Frozen;
using System.Threading;
using Squirix.Server.Core;

namespace Squirix.Server.LocalCache;

/// <summary>Stable page shared by readers. Each optional column is allocated by the writer on its first use.</summary>
/// <typeparam name="T">The stored value type.</typeparam>
internal sealed class SlotPage<T> : SlotPageBase
{
    private SlotPayload<T>[]? _plain;
    private PackedSlotPage<T>? _packed;
    private long[]? _versions;
    private long[]? _deadlines;
    private int[]? _generations;
    private EvictionNode<T>?[]? _policies;
    private FrozenDictionary<string, string>?[]? _tags;

    internal SlotPage(int length, long identityBase)
        : base(length, identityBase)
    {
        _plain = new SlotPayload<T>[length];
    }

    internal SlotPayload<T> ReadPayload(int offset)
    {
        // Capture the plain array before checking the writer’s sealed representation. A reader that
        // started before sealing can finish on the original, immutable array.
        var plain = Volatile.Read(ref _plain);
        return Volatile.Read(ref _packed) is { } packed ? packed.Read(offset) : plain![offset];
    }

    internal void StorePayload(int offset, SlotPayload<T> payload, bool full = false)
    {
        if (_packed is { } packed)
        {
            packed.Write(offset, payload);
        }
        else if (full && typeof(T) == typeof(StoredValue))
        {
            var sealedPage = PackedSlotPage<T>.Seal(_plain!, offset, payload);
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
        PrepareReuse(offset);
        _packed?.PrepareRemove(offset);
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

    internal void PrepareMetadata(DateTime? deadline, FrozenDictionary<string, string>? tags, EvictionNode<T>? policy = null)
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

    internal EvictionNode<T>? Policy(int offset) => Volatile.Read(ref _policies) is { } values ? Volatile.Read(ref values[offset]) : null;

    internal void SetPolicy(int offset, EvictionNode<T>? policy)
    {
        if (Column(ref _policies, policy != null) is { } values)
            Volatile.Write(ref values[offset], policy);
    }

    internal void Clear(int offset)
    {
        if (_packed is { } packed)
            packed.Remove(offset);
        else
            _plain![offset] = default;

        SetDeadline(offset, null);
        SetGeneration(offset, 0);
        SetTags(offset, null);
        SetPolicy(offset, null);
    }

    /// <summary>Replaces what a read returns in one write; call under the cache lock.</summary>
    /// <param name="offset">The slot whose optional fields participate in this read or write.</param>
    /// <param name="key">The storage key representation for this value.</param>
    /// <param name="value">The cached value.</param>
    /// <param name="version">The version.</param>
    /// <param name="expiresUtc">The deadline, or <see langword="null" /> for none.</param>
    /// <param name="tags">The tags.</param>
    internal void Write(int offset, CacheKey key, T? value, long version, DateTime? expiresUtc, FrozenDictionary<string, string>? tags)
    {
        var odd = BeginWrite();
        try
        {
            StorePayload(offset, new SlotPayload<T>(key, value));
            SetVersion(offset, version);
            SetDeadline(offset, expiresUtc);
            SetTags(offset, tags);
        }
        finally
        {
            EndWrite(odd);
        }
    }

    /// <summary>Replaces the deadline; call under the cache lock.</summary>
    /// <param name="offset">The slot whose optional fields participate in this read or write.</param>
    /// <param name="expiresUtc">The deadline, or <see langword="null" /> for none.</param>
    internal void WriteExpiresUtc(int offset, DateTime? expiresUtc)
    {
        var odd = BeginWrite();
        try
        {
            SetDeadline(offset, expiresUtc);
        }
        finally
        {
            EndWrite(odd);
        }
    }

    /// <summary>Replaces the value; call under the cache lock.</summary>
    /// <param name="offset">The slot being replaced.</param>
    /// <param name="key">The storage key representation for this value.</param>
    /// <param name="value">The cached value.</param>
    internal void WriteValue(int offset, CacheKey key, T? value)
    {
        var odd = BeginWrite();
        try
        {
            StorePayload(offset, new SlotPayload<T>(key, value));
        }
        finally
        {
            EndWrite(odd);
        }
    }

    private TField[]? Column<TField>(ref TField[]? column, bool needed)
    {
        if (needed && column == null)
            Volatile.Write(ref column, new TField[Length]);
        return column;
    }
}
