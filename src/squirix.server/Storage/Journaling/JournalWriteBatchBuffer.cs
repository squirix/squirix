using System;
using System.Collections.Generic;
using Squirix.Server.Utils;

namespace Squirix.Server.Storage.Journaling;

/// <summary>Coalesces journal frames on the I/O thread before a single segment write.</summary>
internal sealed class JournalWriteBatchBuffer
{
    /// <summary>Default coalescing buffer capacity when no explicit size is configured.</summary>
    private const int DefaultCapacityBytes = 16 * 1024 * 1024;

    /// <summary>Size of the first backing array, below the large-object-heap threshold.</summary>
    private const int InitialAllocationBytes = 64 * 1024;

    private readonly int _capacityBytes;
    private readonly List<JournalWorkItem> _pending = [];

    /// <summary>
    /// Allocated lazily on first staging and doubled on demand up to the configured capacity, so idle coordinators
    /// (and the many created in tests) do not each hold a multi-megabyte buffer. Never shrinks.
    /// </summary>
    private byte[]? _buffer;

    internal JournalWriteBatchBuffer(int capacityBytes = DefaultCapacityBytes)
    {
        if (capacityBytes <= 0)
            throw new ArgumentOutOfRangeException(nameof(capacityBytes), capacityBytes, "capacity must be greater than zero.");

        _capacityBytes = capacityBytes;
    }

    internal ReadOnlySpan<byte> ActiveSpan => _buffer == null ? ReadOnlySpan<byte>.Empty : _buffer.AsSpan(0, StagedByteLength);

    /// <summary>Gets the current backing array length, or zero before the first staged append.</summary>
    internal int AllocatedLength => _buffer?.Length ?? 0;

    internal bool IsEmpty => StagedByteLength == 0;

    internal IReadOnlyList<JournalWorkItem> PendingAppends => _pending;

    internal int StagedByteLength { get; private set; }

    internal void Clear()
    {
        StagedByteLength = 0;
        _pending.Clear();
    }

    internal bool TryStageAppend(in JournalWorkItem item)
    {
        var frameLength = item.FrameLength;
        if (frameLength <= 0 || StagedByteLength + frameLength > _capacityBytes)
            return false;

        var frameBytes = ThrowHelper.Required(item.FrameBytes, "Append work item is missing frame bytes.");
        var buffer = EnsureCapacity(StagedByteLength + frameLength);
        frameBytes.AsSpan(0, frameLength).CopyTo(buffer.AsSpan(StagedByteLength));
        _pending.Add(item);
        StagedByteLength += frameLength;
        return true;
    }

    /// <summary>
    /// Returns a backing array of at least <paramref name="requiredBytes" /> bytes, never above the configured capacity. The array
    /// is replaced only after the new one is allocated, so an allocation failure leaves the state unchanged.
    /// </summary>
    /// <param name="requiredBytes">Minimum number of bytes the backing array must hold.</param>
    /// <returns>The backing array to stage into.</returns>
    private byte[] EnsureCapacity(int requiredBytes)
    {
        var current = _buffer;
        if (current != null && current.Length >= requiredBytes)
            return current;

        int doubled;
        if (current == null)
            doubled = Math.Min(InitialAllocationBytes, _capacityBytes);
        else
            doubled = current.Length > _capacityBytes / 2 ? _capacityBytes : current.Length * 2;

        _buffer = new byte[Math.Max(doubled, requiredBytes)];
        if (current != null && StagedByteLength > 0)
            current.AsSpan(0, StagedByteLength).CopyTo(_buffer);

        return _buffer;
    }
}
