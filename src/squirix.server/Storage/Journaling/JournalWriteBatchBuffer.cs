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

    /// <summary>Allocated when a batch first needs to combine two frames, then doubled on demand up to the configured capacity.</summary>
    private byte[]? _buffer;

    internal JournalWriteBatchBuffer(int capacityBytes = DefaultCapacityBytes)
    {
        if (capacityBytes <= 0)
            throw new ArgumentOutOfRangeException(nameof(capacityBytes), capacityBytes, "capacity must be greater than zero.");

        _capacityBytes = capacityBytes;
    }

    /// <summary>Gets the bytes for the synchronous write. A single frame stays owned by the pending append registry.</summary>
    internal ReadOnlySpan<byte> ActiveSpan => _pending.Count == 1
        ? _pending[0].FrameBytes.AsSpan(0, StagedByteLength)
        : _buffer.AsSpan(0, StagedByteLength);

    /// <summary>Gets the allocated coalescing array length, or zero before the first multi-frame batch.</summary>
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
        if (frameLength <= 0 || frameLength > _capacityBytes - StagedByteLength)
            return false;

        var frame = ThrowHelper.Required(item.FrameBytes, "Append work item is missing frame bytes.").AsSpan(0, frameLength);
        if (!IsEmpty)
        {
            var buffer = GetOrAllocateBuffer(StagedByteLength + frameLength);
            if (_pending.Count == 1 || buffer != _buffer)
                ActiveSpan.CopyTo(buffer);

            frame.CopyTo(buffer.AsSpan(StagedByteLength));
            _buffer = buffer;
        }

        _pending.Add(item);
        StagedByteLength += frameLength;
        return true;
    }

    /// <summary>Returns the existing array or allocates a larger one without changing the staged batch.</summary>
    /// <param name="requiredBytes">Minimum number of bytes the backing array must hold.</param>
    /// <returns>The backing array to stage into.</returns>
    private byte[] GetOrAllocateBuffer(int requiredBytes)
    {
        var current = _buffer;
        if (current != null && current.Length >= requiredBytes)
            return current;

        int doubled;
        if (current == null)
            doubled = Math.Min(InitialAllocationBytes, _capacityBytes);
        else
            doubled = current.Length > _capacityBytes / 2 ? _capacityBytes : current.Length * 2;

        return new byte[Math.Max(doubled, requiredBytes)];
    }
}
