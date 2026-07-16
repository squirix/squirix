using System;
using System.Collections.Generic;

namespace Squirix.Server.Storage.Journaling;

/// <summary>Coalesces journal frames on the I/O thread before a single segment write.</summary>
internal sealed class JournalWriteBatchBuffer
{
    /// <summary>Default coalescing buffer capacity when no explicit size is configured.</summary>
    private const int DefaultCapacityBytes = 16 * 1024 * 1024;

    private readonly int _capacityBytes;
    private readonly List<JournalWorkItem> _pending = [];

    /// <summary>
    /// Allocated lazily on first staging so idle coordinators (and the many created in tests) do not
    /// each hold a multi-megabyte buffer.
    /// </summary>
    private byte[]? _buffer;

    internal JournalWriteBatchBuffer(int capacityBytes = DefaultCapacityBytes)
    {
        if (capacityBytes <= 0)
            throw new ArgumentOutOfRangeException(nameof(capacityBytes), capacityBytes, "capacity must be greater than zero.");

        _capacityBytes = capacityBytes;
    }

    internal ReadOnlySpan<byte> ActiveSpan => _buffer is null ? ReadOnlySpan<byte>.Empty : _buffer.AsSpan(0, StagedByteLength);

    internal bool IsEmpty => StagedByteLength is 0;

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

        var frameBytes = item.FrameBytes ?? throw new InvalidOperationException("Append work item is missing frame bytes.");
        var buffer = _buffer ??= new byte[_capacityBytes];
        frameBytes.AsSpan(0, frameLength).CopyTo(buffer.AsSpan(StagedByteLength));
        _pending.Add(item);
        StagedByteLength += frameLength;
        return true;
    }
}
