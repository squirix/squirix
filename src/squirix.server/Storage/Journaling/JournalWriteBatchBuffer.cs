using System;
using System.Collections.Generic;

namespace Squirix.Server.Storage.Journaling;

/// <summary>Coalesces journal frames on the I/O thread before a single segment write.</summary>
internal sealed class JournalWriteBatchBuffer
{
    /// <summary>Default coalescing buffer capacity when no explicit size is configured.</summary>
    internal const int DefaultCapacityBytes = 16 * 1024 * 1024;

    private readonly int _capacityBytes;
    private readonly List<PendingAppend> _pending = [];

    // Allocated lazily on first staging so idle coordinators (and the many created in tests) do not
    // each hold a multi-megabyte buffer.
    private byte[]? _buffer;

    public JournalWriteBatchBuffer(int capacityBytes = DefaultCapacityBytes)
    {
        if (capacityBytes <= 0)
            throw new ArgumentOutOfRangeException(nameof(capacityBytes), capacityBytes, "capacity must be greater than zero.");

        _capacityBytes = capacityBytes;
    }

    public bool IsEmpty => StagedByteLength is 0;

    public int StagedByteLength { get; private set; }

    public ReadOnlySpan<byte> ActiveSpan => _buffer is null ? ReadOnlySpan<byte>.Empty : _buffer.AsSpan(0, StagedByteLength);

    public IReadOnlyList<PendingAppend> PendingAppends => _pending;

    public bool TryStageAppend(in JournalWorkItem item)
    {
        var frameLength = item.FrameLength;
        if (frameLength <= 0 || StagedByteLength + frameLength > _capacityBytes)
            return false;

        var frameBytes = item.FrameBytes ?? throw new InvalidOperationException("Append work item is missing frame bytes.");
        var buffer = _buffer ??= new byte[_capacityBytes];
        frameBytes.AsSpan(0, frameLength).CopyTo(buffer.AsSpan(StagedByteLength));
        _pending.Add(new PendingAppend(item));
        StagedByteLength += frameLength;
        return true;
    }

    public void Clear()
    {
        StagedByteLength = 0;
        _pending.Clear();
    }

    internal readonly record struct PendingAppend(JournalWorkItem Item);
}
