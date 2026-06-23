using System;
using System.Collections.Generic;

namespace Squirix.Server.Storage.Journaling;

/// <summary>Coalesces journal frames on the I/O thread before a single segment write.</summary>
internal sealed class JournalWriteBatchBuffer
{
    private const int CapacityBytes = 16 * 1024 * 1024;

    private readonly byte[] _buffer = new byte[CapacityBytes];
    private readonly List<PendingAppend> _pending = [];

    public bool IsEmpty => StagedByteLength is 0;

    public int StagedByteLength { get; private set; }

    public ReadOnlySpan<byte> ActiveSpan => _buffer.AsSpan(0, StagedByteLength);

    public IReadOnlyList<PendingAppend> PendingAppends => _pending;

    public bool TryStageAppend(in JournalWorkItem item)
    {
        var frameLength = item.FrameLength;
        if (frameLength <= 0 || StagedByteLength + frameLength > CapacityBytes)
            return false;

        var frameBytes = item.FrameBytes ?? throw new InvalidOperationException("Append work item is missing frame bytes.");
        frameBytes.AsSpan(0, frameLength).CopyTo(_buffer.AsSpan(StagedByteLength));
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
