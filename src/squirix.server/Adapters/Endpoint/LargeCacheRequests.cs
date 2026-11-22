using System;
using System.IO;
using System.Threading;
using Squirix.Server.Core;
using Squirix.Server.Errors;
using Squirix.Transport.Binary;

namespace Squirix.Server.Adapters.Endpoint;

/// <summary>Admits extended client requests and bounds their buffers across the node.</summary>
internal sealed class LargeCacheRequests
{
    private readonly long _budget;
    private readonly int _peerFrameLimit;
    private long _pending;
    private long _peerPending;

    internal LargeCacheRequests(bool replicated, long memoryBudget)
    {
        ValueLimit = EntryLimits.ValueBytes(replicated);
        _budget = Math.Min(memoryBudget, ValueLimit + Frames.MaxEnvelopeBytes);
        _peerFrameLimit = replicated ? ValueLimit + (2 * Frames.MaxEnvelopeBytes) : Frames.MaxExtendedBodyBytes;
    }

    internal int ValueLimit { get; }

    internal BinaryResponse? Admit(int size, int valueBytes)
    {
        if (valueBytes < 0 || valueBytes > size || size - valueBytes > Frames.MaxEnvelopeBytes)
            throw new InvalidDataException("Invalid extended cache request dimensions.");

        if (valueBytes > ValueLimit)
            return BinaryResponse.FromError(ServerOpContract.PayloadTooLarge(ValueLimit));

        var pending = Interlocked.Add(ref _pending, size);
        if (pending <= _budget)
            return null;

        Release(size);
        return BinaryResponse.FromError(ServerOpContract.MemoryPressure());
    }

    internal void Release(int size) => _ = Interlocked.Add(ref _pending, -size);

    internal bool TryReservePeerFrame(int size)
    {
        if (size > _peerFrameLimit)
            throw new InvalidDataException("Extended peer frame exceeds the transport envelope.");

        var pending = Interlocked.Add(ref _peerPending, size);
        if (pending <= _peerFrameLimit && pending <= _budget + Frames.MaxEnvelopeBytes)
            return true;

        ReleasePeerFrame(size);
        return false;
    }

    internal void ReleasePeerFrame(int size) => _ = Interlocked.Add(ref _peerPending, -size);
}
