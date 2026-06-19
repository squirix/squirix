using System;
using System.Threading;
using System.Threading.Tasks;

namespace Squirix.Server.Storage.Journaling.PipelinedWal.Backends.Pipelined;

internal readonly struct WalWorkItem
{
    public WalWorkKind Kind { get; init; }

    public byte[]? FrameBytes { get; init; }

    public int FrameLength { get; init; }

    public TaskCompletionSource? Completion { get; init; }

    public int ResetSegmentIndex { get; init; }

    public ulong ResetSequence { get; init; }
}
