using System;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.Storage.Replication;

namespace Squirix.Server.UnitTests.Support;

/// <summary>Follower-log fault hooks that park the pool thread of the next armed durable write until the test releases it.</summary>
/// <remarks>
/// Stands in for a flush that never returns: the parked thread ignores cancellation and dispose, like a stuck fsync. The stall
/// is one-shot, so later writes of the same log run normally. Release the stall and await <see cref="Exited" /> before disposing.
/// </remarks>
[ThreadSafe]
internal sealed class StallableFollowerLogFaultHooks : IFollowerLogFaultHooks, IDisposable
{
    private const int Idle = 0;
    private const int FrameArmed = 1;
    private const int MetaArmed = 2;
    private const int Fired = 3;

    private readonly TaskCompletionSource _entered = new(TaskCreationOptions.RunContinuationsAsynchronously);
    private readonly TaskCompletionSource _exited = new(TaskCreationOptions.RunContinuationsAsynchronously);
    private readonly TaskCompletionSource _flushed = new(TaskCreationOptions.RunContinuationsAsynchronously);
    private readonly ManualResetEventSlim _release = new(false);
    private Exception? _lateFailure;
    private int _state = Idle;

    /// <summary>Gets a task that completes once the armed write parked its pool thread.</summary>
    internal Task Entered => _entered.Task;

    /// <summary>Gets a task that completes once the released pool thread left the stall, normally or by throwing the late failure.</summary>
    internal Task Exited => _exited.Task;

    /// <summary>Gets a task that completes once a log flush after the released stall returned, which proves the handle stayed open.</summary>
    internal Task Flushed => _flushed.Task;

    /// <inheritdoc />
    public void Dispose() => _release.Dispose();

    /// <inheritdoc />
    public void OnBeforeMemoryApply()
    {
    }

    /// <inheritdoc />
    public void OnCommitAdvanced()
    {
    }

    /// <inheritdoc />
    public void OnFlushed()
    {
        if (Volatile.Read(ref _state) == Fired)
            _ = _flushed.TrySetResult();
    }

    /// <inheritdoc />
    public void OnFrameWritten() => Stall(FrameArmed);

    /// <inheritdoc />
    public void OnMetaWritten() => Stall(MetaArmed);

    /// <summary>Releases the parked pool thread, which then finishes its write normally.</summary>
    internal void Release() => _release.Set();

    /// <summary>Releases the parked pool thread, which then fails its write with <paramref name="failure" />.</summary>
    /// <param name="failure">The late failure thrown on the pool thread.</param>
    internal void ReleaseWithFailure(Exception failure)
    {
        Volatile.Write(ref _lateFailure, failure);
        _release.Set();
    }

    /// <summary>Arms the stall for the next frame write, before its flush.</summary>
    internal void StallNextFrameWrite() => Volatile.Write(ref _state, FrameArmed);

    /// <summary>Arms the stall for the next metadata write, before its flush and publication.</summary>
    internal void StallNextMetaWrite() => Volatile.Write(ref _state, MetaArmed);

    private void Stall(int point)
    {
        if (Interlocked.CompareExchange(ref _state, Fired, point) != point)
            return;

        try
        {
            _ = _entered.TrySetResult();

            // A stuck fsync honors no token, so the parked thread waits for the test alone.
            _release.Wait(CancellationToken.None);
            if (Volatile.Read(ref _lateFailure) is { } failure)
                throw failure;
        }
        finally
        {
            _ = _exited.TrySetResult();
        }
    }
}
