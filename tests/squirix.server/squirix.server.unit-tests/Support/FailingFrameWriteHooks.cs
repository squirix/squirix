using System;
using System.IO;
using System.Threading;
using Squirix.Server.Attributes;
using Squirix.Server.Storage.Replication;

namespace Squirix.Server.UnitTests.Support;

/// <summary>Follower-log fault hooks that fail every frame write, or every flush after it, while armed, as a disk that keeps refusing the log bytes.</summary>
/// <remarks>Unlike <see cref="StallableFollowerLogFaultHooks" />, the fault is not one-shot: it repeats until the test disarms it.</remarks>
[ThreadSafe]
internal sealed class FailingFrameWriteHooks : IFollowerLogFaultHooks
{
    private int _failures;
    private Exception? _failure;
    private Exception? _flushFailure;

    /// <summary>Gets how many frame writes and flushes the hooks failed.</summary>
    internal int Failures => Volatile.Read(ref _failures);

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
        if (Volatile.Read(ref _flushFailure) is not { } failure)
            return;

        _ = Interlocked.Increment(ref _failures);
        throw failure;
    }

    /// <inheritdoc />
    public void OnFrameWritten()
    {
        if (Volatile.Read(ref _failure) is not { } failure)
            return;

        _ = Interlocked.Increment(ref _failures);
        throw failure;
    }

    /// <inheritdoc />
    public void OnMetaPublishing()
    {
    }

    /// <inheritdoc />
    public void OnSnapshotPublishing()
    {
    }

    /// <summary>Makes every following frame write fail with an <see cref="IOException" />.</summary>
    internal void Arm() => Arm(new IOException("Injected persistent frame write failure."));

    /// <summary>Makes every following frame write fail with <paramref name="failure" />.</summary>
    /// <param name="failure">The failure thrown by every armed frame write.</param>
    internal void Arm(Exception failure) => Volatile.Write(ref _failure, failure);

    /// <summary>Makes every following flush, which comes after the frame bytes were written, fail with an <see cref="IOException" />.</summary>
    internal void ArmFlush() => Volatile.Write(ref _flushFailure, new IOException("Injected persistent flush failure."));

    /// <summary>Lets the following frame writes and flushes succeed again.</summary>
    internal void Disarm()
    {
        Volatile.Write(ref _failure, null);
        Volatile.Write(ref _flushFailure, null);
    }
}
