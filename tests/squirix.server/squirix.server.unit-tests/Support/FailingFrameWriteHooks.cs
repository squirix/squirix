using System.IO;
using System.Threading;
using Squirix.Server.Attributes;
using Squirix.Server.Storage.Replication;

namespace Squirix.Server.UnitTests.Support;

/// <summary>Follower-log fault hooks that fail every frame write while armed, as a disk that keeps refusing the log bytes.</summary>
/// <remarks>Unlike <see cref="StallableFollowerLogFaultHooks" />, the fault is not one-shot: it repeats until the test disarms it.</remarks>
[ThreadSafe]
internal sealed class FailingFrameWriteHooks : IFollowerLogFaultHooks
{
    private int _armed;
    private int _failures;

    /// <summary>Gets how many frame writes the hooks failed.</summary>
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
    }

    /// <inheritdoc />
    public void OnFrameWritten()
    {
        if (Volatile.Read(ref _armed) == 0)
            return;

        _ = Interlocked.Increment(ref _failures);
        throw new IOException("Injected persistent frame write failure.");
    }

    /// <inheritdoc />
    public void OnMetaPublishing()
    {
    }

    /// <inheritdoc />
    public void OnSnapshotPublishing()
    {
    }

    /// <summary>Makes every following frame write fail.</summary>
    internal void Arm() => Volatile.Write(ref _armed, 1);

    /// <summary>Lets the following frame writes succeed again.</summary>
    internal void Disarm() => Volatile.Write(ref _armed, 0);
}
