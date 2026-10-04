using System;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.Storage.Journaling;

namespace Squirix.Server.Benchmarks;

/// <summary>
/// Segment writer that forwards to a real writer, records how long every <see cref="IJournalSegmentWriter.Write" /> and
/// <see cref="IJournalSegmentWriter.FlushToDisk" /> call took, and can block one of them on the journal thread until released, simulating a stalled disk.
/// </summary>
[ThreadSafe]
internal sealed class JournalMeasuringSegmentWriter : IJournalSegmentWriter
{
    private readonly ManualResetEventSlim _released = new(true);
    private readonly IJournalSegmentWriter _inner;
    private TaskCompletionSource _entered = new(TaskCreationOptions.RunContinuationsAsynchronously);
    private int _recording;
    private volatile JournalStallKind _stallKind = JournalStallKind.None;

    /// <summary>Initializes a new instance of the <see cref="JournalMeasuringSegmentWriter" /> class over the default file writer.</summary>
    internal JournalMeasuringSegmentWriter()
        : this(JournalSegmentWriterFactory.Create())
    {
    }

    /// <summary>Initializes a new instance of the <see cref="JournalMeasuringSegmentWriter" /> class.</summary>
    /// <param name="inner">The writer that performs the real I/O.</param>
    internal JournalMeasuringSegmentWriter(IJournalSegmentWriter inner)
    {
        ArgumentNullException.ThrowIfNull(inner);
        _inner = inner;
    }

    long IJournalSegmentWriter.Length => _inner.Length;

    /// <summary>Gets a task that completes once a call has blocked on the armed stall.</summary>
    internal Task Entered => Volatile.Read(ref _entered).Task;

    /// <summary>Gets the recorded durations of <see cref="IJournalSegmentWriter.FlushToDisk" /> calls.</summary>
    internal LatencySamples Flushes { get; } = new();

    /// <summary>Gets the recorded durations of <see cref="IJournalSegmentWriter.Write" /> calls.</summary>
    internal LatencySamples Writes { get; } = new();

    /// <summary>Releases every stall, then disposes the inner writer.</summary>
    public void Dispose()
    {
        Release();
        _inner.Dispose();
        _released.Dispose();
    }

    void IJournalSegmentWriter.FlushToDisk()
    {
        var start = Stopwatch.GetTimestamp();
        BlockIfArmed(JournalStallKind.Flush);
        _inner.FlushToDisk();
        if (Volatile.Read(ref _recording) != 0)
            Flushes.Add(Stopwatch.GetElapsedTime(start));
    }

    void IJournalSegmentWriter.OpenSegment(string path, bool append) => _inner.OpenSegment(path, append);

    void IJournalSegmentWriter.Truncate(long length) => _inner.Truncate(length);

    void IJournalSegmentWriter.Write(ReadOnlySpan<byte> buffer, long fileOffset)
    {
        var start = Stopwatch.GetTimestamp();
        BlockIfArmed(JournalStallKind.Write);
        _inner.Write(buffer, fileOffset);
        if (Volatile.Read(ref _recording) != 0)
            Writes.Add(Stopwatch.GetElapsedTime(start));
    }

    /// <summary>Makes subsequent calls of <paramref name="kind" /> block until <see cref="Release" />.</summary>
    /// <param name="kind">The call to block.</param>
    internal void Arm(JournalStallKind kind)
    {
        Volatile.Write(ref _entered, new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously));
        _released.Reset();
        _stallKind = kind;
    }

    /// <summary>Unblocks the stalled call and disarms the stall; later calls pass through.</summary>
    internal void Release()
    {
        _stallKind = JournalStallKind.None;
        _released.Set();
    }

    /// <summary>Starts or stops recording call durations.</summary>
    /// <param name="recording">Whether durations are recorded.</param>
    internal void SetRecording(bool recording) => Volatile.Write(ref _recording, recording ? 1 : 0);

    private void BlockIfArmed(JournalStallKind kind)
    {
        if (_stallKind != kind)
            return;

        _ = Volatile.Read(ref _entered).TrySetResult();
        _released.Wait(CancellationToken.None);
    }
}
