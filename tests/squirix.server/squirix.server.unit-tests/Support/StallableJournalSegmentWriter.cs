using System;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.Storage;
using Squirix.Server.Storage.Journaling;

namespace Squirix.Server.UnitTests.Support;

/// <summary>
/// Segment writer that forwards to a real writer but can block <see cref="IJournalSegmentWriter.Write" /> or
/// <see cref="IJournalSegmentWriter.FlushToDisk" /> on the journal thread until a test releases it, simulating a stalled disk.
/// </summary>
[ThreadSafe]
internal sealed class StallableJournalSegmentWriter : IJournalSegmentWriter
{
    private readonly IJournalSegmentWriter _inner;

    /// <summary>Initializes a new instance of the <see cref="StallableJournalSegmentWriter" /> class over the default file writer.</summary>
    internal StallableJournalSegmentWriter()
        : this(JournalSegmentWriterFactory.Create(JournalPlatformBackend.Auto))
    {
    }

    /// <summary>Initializes a new instance of the <see cref="StallableJournalSegmentWriter" /> class.</summary>
    /// <param name="inner">Writer that performs the real I/O once a call is not (or no longer) stalled.</param>
    internal StallableJournalSegmentWriter(IJournalSegmentWriter inner)
    {
        ArgumentNullException.ThrowIfNull(inner);
        _inner = inner;
    }

    long IJournalSegmentWriter.Length => _inner.Length;

    /// <summary>Gets the stall switch applied to <see cref="IJournalSegmentWriter.FlushToDisk" />.</summary>
    internal Stall Flush { get; } = new();

    /// <summary>Gets the stall switch applied to <see cref="IJournalSegmentWriter.Write" />.</summary>
    internal Stall Write { get; } = new();

    /// <summary>Releases every stall, then disposes the inner writer.</summary>
    public void Dispose()
    {
        ReleaseAll();
        _inner.Dispose();
        Flush.Dispose();
        Write.Dispose();
    }

    void IJournalSegmentWriter.FlushToDisk()
    {
        Flush.BlockIfArmed();
        _inner.FlushToDisk();
    }

    void IJournalSegmentWriter.OpenSegment(string path, bool append) => _inner.OpenSegment(path, append);

    void IJournalSegmentWriter.Truncate(long length) => _inner.Truncate(length);

    void IJournalSegmentWriter.Write(ReadOnlySpan<byte> buffer, long fileOffset)
    {
        Write.BlockIfArmed();
        _inner.Write(buffer, fileOffset);
    }

    /// <summary>Releases both stall switches.</summary>
    internal void ReleaseAll()
    {
        Flush.Release();
        Write.Release();
    }

    /// <summary>
    /// One armable stall: while armed, every call blocks until <see cref="Release" />. <see cref="Entered" /> completes when the first
    /// call blocks, so tests wait on it instead of sleeping.
    /// </summary>
    [ThreadSafe]
    internal sealed class Stall : IDisposable
    {
        private readonly TaskCompletionSource _entered = new(TaskCreationOptions.RunContinuationsAsynchronously);
        private readonly ManualResetEventSlim _released = new(true);
        private int _armed;

        /// <summary>Gets a task that completes once a call has blocked on this armed stall.</summary>
        internal Task Entered => _entered.Task;

        /// <summary>Releases the wait handle.</summary>
        public void Dispose() => _released.Dispose();

        /// <summary>Makes subsequent calls block until <see cref="Release" />.</summary>
        internal void Arm()
        {
            _released.Reset();
            Volatile.Write(ref _armed, 1);
        }

        /// <summary>Unblocks the stalled call and disarms the stall; later calls pass through.</summary>
        internal void Release()
        {
            Volatile.Write(ref _armed, 0);
            _released.Set();
        }

        internal void BlockIfArmed()
        {
            if (Volatile.Read(ref _armed) == 0)
                return;

            _ = _entered.TrySetResult();
            _released.Wait(CancellationToken.None);
        }
    }
}
