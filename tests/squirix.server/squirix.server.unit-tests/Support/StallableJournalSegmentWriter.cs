using System;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.Storage.Journaling;

namespace Squirix.Server.UnitTests.Support;

/// <summary>
/// Segment writer that forwards to a real writer but can block <see cref="IJournalSegmentWriter.Write" /> (before or after it reached the
/// file) or <see cref="IJournalSegmentWriter.FlushToDisk" /> on the journal thread until a test releases it, simulating a stalled disk.
/// </summary>
[ThreadSafe]
internal sealed class StallableJournalSegmentWriter : IJournalSegmentWriter
{
    private readonly IJournalSegmentWriter _inner;
    private int _disposeCount;

    /// <summary>Initializes a new instance of the <see cref="StallableJournalSegmentWriter" /> class over the default file writer.</summary>
    internal StallableJournalSegmentWriter()
        : this(JournalSegmentWriterFactory.Create())
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

    /// <summary>Gets the stall switch applied after <see cref="IJournalSegmentWriter.Write" /> reached the file, as a write that lands and then hangs.</summary>
    internal Stall AfterWrite { get; } = new();

    /// <summary>Gets the stall switch applied to <see cref="IJournalSegmentWriter.FlushToDisk" />.</summary>
    internal Stall Flush { get; } = new();

    /// <summary>Gets how many times this writer was disposed.</summary>
    internal int DisposeCount => Volatile.Read(ref _disposeCount);

    /// <summary>Gets the stall switch applied to <see cref="IJournalSegmentWriter.Write" />.</summary>
    internal Stall Write { get; } = new();

    /// <summary>Releases every stall, then disposes the inner writer.</summary>
    public void Dispose()
    {
        _ = Interlocked.Increment(ref _disposeCount);
        ReleaseAll();
        _inner.Dispose();
        AfterWrite.Dispose();
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
        AfterWrite.BlockIfArmed();
    }

    /// <summary>Releases every stall switch.</summary>
    internal void ReleaseAll()
    {
        AfterWrite.Release();
        Flush.Release();
        Write.Release();
    }

    /// <summary>
    /// One armable stall: while armed, every call blocks until <see cref="Release" />. <see cref="Entered" /> completes when the first
    /// call blocks, so tests wait on it instead of sleeping. <see cref="ArmAfter" /> lets a given number of calls through first.
    /// </summary>
    [ThreadSafe]
    internal sealed class Stall : IDisposable
    {
        private readonly TaskCompletionSource _entered = new(TaskCreationOptions.RunContinuationsAsynchronously);
        private readonly ManualResetEventSlim _released = new(true);
        private int _armed;
        private int _disposed;
        private Exception? _failure;
        private int _passes;

        /// <summary>Gets a task that completes once a call has blocked on this armed stall.</summary>
        internal Task Entered => _entered.Task;

        /// <summary>Releases the wait handle.</summary>
        public void Dispose()
        {
            Volatile.Write(ref _disposed, 1);
            _released.Dispose();
        }

        /// <summary>Makes subsequent calls block until <see cref="Release" />.</summary>
        internal void Arm() => ArmAfter(0);

        /// <summary>Lets the next <paramref name="passes" /> calls through, then makes later calls block until <see cref="Release" />.</summary>
        /// <param name="passes">Number of calls that pass before the stall blocks.</param>
        internal void ArmAfter(int passes)
        {
            ArgumentOutOfRangeException.ThrowIfNegative(passes);
            Volatile.Write(ref _passes, passes);
            _released.Reset();
            Volatile.Write(ref _armed, 1);
        }

        /// <summary>Unblocks the stalled call and disarms the stall; later calls pass through.</summary>
        internal void Release()
        {
            Volatile.Write(ref _armed, 0);

            // A stop that tears the writer down disposes the stalls; releasing them again afterwards is a no-op.
            if (Volatile.Read(ref _disposed) == 0)
                _released.Set();
        }

        /// <summary>Unblocks the stalled call with <paramref name="failure" />, as a disk that fails after hanging; later calls pass through.</summary>
        /// <param name="failure">Exception the stalled call throws instead of forwarding to the real writer.</param>
        internal void ReleaseWithFailure(Exception failure)
        {
            ArgumentNullException.ThrowIfNull(failure);
            _ = Interlocked.CompareExchange(ref _failure, failure, null);
            Release();
        }

        internal void BlockIfArmed()
        {
            if (Volatile.Read(ref _armed) == 0)
                return;

            if (Interlocked.Decrement(ref _passes) >= 0)
                return;

            _ = _entered.TrySetResult();
            _released.Wait(CancellationToken.None);
            if (Interlocked.Exchange(ref _failure, null) is { } failure)
                throw failure;
        }
    }
}
