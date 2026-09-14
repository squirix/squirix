using System;
using System.Buffers;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging.Abstractions;
using Squirix.Server.Attributes;
using Squirix.Server.Errors;
using Squirix.Server.Storage;
using Squirix.Server.Storage.Journaling;
using Squirix.Server.TestKit;
using Squirix.Server.UnitTests.Support;
using Squirix.Server.Utils;
using Xunit;

namespace Squirix.Server.UnitTests.Persistence.Journaling;

/// <summary>
/// Direct coverage for the journal segment writer drain gates: abandoned appends fail without
/// writing, capacity failures fault acks, and a full batch flushes before restaging.
/// </summary>
[Immutable]
public sealed class JournalEventLoopSegmentWriterTests : IsolatedStorageTestBase
{
    /// <summary>An abandoned append fails idempotently without releasing resources twice.</summary>
    [Fact]
    public void AbandonedAppendFailsWithoutWriting()
    {
        using var setup = CreateSetup();
        var buffer = ArrayPool<byte>.Shared.Rent(64);
        var ack = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var item = JournalWorkItem.Append(buffer, 64, ack);
        setup.Registry.Track(item, buffer, 64, ack);
        _ = Interlocked.Increment(ref setup.Counter.Value);

        var reason = new InvalidOperationException("pipeline failed");
        _ = setup.Registry.FailAll(reason, NullLogger.Instance, setup.Counter);

        Assert.False(setup.Writer.ProcessJournalWorkItem(item));
        Assert.Same(reason, ack.Task.Exception?.InnerException);
        Assert.Equal(0, setup.Counter.Value);
        Assert.True(setup.Writer.TryAcceptAppendIntoBatch(item, out var deferred));
        Assert.False(deferred);
        Assert.Equal(1, setup.Registry.ReturnQuarantinedBuffers());
        Assert.Equal(0, setup.Registry.ReturnQuarantinedBuffers());
    }

    /// <summary>An abandoned durable append fails idempotently without releasing resources twice.</summary>
    [Fact]
    public void AbandonedDurableAppendFails()
    {
        using var setup = CreateSetup();
        var buffer = ArrayPool<byte>.Shared.Rent(64);
        var ack = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var item = JournalWorkItem.AppendWithDurability(ack, buffer, 64);
        setup.Registry.Track(item, buffer, 64, ack);
        _ = Interlocked.Increment(ref setup.Counter.Value);

        var reason = new InvalidOperationException("pipeline failed");
        _ = setup.Registry.FailAll(reason, NullLogger.Instance, setup.Counter);

        Assert.False(setup.Writer.ProcessJournalWorkItem(item));
        Assert.Same(reason, ack.Task.Exception?.InnerException);
        Assert.Equal(0, setup.Counter.Value);
        Assert.Equal(1, setup.Registry.ReturnQuarantinedBuffers());
        Assert.Equal(0, setup.Registry.ReturnQuarantinedBuffers());
    }

    /// <summary>A capacity failure on the append path faults the ack and releases the slot.</summary>
    [Fact]
    public void AppendCapacityFailureFaultsAck()
    {
        using var setup = CreateSetup(1024L * 1024L, 1024, 1);
        var buffer = ArrayPool<byte>.Shared.Rent(64);
        var ack = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var item = JournalWorkItem.Append(buffer, 64, ack);
        setup.Registry.Track(item, buffer, 64, ack);
        _ = Interlocked.Increment(ref setup.Counter.Value);

        Assert.False(setup.Writer.ProcessJournalWorkItem(item));
        _ = Assert.IsType<JournalCapacityExceededException>(ack.Task.Exception?.InnerException);
        Assert.Equal(0, setup.Counter.Value);
    }

    /// <summary>A capacity failure on the durable append path faults the ack and releases the slot.</summary>
    [Fact]
    public void DurableAppendCapacityFailureFaultsAck()
    {
        using var setup = CreateSetup(1024L * 1024L, 1024, 1);
        var buffer = ArrayPool<byte>.Shared.Rent(64);
        var ack = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var item = JournalWorkItem.AppendWithDurability(ack, buffer, 64);
        setup.Registry.Track(item, buffer, 64, ack);
        _ = Interlocked.Increment(ref setup.Counter.Value);

        Assert.False(setup.Writer.ProcessJournalWorkItem(item));
        _ = Assert.IsType<JournalCapacityExceededException>(ack.Task.Exception?.InnerException);
        Assert.Equal(0, setup.Counter.Value);
    }

    /// <summary>A full batch flushes and restages the overflow append instead of dropping it.</summary>
    [Fact]
    public void FullBatchFlushesAndRestages()
    {
        using var setup = CreateSetup(batchCapacityBytes: 100);
        var firstBuffer = ArrayPool<byte>.Shared.Rent(64);
        var firstAck = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var first = JournalWorkItem.Append(firstBuffer, 64, firstAck);
        setup.Registry.Track(first, firstBuffer, 64, firstAck);
        _ = Interlocked.Increment(ref setup.Counter.Value);
        Assert.True(setup.Writer.TryAcceptAppendIntoBatch(first, out var firstDeferred));
        Assert.False(firstDeferred);

        var secondBuffer = ArrayPool<byte>.Shared.Rent(64);
        var secondAck = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var second = JournalWorkItem.Append(secondBuffer, 64, secondAck);
        setup.Registry.Track(second, secondBuffer, 64, secondAck);
        _ = Interlocked.Increment(ref setup.Counter.Value);

        Assert.True(setup.Writer.TryAcceptAppendIntoBatch(second, out var secondDeferred));
        Assert.False(secondDeferred);
        Assert.True(firstAck.Task.IsCompletedSuccessfully);
        Assert.Equal(1, setup.Counter.Value);
        Assert.Same(second, Assert.Single(setup.Batch.PendingAppends));

        Assert.True(setup.Registry.Untrack(second, out var staged));
        Assert.NotNull(staged);
        _ = Interlocked.Decrement(ref setup.Counter.Value);
        ArrayPool<byte>.Shared.ReturnCleared(staged.FrameBytes);
        Assert.Equal(0, setup.Counter.Value);
    }

    /// <summary>A capacity failure on the batch path faults the ack and releases the slot.</summary>
    [Fact]
    public void BatchAppendCapacityFailureFaultsAck()
    {
        using var setup = CreateSetup(1024L * 1024L, 1024, 1);
        var buffer = ArrayPool<byte>.Shared.Rent(64);
        var ack = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var item = JournalWorkItem.Append(buffer, 64, ack);
        setup.Registry.Track(item, buffer, 64, ack);
        _ = Interlocked.Increment(ref setup.Counter.Value);

        Assert.True(setup.Writer.TryAcceptAppendIntoBatch(item, out var deferred));
        Assert.False(deferred);
        _ = Assert.IsType<JournalCapacityExceededException>(ack.Task.Exception?.InnerException);
        Assert.Equal(0, setup.Counter.Value);
    }

    /// <summary>A drain landing after staging but before the flush drops the batch instead of writing it.</summary>
    [Fact]
    public void StagedBatchDropsAfterDrain()
    {
        using var setup = CreateSetup();
        var buffer = ArrayPool<byte>.Shared.Rent(64);
        var ack = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var item = JournalWorkItem.Append(buffer, 64, ack);
        setup.Registry.Track(item, buffer, 64, ack);
        _ = Interlocked.Increment(ref setup.Counter.Value);
        Assert.True(setup.Writer.TryAcceptAppendIntoBatch(item, out var deferred));
        Assert.False(deferred);

        var reason = new InvalidOperationException("pipeline failed");
        _ = setup.Registry.FailAll(reason, NullLogger.Instance, setup.Counter);

        setup.Writer.FlushWriteBatch();

        Assert.Same(reason, ack.Task.Exception?.InnerException);
        Assert.Equal(0, setup.Counter.Value);
        Assert.Empty(setup.Batch.PendingAppends);
        Assert.Equal(1, setup.Registry.ReturnQuarantinedBuffers());
        Assert.Equal(0, setup.Registry.ReturnQuarantinedBuffers());
    }

    /// <summary>A segment open failure faults the ack, releases the slot, and propagates.</summary>
    [Fact]
    public void SegmentOpenFailureFaultsAck()
    {
        var options = new PersistenceOptions { DataDir = Path.Join(Dir, "missing") };
        var registry = new PendingAppendRegistry();
        var counter = new MutableInt32();
        var host = new FakeEventLoopHost(registry, counter);
        using var segmentWriter = JournalSegmentWriterFactory.Create(options.JournalPlatformBackend);
        using var state = new FakeEventLoopState(host, options, new JournalWriteBatchBuffer(), 0, segmentWriter);
        var writer = new JournalEventLoopSegmentWriter(state, new FakeEventLoopRollState());
        var buffer = ArrayPool<byte>.Shared.Rent(64);
        var ack = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var item = JournalWorkItem.Append(buffer, 64, ack);
        registry.Track(item, buffer, 64, ack);
        _ = Interlocked.Increment(ref counter.Value);

        var thrown = NodeExceptionAssert.For<DirectoryNotFoundException>().Throws(
            (Writer: writer, Item: item),
            static s => s.Writer.ProcessJournalWorkItem(s.Item));

        Assert.Same(thrown, ack.Task.Exception?.InnerException);
        Assert.Equal(0, counter.Value);
    }

    /// <summary>A segment open failure on the durable path faults the ack, releases the slot, and propagates.</summary>
    [Fact]
    public void DurableSegmentOpenFailureFaultsAck()
    {
        var options = new PersistenceOptions { DataDir = Path.Join(Dir, "missing") };
        var registry = new PendingAppendRegistry();
        var counter = new MutableInt32();
        var host = new FakeEventLoopHost(registry, counter);
        using var segmentWriter = JournalSegmentWriterFactory.Create(options.JournalPlatformBackend);
        using var state = new FakeEventLoopState(host, options, new JournalWriteBatchBuffer(), 0, segmentWriter);
        var writer = new JournalEventLoopSegmentWriter(state, new FakeEventLoopRollState());
        var buffer = ArrayPool<byte>.Shared.Rent(64);
        var ack = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var item = JournalWorkItem.AppendWithDurability(ack, buffer, 64);
        registry.Track(item, buffer, 64, ack);
        _ = Interlocked.Increment(ref counter.Value);

        var thrown = NodeExceptionAssert.For<DirectoryNotFoundException>().Throws(
            (Writer: writer, Item: item),
            static s => s.Writer.ProcessJournalWorkItem(s.Item));

        Assert.Same(thrown, ack.Task.Exception?.InnerException);
        Assert.Equal(0, counter.Value);
    }

    private static WriterSetup CreateSetup(long totalBytes = 0, int batchCapacityBytes = 1024, int maxTotalBytesMb = 0)
    {
        var options = new PersistenceOptions { JournalMaxTotalBytesMb = maxTotalBytesMb };
        var registry = new PendingAppendRegistry();
        var counter = new MutableInt32();
        var host = new FakeEventLoopHost(registry, counter);
        var batch = new JournalWriteBatchBuffer(batchCapacityBytes);
        var state = new FakeEventLoopState(host, options, batch, totalBytes);
        var roll = new FakeEventLoopRollState("seg-0001");
        return new WriterSetup(new JournalEventLoopSegmentWriter(state, roll), registry, counter, batch, state);
    }

    private sealed class WriterSetup : IDisposable
    {
        internal WriterSetup(JournalEventLoopSegmentWriter writer, PendingAppendRegistry registry, MutableInt32 counter, JournalWriteBatchBuffer batch, FakeEventLoopState state)
        {
            Writer = writer;
            Registry = registry;
            Counter = counter;
            Batch = batch;
            State = state;
        }

        internal JournalEventLoopSegmentWriter Writer { get; }

        internal PendingAppendRegistry Registry { get; }

        internal MutableInt32 Counter { get; }

        internal JournalWriteBatchBuffer Batch { get; }

        private FakeEventLoopState State { get; }

        /// <summary>Releases the fake segment writer.</summary>
        public void Dispose() => State.Dispose();
    }

    private sealed class FakeEventLoopHost : IJournalEventLoopHost
    {
        private readonly MutableInt32 _counter;
        private readonly PendingAppendRegistry _pendingAppends;

        internal FakeEventLoopHost(PendingAppendRegistry pendingAppends, MutableInt32 counter)
        {
            _pendingAppends = pendingAppends;
            _counter = counter;
        }

        PendingAppendRegistry IJournalEventLoopHost.PendingAppends => _pendingAppends;

        void IJournalEventLoopHost.CompleteDurabilityCheckpoint(JournalWorkItem item) => _ = item.Ack?.TrySetResult();

        void IJournalEventLoopHost.DecrementQueuedAppends() => _ = Interlocked.Decrement(ref _counter.Value);

        void IJournalEventLoopHost.FailPipeline(Exception reason)
        {
        }

        void IJournalEventLoopHost.PublishRoll(int targetSegmentIndex)
        {
        }

        void IJournalEventLoopHost.SetNextSequence(ulong value)
        {
        }

        void IJournalEventLoopHost.ThrowIfJournalThreadFailed()
        {
        }
    }

    private sealed class FakeEventLoopState : IJournalEventLoopState, IDisposable
    {
        private readonly IJournalSegmentWriter _segmentWriter;
        private readonly JournalWriteBatchBuffer _writeBatch;
        private readonly FakeEventLoopHost _host;
        private readonly PersistenceOptions _options;
        private readonly JournalSegmentPolicy _policy;
        private long _totalBytes;
        private long _activeBytes;

        internal FakeEventLoopState(FakeEventLoopHost host, PersistenceOptions options, JournalWriteBatchBuffer batch, long totalBytes, IJournalSegmentWriter? segmentWriter = null)
        {
            _host = host;
            _options = options;
            _policy = new JournalSegmentPolicy(options);
            _writeBatch = batch;
            _totalBytes = totalBytes;
            _segmentWriter = segmentWriter ?? new FakeSegmentWriter();
        }

        long IJournalEventLoopState.ActiveSegmentWrittenBytes => _activeBytes;

        JournalDurabilityGroupCommit? IJournalEventLoopState.GroupCommit => null;

        IJournalEventLoopHost IJournalEventLoopState.Host => _host;

        long IJournalEventLoopState.JournalTotalBytes => _totalBytes;

        PersistenceOptions IJournalEventLoopState.Options => _options;

        JournalSegmentPolicy IJournalEventLoopState.Policy => _policy;

        IJournalSegmentWriter IJournalEventLoopState.SegmentWriter => _segmentWriter;

        JournalWriteBatchBuffer IJournalEventLoopState.WriteBatch => _writeBatch;

        void IJournalEventLoopState.AddJournalTotalBytes(long delta) => _totalBytes += delta;

        void IJournalEventLoopState.FsyncOnJournalThread()
        {
        }

        void IJournalEventLoopState.SetActiveSegmentWrittenBytes(long value) => _activeBytes = value;

        void IJournalEventLoopState.SetDirty(bool value)
        {
        }

        void IJournalEventLoopState.SetJournalTotalBytes(long value) => _totalBytes = value;

        /// <summary>Releases the fake segment writer.</summary>
        public void Dispose() => _segmentWriter.Dispose();
    }

    private sealed class FakeEventLoopRollState : IJournalEventLoopRollState
    {
        private string? _activeSegmentPath;

        internal FakeEventLoopRollState(string? activeSegmentPath = null)
        {
            _activeSegmentPath = activeSegmentPath;
        }

        string? IJournalEventLoopRollState.ActiveSegmentPath => _activeSegmentPath;

        int IJournalEventLoopRollState.CurrentSegmentIndex => 0;

        int IJournalEventLoopRollState.JournalSegmentCount => 0;

        int IJournalEventLoopRollState.PendingRollTargetSegmentIndex => 0;

        bool IJournalEventLoopRollState.SegmentRollInFlight => false;

        bool IJournalEventLoopRollState.TryConsumeSegmentRollCompletion() => false;

        void IJournalEventLoopRollState.MarkSegmentRollCompletionPending()
        {
        }

        void IJournalEventLoopRollState.IncrementJournalSegmentCount()
        {
        }

        void IJournalEventLoopRollState.SetActiveSegmentPath(string? value) => _activeSegmentPath = value;

        void IJournalEventLoopRollState.SetCurrentSegmentIndex(int value)
        {
        }

        void IJournalEventLoopRollState.SetJournalSegmentCount(int value)
        {
        }

        void IJournalEventLoopRollState.SetPendingRollTargetSegmentIndex(int value)
        {
        }

        void IJournalEventLoopRollState.SetSegmentRollInFlight(bool value)
        {
        }
    }

    private sealed class FakeSegmentWriter : IJournalSegmentWriter
    {
        long IJournalSegmentWriter.Length => 0;

        void IJournalSegmentWriter.Fsync()
        {
        }

        void IJournalSegmentWriter.OpenSegment(string path, bool append)
        {
        }

        void IJournalSegmentWriter.Truncate(long length)
        {
        }

        void IJournalSegmentWriter.Write(ReadOnlySpan<byte> buffer, long fileOffset)
        {
        }

        /// <summary>Releases test resources.</summary>
        public void Dispose()
        {
        }
    }
}
