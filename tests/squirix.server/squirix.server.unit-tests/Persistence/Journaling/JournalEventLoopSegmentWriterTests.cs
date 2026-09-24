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
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Persistence.Journaling;

/// <summary>
/// Direct coverage for the journal segment writer drain gates: abandoned appends fail without
/// writing, capacity failures fault acks, and a full batch flushes before restaging.
/// </summary>
[Immutable]
public sealed class JournalEventLoopSegmentWriterTests : IsolatedStorageTestBase
{
    /// <summary>An abandoned append fails idempotently without releasing resources twice.</summary>
    [Test]
    public async Task AbandonedAppendFailsWithoutWriting()
    {
        using var setup = CreateSetup();
        var buffer = ArrayPool<byte>.Shared.Rent(64);
        var ack = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var item = JournalWorkItem.Append(buffer, 64, ack);
        setup.Registry.Track(item, buffer, 64, ack);
        _ = Interlocked.Increment(ref setup.Counter.Value);

        var reason = new InvalidOperationException("pipeline failed");
        _ = setup.Registry.FailAll(reason, NullLogger.Instance, setup.Counter);

        _ = await Assert.That(setup.Writer.ProcessJournalWorkItem(item)).IsFalse();
        _ = await Assert.That(ack.Task.Exception?.InnerException).IsSameReferenceAs(reason);
        _ = await Assert.That(setup.Counter.Value).IsEqualTo(0);
        _ = await Assert.That(setup.Writer.TryAcceptAppendIntoBatch(item, out var deferred)).IsTrue();
        _ = await Assert.That(deferred).IsFalse();
        _ = await Assert.That(setup.Registry.ReturnQuarantinedBuffers()).IsEqualTo(1);
        _ = await Assert.That(setup.Registry.ReturnQuarantinedBuffers()).IsEqualTo(0);
    }

    /// <summary>An abandoned durable append fails idempotently without releasing resources twice.</summary>
    [Test]
    public async Task AbandonedDurableAppendFails()
    {
        using var setup = CreateSetup();
        var buffer = ArrayPool<byte>.Shared.Rent(64);
        var ack = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var item = JournalWorkItem.AppendWithDurability(ack, buffer, 64);
        setup.Registry.Track(item, buffer, 64, ack);
        _ = Interlocked.Increment(ref setup.Counter.Value);

        var reason = new InvalidOperationException("pipeline failed");
        _ = setup.Registry.FailAll(reason, NullLogger.Instance, setup.Counter);

        _ = await Assert.That(setup.Writer.ProcessJournalWorkItem(item)).IsFalse();
        _ = await Assert.That(ack.Task.Exception?.InnerException).IsSameReferenceAs(reason);
        _ = await Assert.That(setup.Counter.Value).IsEqualTo(0);
        _ = await Assert.That(setup.Registry.ReturnQuarantinedBuffers()).IsEqualTo(1);
        _ = await Assert.That(setup.Registry.ReturnQuarantinedBuffers()).IsEqualTo(0);
    }

    /// <summary>A capacity failure on the append path faults the ack and releases the slot.</summary>
    [Test]
    public async Task AppendCapacityFailureFaultsAck()
    {
        using var setup = CreateSetup(1024L * 1024L, 1024, 1);
        var buffer = ArrayPool<byte>.Shared.Rent(64);
        var ack = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var item = JournalWorkItem.Append(buffer, 64, ack);
        setup.Registry.Track(item, buffer, 64, ack);
        _ = Interlocked.Increment(ref setup.Counter.Value);

        _ = await Assert.That(setup.Writer.ProcessJournalWorkItem(item)).IsFalse();
        _ = await Assert.That(ack.Task.Exception?.InnerException).IsTypeOf<JournalCapacityExceededException>();
        _ = await Assert.That(setup.Counter.Value).IsEqualTo(0);
    }

    /// <summary>A capacity failure on the batch path faults the ack and releases the slot.</summary>
    [Test]
    public async Task BatchAppendCapacityFailureFaultsAck()
    {
        using var setup = CreateSetup(1024L * 1024L, 1024, 1);
        var buffer = ArrayPool<byte>.Shared.Rent(64);
        var ack = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var item = JournalWorkItem.Append(buffer, 64, ack);
        setup.Registry.Track(item, buffer, 64, ack);
        _ = Interlocked.Increment(ref setup.Counter.Value);

        _ = await Assert.That(setup.Writer.TryAcceptAppendIntoBatch(item, out var deferred)).IsTrue();
        _ = await Assert.That(deferred).IsFalse();
        _ = await Assert.That(ack.Task.Exception?.InnerException).IsTypeOf<JournalCapacityExceededException>();
        _ = await Assert.That(setup.Counter.Value).IsEqualTo(0);
    }

    /// <summary>A capacity failure on the durable append path faults the ack and releases the slot.</summary>
    [Test]
    public async Task DurableAppendCapacityFailureFaultsAck()
    {
        using var setup = CreateSetup(1024L * 1024L, 1024, 1);
        var buffer = ArrayPool<byte>.Shared.Rent(64);
        var ack = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var item = JournalWorkItem.AppendWithDurability(ack, buffer, 64);
        setup.Registry.Track(item, buffer, 64, ack);
        _ = Interlocked.Increment(ref setup.Counter.Value);

        _ = await Assert.That(setup.Writer.ProcessJournalWorkItem(item)).IsFalse();
        _ = await Assert.That(ack.Task.Exception?.InnerException).IsTypeOf<JournalCapacityExceededException>();
        _ = await Assert.That(setup.Counter.Value).IsEqualTo(0);
    }

    /// <summary>A segment open failure on the durable path faults the ack, releases the slot, and propagates.</summary>
    [Test]
    public async Task DurableSegmentOpenFailureFaultsAck()
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

        var thrown = NodeExceptionAssert.For<DirectoryNotFoundException>().Throws((Writer: writer, Item: item), static s => s.Writer.ProcessJournalWorkItem(s.Item));

        _ = await Assert.That(ack.Task.Exception?.InnerException).IsSameReferenceAs(thrown);
        _ = await Assert.That(counter.Value).IsEqualTo(0);
    }

    /// <summary>A full batch flushes and restages the overflow append instead of dropping it.</summary>
    [Test]
    public async Task FullBatchFlushesAndRestages()
    {
        using var setup = CreateSetup(batchCapacityBytes: 100);
        var firstBuffer = ArrayPool<byte>.Shared.Rent(64);
        var firstAck = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var first = JournalWorkItem.Append(firstBuffer, 64, firstAck);
        setup.Registry.Track(first, firstBuffer, 64, firstAck);
        _ = Interlocked.Increment(ref setup.Counter.Value);
        _ = await Assert.That(setup.Writer.TryAcceptAppendIntoBatch(first, out var firstDeferred)).IsTrue();
        _ = await Assert.That(firstDeferred).IsFalse();

        var secondBuffer = ArrayPool<byte>.Shared.Rent(64);
        var secondAck = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var second = JournalWorkItem.Append(secondBuffer, 64, secondAck);
        setup.Registry.Track(second, secondBuffer, 64, secondAck);
        _ = Interlocked.Increment(ref setup.Counter.Value);

        _ = await Assert.That(setup.Writer.TryAcceptAppendIntoBatch(second, out var secondDeferred)).IsTrue();
        _ = await Assert.That(secondDeferred).IsFalse();
        _ = await Assert.That(firstAck.Task.IsCompletedSuccessfully).IsTrue();
        _ = await Assert.That(setup.Counter.Value).IsEqualTo(1);
        var singleAppend = await Assert.That(setup.Batch.PendingAppends).HasSingleItem();
        _ = await Assert.That(singleAppend).IsSameReferenceAs(second);

        _ = await Assert.That(setup.Registry.Untrack(second, out var staged)).IsTrue();
        _ = await Assert.That(staged).IsNotNull();
        _ = Interlocked.Decrement(ref setup.Counter.Value);
        ArrayPool<byte>.Shared.ReturnCleared(staged.FrameBytes);
        _ = await Assert.That(setup.Counter.Value).IsEqualTo(0);
    }

    /// <summary>A segment open failure faults the ack, releases the slot, and propagates.</summary>
    [Test]
    public async Task SegmentOpenFailureFaultsAck()
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

        var thrown = NodeExceptionAssert.For<DirectoryNotFoundException>().Throws((Writer: writer, Item: item), static s => s.Writer.ProcessJournalWorkItem(s.Item));

        _ = await Assert.That(ack.Task.Exception?.InnerException).IsSameReferenceAs(thrown);
        _ = await Assert.That(counter.Value).IsEqualTo(0);
    }

    /// <summary>A drain landing after staging but before the flush drops the batch instead of writing it.</summary>
    [Test]
    public async Task StagedBatchDropsAfterDrain()
    {
        using var setup = CreateSetup();
        var buffer = ArrayPool<byte>.Shared.Rent(64);
        var ack = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var item = JournalWorkItem.Append(buffer, 64, ack);
        setup.Registry.Track(item, buffer, 64, ack);
        _ = Interlocked.Increment(ref setup.Counter.Value);
        _ = await Assert.That(setup.Writer.TryAcceptAppendIntoBatch(item, out var deferred)).IsTrue();
        _ = await Assert.That(deferred).IsFalse();

        var reason = new InvalidOperationException("pipeline failed");
        _ = setup.Registry.FailAll(reason, NullLogger.Instance, setup.Counter);

        setup.Writer.FlushWriteBatch();

        _ = await Assert.That(ack.Task.Exception?.InnerException).IsSameReferenceAs(reason);
        _ = await Assert.That(setup.Counter.Value).IsEqualTo(0);
        _ = await Assert.That(setup.Batch.PendingAppends).IsEmpty();
        _ = await Assert.That(setup.Registry.ReturnQuarantinedBuffers()).IsEqualTo(1);
        _ = await Assert.That(setup.Registry.ReturnQuarantinedBuffers()).IsEqualTo(0);
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

        void IJournalEventLoopRollState.IncrementJournalSegmentCount()
        {
        }

        void IJournalEventLoopRollState.MarkSegmentRollCompletionPending()
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

        bool IJournalEventLoopRollState.TryConsumeSegmentRollCompletion() => false;
    }

    private sealed class FakeEventLoopState : IJournalEventLoopState, IDisposable
    {
        private readonly FakeEventLoopHost _host;
        private readonly PersistenceOptions _options;
        private readonly JournalSegmentPolicy _policy;
        private readonly IJournalSegmentWriter _segmentWriter;
        private readonly JournalWriteBatchBuffer _writeBatch;
        private long _activeBytes;
        private long _totalBytes;

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

        /// <summary>Releases the fake segment writer.</summary>
        public void Dispose() => _segmentWriter.Dispose();

        void IJournalEventLoopState.FlushToDisk()
        {
        }

        void IJournalEventLoopState.SetActiveSegmentWrittenBytes(long value) => _activeBytes = value;

        void IJournalEventLoopState.SetDirty(bool value)
        {
        }

        void IJournalEventLoopState.SetJournalTotalBytes(long value) => _totalBytes = value;
    }

    private sealed class FakeSegmentWriter : IJournalSegmentWriter
    {
        long IJournalSegmentWriter.Length => 0;

        /// <summary>Releases test resources.</summary>
        public void Dispose()
        {
        }

        void IJournalSegmentWriter.FlushToDisk()
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

        internal JournalWriteBatchBuffer Batch { get; }

        internal MutableInt32 Counter { get; }

        internal PendingAppendRegistry Registry { get; }

        internal JournalEventLoopSegmentWriter Writer { get; }

        private FakeEventLoopState State { get; }

        /// <summary>Releases the fake segment writer.</summary>
        public void Dispose() => State.Dispose();
    }
}
