using System;
using System.Buffers;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging.Abstractions;
using Squirix.Server.Attributes;
using Squirix.Server.Errors;
using Squirix.Server.Storage;
using Squirix.Server.Storage.Journaling;
using Squirix.Server.Storage.Journaling.Abstractions;
using Squirix.Server.Storage.Journaling.Read;
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
    private const int OversizedFrameLength = (1024 * 1024) - JournalFraming.FileHeaderSize + 1;

    private const string SegmentSearchPattern = $"{FilePrefixes.Journal}*{FileExtensions.Journal}";

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

    /// <summary>A batch abandoned by a drain that lands mid-write is truncated and the truncation is flushed.</summary>
    [Test]
    public async Task AbandonedBatchFlushesTruncatedSegment()
    {
        var segmentWriter = new RecordingSegmentWriter();
        using var setup = CreateSetup(segmentWriter: segmentWriter);
        var buffer = ArrayPool<byte>.Shared.Rent(64);
        var ack = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var item = JournalWorkItem.Append(buffer, 64, ack);
        setup.Registry.Track(item, buffer, 64, ack);
        _ = Interlocked.Increment(ref setup.Counter.Value);
        _ = await Assert.That(setup.Writer.TryAcceptAppendIntoBatch(item, out var deferred)).IsTrue();
        _ = await Assert.That(deferred).IsFalse();
        var reason = new InvalidOperationException("pipeline failed");
        segmentWriter.OnWrite = Bind.To<(PendingAppendRegistry Registry, MutableInt32 Counter, InvalidOperationException Reason), Action>(
            (setup.Registry, setup.Counter, reason),
            static s => () => _ = s.Registry.FailAll(s.Reason, NullLogger.Instance, s.Counter));

        setup.Writer.FlushWriteBatch();

        _ = await Assert.That(ack.Task.Exception?.InnerException).IsSameReferenceAs(reason);
        _ = await Assert.That(segmentWriter.Trace).IsEqualTo("Write,Truncate:0,FlushToDisk");
    }

    /// <summary>
    /// An ack-less append rejected for capacity on the unbatched path releases its slot and buffer, then throws instead of
    /// being dropped silently.
    /// </summary>
    [Test]
    public async Task AckLessAppendCapacityRejectionThrows()
    {
        using var setup = CreateSetup(1024L * 1024L, 1024, 1);
        var buffer = ArrayPool<byte>.Shared.Rent(64);
        var item = JournalWorkItem.Append(buffer, 64);
        setup.Registry.Track(item, buffer, 64, null);
        _ = Interlocked.Increment(ref setup.Counter.Value);

        var thrown = NodeExceptionAssert.For<InvalidOperationException>().Throws((setup.Writer, Item: item), static s => s.Writer.ProcessJournalWorkItem(s.Item));

        _ = await Assert.That(thrown.InnerException).IsTypeOf<JournalCapacityExceededException>();
        _ = await Assert.That(setup.Counter.Value).IsEqualTo(0);
        _ = await Assert.That(setup.Registry.PendingCount).IsEqualTo(0);
        _ = await Assert.That(setup.Registry.ReturnQuarantinedBuffers()).IsEqualTo(0);
    }

    /// <summary>
    /// An ack-less append the journal thread rejects for capacity fails the pipeline through the event loop instead of being
    /// dropped while a later checkpoint reports it durable. Its slot and buffer are released by the journal thread.
    /// </summary>
    [Test]
    public async Task AckLessCapacityRejectionFailsPipeline()
    {
        var options = new PersistenceOptions { DataDir = Dir, JournalMaxTotalBytesMb = 1 };
        var counter = new MutableInt32();
        var host = RocksDoubles.CreateEventLoopHost(counter);
        var registry = host.State.PendingAppends;
        using var ring = new BoundedJournalRing(4);
        using var segmentWriter = new FakeSegmentWriter();
        var eventLoop = new JournalEventLoop(
            host.Host,
            ring,
            segmentWriter,
            options,
            new JournalEventLoopStartup(1, 1024L * 1024L, 1, JournalSegmentProbe.Probe(Dir, 1), new JournalCapacityResyncSignal()),
            NullLogger<JournalEventLoop>.Instance,
            CancellationToken.None);
        var buffer = ArrayPool<byte>.Shared.Rent(64);
        var item = JournalWorkItem.Append(buffer, 64);
        registry.Track(item, buffer, 64, null);
        _ = Interlocked.Increment(ref counter.Value);
        await ring.EnqueueAsync(item, CancellationToken.None);

        // The shutdown marker behind the append ends the loop if the rejection ever stops failing the pipeline.
        await ring.EnqueueAsync(JournalWorkItem.Shutdown(), CancellationToken.None);

        eventLoop.Run();

        var failure = await Assert.That(host.State.PipelineFailure).IsTypeOf<InvalidOperationException>();
        _ = await Assert.That(failure?.InnerException).IsTypeOf<JournalCapacityExceededException>();
        _ = await Assert.That(counter.Value).IsEqualTo(0);
        _ = await Assert.That(registry.PendingCount).IsEqualTo(0);
        _ = await Assert.That(registry.ReturnQuarantinedBuffers()).IsEqualTo(0);
    }

    /// <summary>
    /// An ack-less frame larger than an empty segment, bypassing producer admission, fails the pipeline on the journal thread without a
    /// roll: no new segment and no roll publication.
    /// </summary>
    [Test]
    public async Task AckLessOversizedFrameFailsPipeline()
    {
        var run = await RunOversizedFrameAsync(static buffer => JournalWorkItem.Append(buffer, OversizedFrameLength));

        var failure = await Assert.That(run.PipelineFailure).IsTypeOf<InvalidOperationException>();
        var rejection = await Assert.That(failure?.InnerException).IsTypeOf<JournalCapacityExceededException>();
        _ = await Assert.That(rejection?.Message).Contains("segment size");
        await AssertNoRollAsync(run);
    }

    /// <summary>
    /// An ack-less append deferred for a roll that, once the roll completes, needs another roll the segment count forbids is released and
    /// fails the pipeline through the event loop instead of being dropped.
    /// </summary>
    [Test]
    public async Task AckLessRollRejectionFailsPipeline()
    {
        var options = new PersistenceOptions { DataDir = Dir, JournalMaxSegmentMb = 1, JournalMaxSegmentCount = 2 };
        var counter = new MutableInt32();
        var host = RocksDoubles.CreateEventLoopHost(counter);
        var registry = host.State.PendingAppends;
        using var ring = new BoundedJournalRing(4);

        // Every segment the journal thread opens looks nearly full, so the append rolls, and rolls again once the first roll completes.
        using var writer = new FakeSegmentWriter((1024L * 1024L) - 10L);
        var eventLoop = new JournalEventLoop(
            host.Host,
            ring,
            writer,
            options,
            new JournalEventLoopStartup(1, 0L, 1, JournalSegmentProbe.Probe(Dir, 1), new JournalCapacityResyncSignal()),
            NullLogger<JournalEventLoop>.Instance,
            CancellationToken.None);
        host.State.OnRollPublished = eventLoop.MarkSegmentRollCompletionPending;
        var buffer = ArrayPool<byte>.Shared.Rent(64);
        var item = JournalWorkItem.Append(buffer, 64);
        registry.Track(item, buffer, 64, null);
        _ = Interlocked.Increment(ref counter.Value);
        await ring.EnqueueAsync(item, CancellationToken.None);
        await ring.EnqueueAsync(JournalWorkItem.Shutdown(), CancellationToken.None);

        eventLoop.Run();

        var failure = await Assert.That(host.State.PipelineFailure).IsTypeOf<InvalidOperationException>();
        var rejection = await Assert.That(failure?.InnerException).IsTypeOf<JournalCapacityExceededException>();
        _ = await Assert.That(rejection?.Message).Contains("segment count");
        _ = await Assert.That(eventLoop.CurrentSegmentIndex).IsEqualTo(2);
        _ = await Assert.That(counter.Value).IsEqualTo(0);
        _ = await Assert.That(registry.PendingCount).IsEqualTo(0);
        _ = await Assert.That(registry.ReturnQuarantinedBuffers()).IsEqualTo(0);
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

    /// <summary>A failed unbatched frame write latches the pipeline failure before the append's write ack faults.</summary>
    [Test]
    public async Task AppendWriteFailureLatchesBeforeAckFault()
    {
        var writeFailure = new IOException("write failed");
        var ack = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var ackFaultedAtLatch = false;
        Exception? latched = null;
        using var setup = CreateSetup(
            segmentWriter: new FailingWriteSegmentWriter(writeFailure),
            onPipelineFailed: reason =>
            {
                ackFaultedAtLatch = ack.Task.IsCompleted;
                latched = reason;
            });
        var buffer = ArrayPool<byte>.Shared.Rent(64);
        var item = JournalWorkItem.Append(buffer, 64, ack);
        setup.Registry.Track(item, buffer, 64, ack);
        _ = Interlocked.Increment(ref setup.Counter.Value);

        var thrown = NodeExceptionAssert.For<IOException>().Throws((setup.Writer, Item: item), static s => s.Writer.ProcessJournalWorkItem(s.Item));

        _ = await Assert.That(thrown).IsSameReferenceAs(writeFailure);
        _ = await Assert.That(latched).IsSameReferenceAs(writeFailure);
        _ = await Assert.That(ackFaultedAtLatch).IsFalse();
        _ = await Assert.That(ack.Task.Exception?.InnerException).IsSameReferenceAs(writeFailure);
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

    /// <summary>
    /// A group flush that fails while the unbatched path drains due batches must not leave the append's write ack pending: the append is
    /// already untracked, so no failure drain could fault it and its producer would wait forever.
    /// </summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task GroupFlushFailureCompletesWriteAck(CancellationToken cancellationToken)
    {
        var options = new PersistenceOptions { JournalGroupCommitMaxWait = TimeSpan.FromSeconds(30), JournalGroupCommitMaxBatch = 1 };
        var flushFailure = new InvalidOperationException("flush failed");
        var groupCommit = new JournalDurabilityGroupCommit(new FailingFlush(flushFailure).ThrowAction, static () => { }, static _ => { }, options, TimeProvider.System);
        using var setup = CreateSetup(options: options, groupCommit: groupCommit);
        var waiter = groupCommit.AwaitCommitAsync(cancellationToken).AsTask();
        var buffer = ArrayPool<byte>.Shared.Rent(64);
        var ack = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var item = JournalWorkItem.Append(buffer, 64, ack);
        setup.Registry.Track(item, buffer, 64, ack);
        _ = Interlocked.Increment(ref setup.Counter.Value);

        var thrown = NodeExceptionAssert.For<InvalidOperationException>().Throws((setup.Writer, Item: item), static s => s.Writer.ProcessJournalWorkItem(s.Item));

        _ = await Assert.That(thrown).IsSameReferenceAs(flushFailure);
        _ = await Assert.That(ack.Task.IsCompletedSuccessfully).IsTrue();
        _ = await NodeAsyncAssert.ThrowsAsync<InvalidOperationException>(waiter);
    }

    /// <summary>
    /// Frames staged through a batch buffer that grows several times, with a batch boundary in between, reach the segment file as
    /// the header followed by every frame in order, byte for byte.
    /// </summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task GrownBatchWritesSameBytesToSegment(CancellationToken cancellationToken)
    {
        const int frameLength = 30_000;
        const int frameCount = 25;
        const int batchCapacityBytes = 200_000;
        var options = new PersistenceOptions { DataDir = Dir };
        var counter = new MutableInt32();
        var host = RocksDoubles.CreateEventLoopHost(counter);
        var registry = host.State.PendingAppends;
        var batch = new JournalWriteBatchBuffer(batchCapacityBytes);
        var expected = new byte[JournalFraming.FileHeaderSize + (frameLength * frameCount)];
        JournalFraming.WriteFileHeader(expected.AsSpan(0, JournalFraming.FileHeaderSize));

        using (var segmentWriter = JournalSegmentWriterFactory.Create())
        using (var state = new FakeEventLoopState(host.Host, options, batch, 0, segmentWriter))
        {
            var writer = new JournalEventLoopSegmentWriter(state, new FakeEventLoopRollState());
            for (var i = 0; i < frameCount; i++)
            {
                var frameBuffer = ArrayPool<byte>.Shared.Rent(frameLength);
                for (var j = 0; j < frameLength; j++)
                    frameBuffer[j] = byte.CreateTruncating((i * 7) + (j * 13));

                frameBuffer.AsSpan(0, frameLength).CopyTo(expected.AsSpan(JournalFraming.FileHeaderSize + (i * frameLength)));
                var item = JournalWorkItem.Append(frameBuffer, frameLength);
                registry.Track(item, frameBuffer, frameLength, null);
                _ = Interlocked.Increment(ref counter.Value);

                _ = await Assert.That(writer.TryAcceptAppendIntoBatch(item, out var deferred)).IsTrue();
                _ = await Assert.That(deferred).IsFalse();
            }

            _ = await Assert.That(batch.AllocatedLength).IsGreaterThan(64 * 1024);
            _ = await Assert.That(batch.AllocatedLength).IsLessThanOrEqualTo(batchCapacityBytes);

            writer.FlushWriteBatch();
        }

        var segmentFiles = Directory.GetFiles(Dir, SegmentSearchPattern, SearchOption.TopDirectoryOnly);
        var segmentFile = await Assert.That(segmentFiles).HasSingleItem();
        var actual = await File.ReadAllBytesAsync(segmentFile, cancellationToken);
        _ = await Assert.That(actual.AsSpan().SequenceEqual(expected)).IsTrue();
        _ = await Assert.That(counter.Value).IsEqualTo(0);
    }

    /// <summary>
    /// A frame larger than an empty segment, bypassing producer admission, is rejected through its ack before the journal thread decides
    /// to roll: a roll can never make it fit, so no new segment is created and no roll is published.
    /// </summary>
    [Test]
    public async Task OversizedFrameFaultsAckWithoutRoll()
    {
        var ack = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var run = await RunOversizedFrameAsync(buffer => JournalWorkItem.Append(buffer, OversizedFrameLength, ack));

        var rejection = await Assert.That(ack.Task.Exception?.InnerException).IsTypeOf<JournalCapacityExceededException>();
        _ = await Assert.That(rejection?.Message).Contains("segment size");
        _ = await Assert.That(run.PipelineFailure).IsNull();
        await AssertNoRollAsync(run);
    }

    /// <summary>
    /// A permission failure while preparing the roll target fails the pipeline on the journal thread instead of escaping the loop, and
    /// advertises no roll.
    /// </summary>
    [Test]
    public async Task RollAccessDeniedFailsPipeline()
    {
        var options = new PersistenceOptions { DataDir = Dir, JournalMaxSegmentMb = 1, JournalMaxSegmentCount = 4 };
        var counter = new MutableInt32();
        var host = RocksDoubles.CreateEventLoopHost(counter);
        var registry = host.State.PendingAppends;
        using var ring = new BoundedJournalRing(4);

        // The open segment looks nearly full, so the first append rolls; a directory at the roll temp path denies the header write.
        using var segmentWriter = new FakeSegmentWriter((1024L * 1024L) - 10L);
        var eventLoop = new JournalEventLoop(
            host.Host,
            ring,
            segmentWriter,
            options,
            new JournalEventLoopStartup(1, 0L, 1, JournalSegmentProbe.Probe(Dir, 1), new JournalCapacityResyncSignal()),
            NullLogger<JournalEventLoop>.Instance,
            CancellationToken.None);
        _ = Directory.CreateDirectory(JournalReadPath.BuildRollTempPath(Dir, 2));
        var buffer = ArrayPool<byte>.Shared.Rent(64);
        var item = JournalWorkItem.Append(buffer, 64);
        registry.Track(item, buffer, 64, null);
        _ = Interlocked.Increment(ref counter.Value);
        await ring.EnqueueAsync(item, CancellationToken.None);
        await ring.EnqueueAsync(JournalWorkItem.Shutdown(), CancellationToken.None);

        eventLoop.Run();

        _ = await Assert.That(host.State.PipelineFailure).IsTypeOf<UnauthorizedAccessException>();
        _ = await Assert.That(host.State.RollsPublished).IsEqualTo(0);
        _ = await Assert.That(eventLoop.CurrentSegmentIndex).IsEqualTo(1);
        _ = await Assert.That(File.Exists(JournalReadPath.BuildSegmentPath(Dir, 2))).IsFalse();
        _ = await Assert.That(registry.PendingCount).IsEqualTo(1);
        _ = await Assert.That(counter.Value).IsEqualTo(1);

        _ = registry.FailAll(host.State.PipelineFailure!, NullLogger.Instance, counter);
        _ = await Assert.That(registry.ReturnQuarantinedBuffers()).IsEqualTo(1);
    }

    /// <summary>A segment open failure faults the ack, releases the slot, and propagates.</summary>
    [Test]
    public async Task SegmentOpenFailureFaultsAck()
    {
        var options = new PersistenceOptions { DataDir = Path.Join(Dir, "missing") };
        var counter = new MutableInt32();
        var host = RocksDoubles.CreateEventLoopHost(counter);
        var registry = host.State.PendingAppends;
        using var segmentWriter = JournalSegmentWriterFactory.Create();
        using var state = new FakeEventLoopState(host.Host, options, new JournalWriteBatchBuffer(), 0, segmentWriter);
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

    /// <summary>A failed frame write truncates the segment and then flushes the truncation.</summary>
    [Test]
    public async Task WriteFailureFlushesTruncatedSegment()
    {
        var writeFailure = new IOException("write failed");
        var segmentWriter = new RecordingSegmentWriter { WriteFailure = writeFailure };
        using var setup = CreateSetup(segmentWriter: segmentWriter);
        var buffer = ArrayPool<byte>.Shared.Rent(64);
        var ack = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var item = JournalWorkItem.Append(buffer, 64, ack);
        setup.Registry.Track(item, buffer, 64, ack);
        _ = Interlocked.Increment(ref setup.Counter.Value);

        var thrown = NodeExceptionAssert.For<IOException>().Throws((setup.Writer, Item: item), static s => s.Writer.ProcessJournalWorkItem(s.Item));

        _ = await Assert.That(thrown).IsSameReferenceAs(writeFailure);
        _ = await Assert.That(segmentWriter.Trace).IsEqualTo("Write,Truncate:0,FlushToDisk");
    }

    private static async Task AssertNoRollAsync(OversizedFrameRun run)
    {
        _ = await Assert.That(run.RollsPublished).IsEqualTo(0);
        _ = await Assert.That(run.CurrentSegmentIndex).IsEqualTo(1);
        _ = await Assert.That(run.SegmentFiles).IsEqualTo(1);
        _ = await Assert.That(run.QueuedAppends).IsEqualTo(0);
        _ = await Assert.That(run.PendingCount).IsEqualTo(0);
    }

    private static WriterSetup CreateSetup(
        long totalBytes = 0,
        int batchCapacityBytes = 1024,
        int maxTotalBytesMb = 0,
        PersistenceOptions? options = null,
        IJournalSegmentWriter? segmentWriter = null,
        JournalDurabilityGroupCommit? groupCommit = null,
        Action<Exception>? onPipelineFailed = null)
    {
        options ??= new PersistenceOptions { JournalMaxTotalBytesMb = maxTotalBytesMb };
        var counter = new MutableInt32();
        var host = RocksDoubles.CreateEventLoopHost(counter);
        host.State.OnPipelineFailed = onPipelineFailed;
        var batch = new JournalWriteBatchBuffer(batchCapacityBytes);
        var state = new FakeEventLoopState(host.Host, options, batch, totalBytes, segmentWriter) { GroupCommit = groupCommit };
        var roll = new FakeEventLoopRollState("seg-0001");
        return new WriterSetup(new JournalEventLoopSegmentWriter(state, roll), host.State.PendingAppends, counter, batch, state);
    }

    private static void WriteHeaderOnlySegment(string segmentPath)
    {
        Span<byte> header = stackalloc byte[JournalFraming.FileHeaderSize];
        JournalFraming.WriteFileHeader(header);
        File.WriteAllBytes(segmentPath, header);
    }

    /// <summary>
    /// Runs the event loop over one frame larger than an empty one-megabyte segment, bypassing producer admission. The current segment is
    /// already on disk with only its header, and every published roll completes at once, so a journal thread that kept rolling for the
    /// frame would add one segment and one roll publication per retry up to the segment count limit.
    /// </summary>
    /// <param name="createItem">Creates the work item over the rented frame buffer.</param>
    /// <returns>What the run left behind.</returns>
    private async Task<OversizedFrameRun> RunOversizedFrameAsync(Func<byte[], JournalWorkItem> createItem)
    {
        WriteHeaderOnlySegment(JournalReadPath.BuildSegmentPath(Dir, 1));

        var options = new PersistenceOptions { DataDir = Dir, JournalMaxSegmentMb = 1, JournalMaxSegmentCount = 4 };
        var counter = new MutableInt32();
        var host = RocksDoubles.CreateEventLoopHost(counter);
        var registry = host.State.PendingAppends;
        using var ring = new BoundedJournalRing(4);
        using var segmentWriter = JournalSegmentWriterFactory.Create();
        var startup = new JournalEventLoopStartup(1, JournalFraming.FileHeaderSize, 1, JournalSegmentProbe.Probe(Dir, 1), new JournalCapacityResyncSignal());
        var eventLoop = new JournalEventLoop(host.Host, ring, segmentWriter, options, startup, NullLogger<JournalEventLoop>.Instance, CancellationToken.None);
        var rollsPublished = 0;
        host.State.OnRollPublished = () =>
        {
            rollsPublished++;
            eventLoop.MarkSegmentRollCompletionPending();
        };

        var buffer = ArrayPool<byte>.Shared.Rent(OversizedFrameLength);
        var item = createItem(buffer);
        registry.Track(item, buffer, OversizedFrameLength, item.Ack);
        _ = Interlocked.Increment(ref counter.Value);
        await ring.EnqueueAsync(item, CancellationToken.None);

        // The shutdown marker behind the frame ends the loop whether the frame is rejected at once or only after the rolls run out.
        await ring.EnqueueAsync(JournalWorkItem.Shutdown(), CancellationToken.None);

        eventLoop.Run();

        var segmentFiles = Directory.GetFiles(Dir, SegmentSearchPattern, SearchOption.TopDirectoryOnly).Length;
        return new OversizedFrameRun(host.State.PipelineFailure, rollsPublished, eventLoop.CurrentSegmentIndex, segmentFiles, counter.Value, registry.PendingCount);
    }

    /// <summary>What an oversized-frame event loop run left behind.</summary>
    /// <param name="PipelineFailure">The pipeline failure the journal thread reported, if any.</param>
    /// <param name="RollsPublished">Number of roll publications (the manifest hand-off).</param>
    /// <param name="CurrentSegmentIndex">The journal thread's current segment index after the run.</param>
    /// <param name="SegmentFiles">Number of journal segment files on disk.</param>
    /// <param name="QueuedAppends">Queued-append counter after the run.</param>
    /// <param name="PendingCount">Appends still tracked in the pending-append registry.</param>
    private sealed record OversizedFrameRun(Exception? PipelineFailure, int RollsPublished, int CurrentSegmentIndex, int SegmentFiles, int QueuedAppends, int PendingCount);

    private sealed class FailingFlush
    {
        private readonly Exception _exception;

        internal FailingFlush(Exception exception)
        {
            _exception = exception;
            ThrowAction = Throw;
        }

        internal Action ThrowAction { get; }

        private void Throw() => throw _exception;
    }

    private sealed class FailingWriteSegmentWriter : IJournalSegmentWriter
    {
        private readonly Exception _failure;

        internal FailingWriteSegmentWriter(Exception failure)
        {
            _failure = failure;
        }

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

        void IJournalSegmentWriter.Write(ReadOnlySpan<byte> buffer, long fileOffset) => throw _failure;
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

        void IJournalEventLoopRollState.SetRollTargetCounted(bool value)
        {
        }

        void IJournalEventLoopRollState.SetSegmentRollInFlight(bool value)
        {
        }

        bool IJournalEventLoopRollState.TryConsumeSegmentRollCompletion() => false;
    }

    private sealed class FakeEventLoopState : IJournalEventLoopState, IDisposable
    {
        private readonly IJournalEventLoopHost _host;
        private readonly PersistenceOptions _options;
        private readonly JournalSegmentPolicy _policy;
        private readonly IJournalSegmentWriter _segmentWriter;
        private readonly JournalWriteBatchBuffer _writeBatch;
        private long _activeBytes;
        private long _totalBytes;

        internal FakeEventLoopState(
            IJournalEventLoopHost host,
            PersistenceOptions options,
            JournalWriteBatchBuffer batch,
            long totalBytes,
            IJournalSegmentWriter? segmentWriter = null)
        {
            _host = host;
            _options = options;
            _policy = new JournalSegmentPolicy(options);
            _writeBatch = batch;
            _totalBytes = totalBytes;
            _segmentWriter = segmentWriter ?? new FakeSegmentWriter();
        }

        public JournalCapacityResyncSignal CapacityResync { get; } = new();

        public JournalDurabilityGroupCommit? GroupCommit { get; init; }

        long IJournalEventLoopState.ActiveSegmentWrittenBytes => _activeBytes;

        JournalDurabilityGroupCommit? IJournalEventLoopState.GroupCommit => GroupCommit;

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

        void IJournalEventLoopState.SetOpenCreatesSegment(bool value)
        {
        }
    }

    private sealed class FakeSegmentWriter : IJournalSegmentWriter
    {
        private readonly long _length;

        /// <summary>Initializes a new instance of the <see cref="FakeSegmentWriter" /> class.</summary>
        /// <param name="length">Length every opened segment reports.</param>
        internal FakeSegmentWriter(long length = 0L)
        {
            _length = length;
        }

        long IJournalSegmentWriter.Length => _length;

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

    private sealed class RecordingSegmentWriter : IJournalSegmentWriter
    {
        private readonly List<string> _calls = [];

        public Action? OnWrite { get; set; }

        /// <summary>Gets the recorded calls joined in call order.</summary>
        public string Trace => string.Join(',', _calls);

        public Exception? WriteFailure { get; init; }

        long IJournalSegmentWriter.Length => 0;

        /// <summary>Releases test resources.</summary>
        public void Dispose()
        {
        }

        void IJournalSegmentWriter.FlushToDisk() => _calls.Add("FlushToDisk");

        void IJournalSegmentWriter.OpenSegment(string path, bool append)
        {
        }

        void IJournalSegmentWriter.Truncate(long length) => _calls.Add($"Truncate:{length}");

        void IJournalSegmentWriter.Write(ReadOnlySpan<byte> buffer, long fileOffset)
        {
            _calls.Add("Write");
            OnWrite?.Invoke();
            if (WriteFailure != null)
                throw WriteFailure;
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
