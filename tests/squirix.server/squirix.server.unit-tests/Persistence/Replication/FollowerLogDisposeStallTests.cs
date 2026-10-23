using System;
using System.IO;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Time.Testing;
using Squirix.Server.Attributes;
using Squirix.Server.Storage.Replication;
using Squirix.Server.TestKit;
using Squirix.Server.TestKit.Replication;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Persistence.Replication;

/// <summary>A follower-log dispose releases the callers of a durable write that never returns and leaks the stuck thread loudly.</summary>
[Immutable]
public sealed class FollowerLogDisposeStallTests : IsolatedStorageTestBase
{
    private const string GroupId = "grp-1";
    private const int AppendUnconfirmedEventId = 4077;
    private const int LeakedOnShutdownEventId = 4009;

    private static readonly TimeSpan ShutdownBudget = TimeSpan.FromMilliseconds(200);
    private static readonly TimeSpan StallTimeout = TimeSpan.FromSeconds(10);

    /// <summary>Disposing the log while a caller's frame flush never returns reports that caller's append as unconfirmed, caused by <see cref="ObjectDisposedException" />, within the budget.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task DisposeReleasesCallerStuckInLogFlush(CancellationToken cancellationToken)
    {
        using var hooks = new StallableFollowerLogFaultHooks();
        var clock = new FakeTimeProvider();
        var logger = new EventRecordingLogger();
        var log = await OpenLogOnClockAsync(hooks, logger, clock, cancellationToken);
        try
        {
            hooks.StallNextFrameWrite();
            var append = log.AppendAsync(Append(1UL, "a"), cancellationToken);
            await hooks.Entered.WaitAsync(StallTimeout, TimeProvider.System, cancellationToken);
            _ = await Assert.That(append.IsCompleted).IsFalse();

            await DisposeStalledAsync(log, clock, cancellationToken);

            await AssertUnconfirmedByShutdownAsync(append, logger, cancellationToken);
            hooks.Release();
            await hooks.Flushed.WaitAsync(StallTimeout, TimeProvider.System, cancellationToken);
        }
        finally
        {
            ReleaseAndCloseLeakedHandle(log, hooks);
        }
    }

    /// <summary>
    /// The timed-out dispose logs the leak once and leaves the handle open under the stuck flush: once released, the flush completes on
    /// the same handle instead of failing on a closed one.
    /// </summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task StuckFlushDisposeLeaksHandleLoudly(CancellationToken cancellationToken)
    {
        using var hooks = new StallableFollowerLogFaultHooks();
        var logger = new EventRecordingLogger();
        var clock = new FakeTimeProvider();
        var log = await OpenLogOnClockAsync(hooks, logger, clock, cancellationToken);
        try
        {
            hooks.StallNextFrameWrite();
            var append = log.AppendAsync(Append(1UL, "a"), cancellationToken);
            await hooks.Entered.WaitAsync(StallTimeout, TimeProvider.System, cancellationToken);

            await DisposeStalledAsync(log, clock, cancellationToken);
            await log.DisposeAsync();
            await AssertUnconfirmedByShutdownAsync(append, logger, cancellationToken);

            _ = await Assert.That(logger.Count(LeakedOnShutdownEventId)).IsEqualTo(1);
            _ = await Assert.That(logger.Find(LeakedOnShutdownEventId)?.Level).IsEqualTo(LogLevel.Error);

            hooks.Release();
            await hooks.Flushed.WaitAsync(StallTimeout, TimeProvider.System, cancellationToken);
        }
        finally
        {
            ReleaseAndCloseLeakedHandle(log, hooks);
        }
    }

    /// <summary>
    /// A flush that lands after the dispose faulted its caller changes no in-memory state, and a restart recovers the late frame as an
    /// uncommitted tail, as after a crash between the frame flush and the metadata publication.
    /// </summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task LateFlushAfterDisposeIsNoOp(CancellationToken cancellationToken)
    {
        using var hooks = new StallableFollowerLogFaultHooks();
        var clock = new FakeTimeProvider();
        var logger = new EventRecordingLogger();
        var log = await OpenLogOnClockAsync(hooks, logger, clock, cancellationToken);
        IFollowerLogState state = log;
        IFollowerLogDurability durable = log;
        try
        {
            _ = await log.AppendAsync(Append(1UL, "a"), cancellationToken);
            _ = await log.AdvanceCommitAsync(1UL, cancellationToken);
            var lengthBefore = durable.LogLength;

            hooks.StallNextFrameWrite();
            var append = log.AppendAsync(Append(2UL, "b"), cancellationToken);
            await hooks.Entered.WaitAsync(StallTimeout, TimeProvider.System, cancellationToken);
            await DisposeStalledAsync(log, clock, cancellationToken);
            await AssertUnconfirmedByShutdownAsync(append, logger, cancellationToken);

            hooks.Release();
            await hooks.Flushed.WaitAsync(StallTimeout, TimeProvider.System, cancellationToken);

            _ = await Assert.That(state.LastLogIndex).IsEqualTo(1UL);
            _ = await Assert.That(durable.LogLength).IsEqualTo(lengthBefore);
        }
        finally
        {
            ReleaseAndCloseLeakedHandle(log, hooks);
        }

        await using var reopened = new FollowerLog(Dir, GroupId, GroupComposition.Create(GroupId), NullLogger<FollowerLog>.Instance, TimeProvider.System);
        await reopened.OpenAsync(cancellationToken);
        var tail = await reopened.GetUncommittedTailAsync(cancellationToken);

        _ = await Assert.That(reopened.Readiness).IsEqualTo(FollowerLogReadiness.Ready);
        _ = await Assert.That(FollowerLogTestKit.Payload(await reopened.GetCommittedEntriesAsync(cancellationToken))).IsEqualTo("a");
        _ = await Assert.That(tail).HasSingleItem();
        _ = await Assert.That(tail[0].LogIndex).IsEqualTo(2UL);
    }

    /// <summary>A flush that fails after the dispose cut the append short keeps the shutdown fault and leaves memory untouched; the log stays latched failed.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task LateFlushFailureKeepsDisposeFault(CancellationToken cancellationToken)
    {
        using var hooks = new StallableFollowerLogFaultHooks();
        var logger = new EventRecordingLogger();
        var clock = new FakeTimeProvider();
        var log = await OpenLogOnClockAsync(hooks, logger, clock, cancellationToken);
        IFollowerLogState state = log;
        try
        {
            hooks.StallNextFrameWrite();
            var append = log.AppendAsync(Append(1UL, "a"), cancellationToken);
            await hooks.Entered.WaitAsync(StallTimeout, TimeProvider.System, cancellationToken);
            await DisposeStalledAsync(log, clock, cancellationToken);

            hooks.ReleaseWithFailure(new IOException("Injected flush failure after the dispose."));
            await hooks.Exited.WaitAsync(StallTimeout, TimeProvider.System, cancellationToken);

            await AssertUnconfirmedByShutdownAsync(append, logger, cancellationToken);
            _ = await Assert.That(log.Readiness).IsEqualTo(FollowerLogReadiness.Failed);
            _ = await Assert.That(state.LastLogIndex).IsEqualTo(0UL);
            _ = await Assert.That(logger.Count(LeakedOnShutdownEventId)).IsEqualTo(1);
        }
        finally
        {
            ReleaseAndCloseLeakedHandle(log, hooks);
        }
    }

    /// <summary>Callers queued on the gate behind the stuck holder, even without a cancelable token, are released by the dispose too.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task DisposeReleasesQueuedCallersBehindStall(CancellationToken cancellationToken)
    {
        using var hooks = new StallableFollowerLogFaultHooks();
        var clock = new FakeTimeProvider();
        var logger = new EventRecordingLogger();
        var log = await OpenLogOnClockAsync(hooks, logger, clock, cancellationToken);
        try
        {
            hooks.StallNextFrameWrite();
            var append = log.AppendAsync(Append(1UL, "a"), cancellationToken);
            await hooks.Entered.WaitAsync(StallTimeout, TimeProvider.System, cancellationToken);
            var commit = log.AdvanceCommitAsync(1UL, CancellationToken.None);
            var applied = log.AdvanceAppliedAsync(1UL, CancellationToken.None);
            _ = await Assert.That(commit.IsCompleted || applied.IsCompleted).IsFalse();

            await DisposeStalledAsync(log, clock, cancellationToken);

            _ = await NodeAsyncAssert.ThrowsAsync<ObjectDisposedException>(commit.WaitAsync(StallTimeout, TimeProvider.System, cancellationToken));
            _ = await NodeAsyncAssert.ThrowsAsync<ObjectDisposedException>(applied.WaitAsync(StallTimeout, TimeProvider.System, cancellationToken));
            await AssertUnconfirmedByShutdownAsync(append, logger, cancellationToken);
            _ = await NodeAsyncAssert.ThrowsAsync<ObjectDisposedException>(log.AdvanceCommitAsync(1UL, cancellationToken));
            hooks.Release();
            await hooks.Flushed.WaitAsync(StallTimeout, TimeProvider.System, cancellationToken);
        }
        finally
        {
            ReleaseAndCloseLeakedHandle(log, hooks);
        }
    }

    /// <summary>
    /// A commit-index advance stuck in its metadata write is faulted by the dispose, and the shutdown fault neither fails readiness nor
    /// advances the in-memory commit index.
    /// </summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task StuckMetaFlushFaultsCommitAdvance(CancellationToken cancellationToken)
    {
        using var hooks = new StallableFollowerLogFaultHooks();
        var clock = new FakeTimeProvider();
        var log = await OpenLogOnClockAsync(hooks, new EventRecordingLogger(), clock, cancellationToken);
        IFollowerLogState state = log;
        var metadataPath = GroupStoragePaths.GetMetadataPath(Dir, GroupId);
        try
        {
            _ = await log.AppendAsync(Append(1UL, "a"), cancellationToken);

            hooks.StallNextMetaWrite();
            var commit = log.AdvanceCommitAsync(1UL, cancellationToken);
            await hooks.Entered.WaitAsync(StallTimeout, TimeProvider.System, cancellationToken);
            await DisposeStalledAsync(log, clock, cancellationToken);

            _ = await NodeAsyncAssert.ThrowsAsync<ObjectDisposedException>(commit.WaitAsync(StallTimeout, TimeProvider.System, cancellationToken));
            _ = await Assert.That(log.Readiness).IsEqualTo(FollowerLogReadiness.Ready);
            _ = await Assert.That(state.Meta.CommitIndex).IsEqualTo(0UL);

            // The released write still lands in the metadata file through the leaked handle; it must finish before the directory is removed.
            hooks.Release();
            await hooks.Exited.WaitAsync(StallTimeout, TimeProvider.System, cancellationToken);
            _ = await Assert.That(SpinWait.SpinUntil(() => GroupMetadataStore.Read(metadataPath).Metadata.CommitIndex == 1UL, StallTimeout)).IsTrue();
        }
        finally
        {
            ReleaseAndCloseLeakedHandle(log, hooks);
        }
    }

    /// <summary>A dispose that cuts the commit-index write behind already durable frames reports the append as unconfirmed and latches the log.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task DisposeDuringCommitWriteIsUnconfirmed(CancellationToken cancellationToken)
    {
        using var hooks = new StallableFollowerLogFaultHooks();
        var clock = new FakeTimeProvider();
        var logger = new EventRecordingLogger();
        var log = await OpenLogOnClockAsync(hooks, logger, clock, cancellationToken);
        try
        {
            _ = await log.AppendAsync(Append(1UL, "a"), cancellationToken);
            var request = new FollowerLogAppendRequest(
                "leader-1",
                1UL,
                1UL,
                1UL,
                1UL,
                ReadOnlyMemory<FollowerLogEntry>.Of(new FollowerLogEntry(2UL, 1UL, Encoding.UTF8.GetBytes("b"))));

            hooks.StallNextMetaWrite();
            var append = log.AppendAsync(request, cancellationToken);
            await hooks.Entered.WaitAsync(StallTimeout, TimeProvider.System, cancellationToken);
            await DisposeStalledAsync(log, clock, cancellationToken);

            await AssertUnconfirmedByShutdownAsync(append, logger, cancellationToken);
            _ = await Assert.That(log.Readiness).IsEqualTo(FollowerLogReadiness.Failed);

            hooks.Release();
            await hooks.Exited.WaitAsync(StallTimeout, TimeProvider.System, cancellationToken);

            // Exited precedes the publication, so the released write must land before the files are reopened.
            var metadataPath = GroupStoragePaths.GetMetadataPath(Dir, GroupId);
            _ = await Assert.That(SpinWait.SpinUntil(() => GroupMetadataStore.Read(metadataPath).Metadata.CommitIndex == 1UL, StallTimeout)).IsTrue();
        }
        finally
        {
            ReleaseAndCloseLeakedHandle(log, hooks);
        }

        await using var reopened = new FollowerLog(Dir, GroupId, GroupComposition.Create(GroupId), NullLogger<FollowerLog>.Instance, TimeProvider.System);
        await reopened.OpenAsync(cancellationToken);
        var status = await reopened.GetStatusAsync(cancellationToken);

        _ = await Assert.That(status.LastLogIndex).IsEqualTo(2UL);
    }

    /// <summary>A dispose that cuts the term write ahead of the frames is a definite failure: nothing was written, so the log stays ready.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task DisposeDuringTermWriteIsRefused(CancellationToken cancellationToken)
    {
        using var hooks = new StallableFollowerLogFaultHooks();
        var clock = new FakeTimeProvider();
        var log = await OpenLogOnClockAsync(hooks, new EventRecordingLogger(), clock, cancellationToken);
        try
        {
            var request = new FollowerLogAppendRequest(
                "leader-1",
                2UL,
                0UL,
                0UL,
                0UL,
                ReadOnlyMemory<FollowerLogEntry>.Of(new FollowerLogEntry(1UL, 2UL, Encoding.UTF8.GetBytes("a"))));

            hooks.StallNextMetaWrite();
            var append = log.AppendAsync(request, cancellationToken);
            await hooks.Entered.WaitAsync(StallTimeout, TimeProvider.System, cancellationToken);
            await DisposeStalledAsync(log, clock, cancellationToken);

            _ = await NodeAsyncAssert.ThrowsAsync<ReplicaShutdownRefusedException>(append.WaitAsync(StallTimeout, TimeProvider.System, cancellationToken));
            _ = await Assert.That(log.Readiness).IsEqualTo(FollowerLogReadiness.Ready);

            hooks.Release();
            await hooks.Exited.WaitAsync(StallTimeout, TimeProvider.System, cancellationToken);

            // Exited precedes the publication, so the released write must land before the files are reopened.
            var metadataPath = GroupStoragePaths.GetMetadataPath(Dir, GroupId);
            _ = await Assert.That(SpinWait.SpinUntil(() => GroupMetadataStore.Read(metadataPath).Metadata.CurrentTerm == 2UL, StallTimeout)).IsTrue();
        }
        finally
        {
            ReleaseAndCloseLeakedHandle(log, hooks);
        }

        await using var reopened = new FollowerLog(Dir, GroupId, GroupComposition.Create(GroupId), NullLogger<FollowerLog>.Instance, TimeProvider.System);
        await reopened.OpenAsync(cancellationToken);
        var status = await reopened.GetStatusAsync(cancellationToken);

        _ = await Assert.That(status.LastLogIndex).IsEqualTo(0UL);
    }

    /// <summary>A caller queued behind a stuck holder, and a later caller, are refused as shutdown before any frame is scheduled; the holder stays unconfirmed.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task QueuedAppendRefusedOnShutdown(CancellationToken cancellationToken)
    {
        using var hooks = new StallableFollowerLogFaultHooks();
        var clock = new FakeTimeProvider();
        var logger = new EventRecordingLogger();
        var log = await OpenLogOnClockAsync(hooks, logger, clock, cancellationToken);
        try
        {
            hooks.StallNextFrameWrite();
            var holder = log.AppendAsync(Append(1UL, "a"), cancellationToken);
            await hooks.Entered.WaitAsync(StallTimeout, TimeProvider.System, cancellationToken);
            var queued = log.AppendAsync(Append(2UL, "b"), CancellationToken.None);
            _ = await Assert.That(queued.IsCompleted).IsFalse();

            await DisposeStalledAsync(log, clock, cancellationToken);

            _ = await NodeAsyncAssert.ThrowsAsync<ReplicaShutdownRefusedException>(queued.WaitAsync(StallTimeout, TimeProvider.System, cancellationToken));
            _ = await NodeAsyncAssert.ThrowsAsync<ReplicaShutdownRefusedException>(log.AppendAsync(Append(2UL, "b"), cancellationToken));
            _ = await NodeAsyncAssert.ThrowsAsync<ReplicaShutdownRefusedException>(
                log.AppendAsync(Append(2UL, "b"), ReadOnlyMemory<byte>.Empty, 0UL, cancellationToken));
            var failure = await NodeAsyncAssert.ThrowsAsync<GroupLogAppendUnconfirmedException>(holder.WaitAsync(StallTimeout, TimeProvider.System, cancellationToken));
            _ = await Assert.That(failure.InnerException).IsNotTypeOf<ReplicaShutdownRefusedException>();
            _ = await Assert.That(failure.InnerException).IsTypeOf<ObjectDisposedException>();
            hooks.Release();
            await hooks.Flushed.WaitAsync(StallTimeout, TimeProvider.System, cancellationToken);
        }
        finally
        {
            ReleaseAndCloseLeakedHandle(log, hooks);
        }
    }

    /// <summary>
    /// Without a stall the dispose drains without the budget ever expiring (its clock never moves), closes the handle, logs no leak, and
    /// later callers get NotReady.
    /// </summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task DisposeWithoutStallClosesHandle(CancellationToken cancellationToken)
    {
        using var hooks = new StallableFollowerLogFaultHooks();
        var logger = new EventRecordingLogger();
        var log = await OpenLogOnClockAsync(hooks, logger, new FakeTimeProvider(), cancellationToken);
        IFollowerLogDurability durable = log;
        _ = await log.AppendAsync(Append(1UL, "a"), cancellationToken);

        await log.DisposeAsync().AsTask().WaitAsync(StallTimeout, TimeProvider.System, cancellationToken);
        var refused = await log.AppendAsync(Append(2UL, "b"), cancellationToken);
        var refusedWithTopology = await log.AppendAsync(Append(2UL, "b"), ReadOnlyMemory<byte>.Empty, 0UL, cancellationToken);

        _ = await Assert.That(log.IsDisposed).IsTrue();
        _ = await Assert.That(refused.Success).IsFalse();
        _ = await Assert.That(refused.RefusalCode).IsEqualTo(FollowerLogRefusal.NotReady);
        _ = await Assert.That(refusedWithTopology.Success).IsFalse();
        _ = await Assert.That(refusedWithTopology.RefusalCode).IsEqualTo(FollowerLogRefusal.NotReady);
        _ = await Assert.That(logger.Count(LeakedOnShutdownEventId)).IsEqualTo(0);
        _ = await Assert.That(logger.Count(AppendUnconfirmedEventId)).IsEqualTo(0);
        _ = NodeExceptionAssert.For<InvalidOperationException>().Throws(durable.Durability, static durability => durability.Flush());
    }

    private static FollowerLogAppendRequest Append(ulong index, string payload) => new(
        "leader-1",
        1UL,
        index - 1,
        index == 1UL ? 0UL : 1UL,
        0UL,
        ReadOnlyMemory<FollowerLogEntry>.Of(new FollowerLogEntry(index, 1UL, Encoding.UTF8.GetBytes(payload))));

    private static bool OpensForWrite(string path)
    {
        try
        {
            using var handle = File.OpenHandle(path, FileMode.Open, FileAccess.ReadWrite, FileShare.Read);
            return true;
        }
        catch (IOException)
        {
            return false;
        }
    }

    private static async Task AssertUnconfirmedByShutdownAsync(Task append, EventRecordingLogger logger, CancellationToken cancellationToken)
    {
        var failure = await NodeAsyncAssert.ThrowsAsync<GroupLogAppendUnconfirmedException>(append.WaitAsync(StallTimeout, TimeProvider.System, cancellationToken));

        _ = await Assert.That(failure.InnerException).IsTypeOf<ObjectDisposedException>();
        _ = await Assert.That(logger.Count(AppendUnconfirmedEventId)).IsEqualTo(1);
    }

    private static async Task DisposeStalledAsync(FollowerLog log, FakeTimeProvider clock, CancellationToken cancellationToken)
    {
        // The budget timer is armed synchronously by DisposeAsync, so advancing the shutdown clock expires it deterministically.
        var disposing = log.DisposeAsync().AsTask();
        clock.Advance(ShutdownBudget);
        await disposing.WaitAsync(StallTimeout, TimeProvider.System, cancellationToken);
    }

    private Task<FollowerLog> OpenLogOnClockAsync(
        StallableFollowerLogFaultHooks hooks,
        ILogger<FollowerLog> logger,
        TimeProvider clock,
        CancellationToken cancellationToken) =>
        OpenLogAsync(
            new FollowerLogOptions { TimeProvider = TimeProvider.System, FaultHooks = hooks, ShutdownBudget = ShutdownBudget, ShutdownTimeProvider = clock },
            logger,
            cancellationToken);

    private async Task<FollowerLog> OpenLogAsync(FollowerLogOptions options, ILogger<FollowerLog> logger, CancellationToken cancellationToken)
    {
        var log = new FollowerLog(Dir, GroupId, GroupComposition.Create(GroupId), logger, options);
        try
        {
            await log.OpenSeededAsync(cancellationToken);
        }
        catch
        {
            await log.DisposeAsync();
            throw;
        }

        return log;
    }

    /// <summary>Releases the stall and closes the handle the timed-out dispose leaked, as the process exit would.</summary>
    /// <param name="log">The disposed log.</param>
    /// <param name="hooks">The stall hooks of the log.</param>
    /// <remarks>
    /// The stall exits before the released metadata write and its flush, and a handle disposed under them closes only once they return, so
    /// this waits until the metadata file opens for writing again; otherwise the reopen that follows can find it still held.
    /// </remarks>
    private void ReleaseAndCloseLeakedHandle(FollowerLog log, StallableFollowerLogFaultHooks hooks)
    {
        hooks.Release();
        IFollowerLogDurability durable = log;
        durable.Durability.Dispose();
        log.Metadata.Dispose();
        var metadataPath = GroupStoragePaths.GetMetadataPath(Dir, GroupId);
        _ = SpinWait.SpinUntil(() => OpensForWrite(metadataPath), StallTimeout);
    }
}
