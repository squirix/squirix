using System;
using System.IO;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
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
    private const int LeakedOnShutdownEventId = 4009;

    private static readonly TimeSpan ShutdownBudget = TimeSpan.FromMilliseconds(200);
    private static readonly TimeSpan StallTimeout = TimeSpan.FromSeconds(10);

    /// <summary>Disposing the log while a caller's frame flush never returns faults that caller with <see cref="ObjectDisposedException" /> within the budget.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task DisposeReleasesCallerStuckInLogFlush(CancellationToken cancellationToken)
    {
        using var hooks = new StallableFollowerLogFaultHooks();
        var log = await OpenLogAsync(hooks, new EventRecordingLogger(), cancellationToken);
        try
        {
            hooks.StallNextFrameWrite();
            var append = log.AppendAsync(Append(1UL, "a"), cancellationToken);
            await hooks.Entered.WaitAsync(StallTimeout, TimeProvider.System, cancellationToken);
            _ = await Assert.That(append.IsCompleted).IsFalse();

            await log.DisposeAsync().AsTask().WaitAsync(StallTimeout, TimeProvider.System, cancellationToken);

            _ = await NodeAsyncAssert.ThrowsAsync<ObjectDisposedException>(append.WaitAsync(StallTimeout, TimeProvider.System, cancellationToken));
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
        var log = await OpenLogAsync(hooks, logger, cancellationToken);
        try
        {
            hooks.StallNextFrameWrite();
            var append = log.AppendAsync(Append(1UL, "a"), cancellationToken);
            await hooks.Entered.WaitAsync(StallTimeout, TimeProvider.System, cancellationToken);

            await log.DisposeAsync().AsTask().WaitAsync(StallTimeout, TimeProvider.System, cancellationToken);
            await log.DisposeAsync();
            _ = await NodeAsyncAssert.ThrowsAsync<ObjectDisposedException>(append.WaitAsync(StallTimeout, TimeProvider.System, cancellationToken));

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
        var log = await OpenLogAsync(hooks, new EventRecordingLogger(), cancellationToken);
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
            await log.DisposeAsync().AsTask().WaitAsync(StallTimeout, TimeProvider.System, cancellationToken);
            _ = await NodeAsyncAssert.ThrowsAsync<ObjectDisposedException>(append.WaitAsync(StallTimeout, TimeProvider.System, cancellationToken));

            hooks.Release();
            await hooks.Flushed.WaitAsync(StallTimeout, TimeProvider.System, cancellationToken);

            _ = await Assert.That(state.LastLogIndex).IsEqualTo(1UL);
            _ = await Assert.That(state.Meta.LastLogIndex).IsEqualTo(1UL);
            _ = await Assert.That(durable.LogLength).IsEqualTo(lengthBefore);
        }
        finally
        {
            ReleaseAndCloseLeakedHandle(log, hooks);
        }

        await using var reopened = new FollowerLog(Dir, GroupId, GroupComposition.Create(GroupId));
        await reopened.OpenAsync(cancellationToken);
        var tail = await reopened.GetUncommittedTailAsync(cancellationToken);

        _ = await Assert.That(reopened.Readiness).IsEqualTo(FollowerLogReadiness.Ready);
        _ = await Assert.That(FollowerLogTestKit.Payload(await reopened.GetCommittedEntriesAsync(cancellationToken))).IsEqualTo("a");
        _ = await Assert.That(tail).HasSingleItem();
        _ = await Assert.That(tail[0].LogIndex).IsEqualTo(2UL);
    }

    /// <summary>A flush that fails after the dispose faulted its caller keeps the shutdown fault and changes neither readiness nor memory.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task LateFlushFailureKeepsDisposeFault(CancellationToken cancellationToken)
    {
        using var hooks = new StallableFollowerLogFaultHooks();
        var logger = new EventRecordingLogger();
        var log = await OpenLogAsync(hooks, logger, cancellationToken);
        IFollowerLogState state = log;
        try
        {
            hooks.StallNextFrameWrite();
            var append = log.AppendAsync(Append(1UL, "a"), cancellationToken);
            await hooks.Entered.WaitAsync(StallTimeout, TimeProvider.System, cancellationToken);
            await log.DisposeAsync().AsTask().WaitAsync(StallTimeout, TimeProvider.System, cancellationToken);

            hooks.ReleaseWithFailure(new IOException("Injected flush failure after the dispose."));
            await hooks.Exited.WaitAsync(StallTimeout, TimeProvider.System, cancellationToken);

            _ = await NodeAsyncAssert.ThrowsAsync<ObjectDisposedException>(append.WaitAsync(StallTimeout, TimeProvider.System, cancellationToken));
            _ = await Assert.That(log.Readiness).IsEqualTo(FollowerLogReadiness.Ready);
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
        var log = await OpenLogAsync(hooks, new EventRecordingLogger(), cancellationToken);
        try
        {
            hooks.StallNextFrameWrite();
            var append = log.AppendAsync(Append(1UL, "a"), cancellationToken);
            await hooks.Entered.WaitAsync(StallTimeout, TimeProvider.System, cancellationToken);
            var commit = log.AdvanceCommitAsync(1UL, CancellationToken.None);
            var applied = log.AdvanceAppliedAsync(1UL, CancellationToken.None);
            _ = await Assert.That(commit.IsCompleted || applied.IsCompleted).IsFalse();

            await log.DisposeAsync().AsTask().WaitAsync(StallTimeout, TimeProvider.System, cancellationToken);

            _ = await NodeAsyncAssert.ThrowsAsync<ObjectDisposedException>(commit.WaitAsync(StallTimeout, TimeProvider.System, cancellationToken));
            _ = await NodeAsyncAssert.ThrowsAsync<ObjectDisposedException>(applied.WaitAsync(StallTimeout, TimeProvider.System, cancellationToken));
            _ = await NodeAsyncAssert.ThrowsAsync<ObjectDisposedException>(append.WaitAsync(StallTimeout, TimeProvider.System, cancellationToken));
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
        var log = await OpenLogAsync(hooks, new EventRecordingLogger(), cancellationToken);
        IFollowerLogState state = log;
        var metadataTempPath = FollowerLogPaths.Create(Dir, GroupId).MetadataTempPath;
        try
        {
            _ = await log.AppendAsync(Append(1UL, "a"), cancellationToken);

            hooks.StallNextMetaWrite();
            var commit = log.AdvanceCommitAsync(1UL, cancellationToken);
            await hooks.Entered.WaitAsync(StallTimeout, TimeProvider.System, cancellationToken);
            await log.DisposeAsync().AsTask().WaitAsync(StallTimeout, TimeProvider.System, cancellationToken);

            _ = await NodeAsyncAssert.ThrowsAsync<ObjectDisposedException>(commit.WaitAsync(StallTimeout, TimeProvider.System, cancellationToken));
            _ = await Assert.That(log.Readiness).IsEqualTo(FollowerLogReadiness.Ready);
            _ = await Assert.That(state.Meta.CommitIndex).IsEqualTo(0UL);

            // The released write still publishes the metadata; it must finish before the directory is removed.
            hooks.Release();
            await hooks.Exited.WaitAsync(StallTimeout, TimeProvider.System, cancellationToken);
            _ = await Assert.That(SpinWait.SpinUntil(() => !File.Exists(metadataTempPath), StallTimeout)).IsTrue();
        }
        finally
        {
            ReleaseAndCloseLeakedHandle(log, hooks);
        }
    }

    /// <summary>Without a stall the dispose drains within the budget, closes the handle, logs no leak, and later callers get NotReady.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task DisposeWithoutStallClosesHandle(CancellationToken cancellationToken)
    {
        using var hooks = new StallableFollowerLogFaultHooks();
        var logger = new EventRecordingLogger();
        var log = await OpenLogAsync(hooks, logger, cancellationToken);
        IFollowerLogDurability durable = log;
        _ = await log.AppendAsync(Append(1UL, "a"), cancellationToken);

        await log.DisposeAsync().AsTask().WaitAsync(StallTimeout, TimeProvider.System, cancellationToken);
        var refused = await log.AppendAsync(Append(2UL, "b"), cancellationToken);

        _ = await Assert.That(refused.Success).IsFalse();
        _ = await Assert.That(refused.RefusalCode).IsEqualTo(FollowerLogRefusal.NotReady);
        _ = await Assert.That(logger.Count(LeakedOnShutdownEventId)).IsEqualTo(0);
        _ = NodeExceptionAssert.For<InvalidOperationException>().Throws(durable.Durability, static durability => durability.Flush());
    }

    private static FollowerLogAppendRequest Append(ulong index, string payload) => new(
        "leader-1",
        1UL,
        index - 1,
        index == 1UL ? 0UL : 1UL,
        0UL,
        ReadOnlyMemory<FollowerLogEntry>.Of(new FollowerLogEntry(index, 1UL, Encoding.UTF8.GetBytes(payload))));

    /// <summary>Releases the stall and closes the handle the timed-out dispose leaked, as the process exit would.</summary>
    /// <param name="log">The disposed log.</param>
    /// <param name="hooks">The stall hooks of the log.</param>
    private static void ReleaseAndCloseLeakedHandle(FollowerLog log, StallableFollowerLogFaultHooks hooks)
    {
        hooks.Release();
        IFollowerLogDurability durable = log;
        durable.Durability.Dispose();
    }

    private async Task<FollowerLog> OpenLogAsync(StallableFollowerLogFaultHooks hooks, ILogger logger, CancellationToken cancellationToken)
    {
        var options = new FollowerLogOptions { FaultHooks = hooks, Log = logger, ShutdownBudget = ShutdownBudget };
        var log = new FollowerLog(Dir, GroupId, GroupComposition.Create(GroupId), options);
        try
        {
            await log.OpenAsync(cancellationToken);
        }
        catch
        {
            await log.DisposeAsync();
            throw;
        }

        return log;
    }
}
