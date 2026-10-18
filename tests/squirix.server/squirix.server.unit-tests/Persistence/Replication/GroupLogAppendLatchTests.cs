using System;
using System.IO;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Squirix.Server.Attributes;
using Squirix.Server.Storage.Replication;
using Squirix.Server.TestKit;
using Squirix.Server.TestKit.IO;
using Squirix.Server.TestKit.Replication;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Persistence.Replication;

/// <summary>A group log that fails after its frames were handed to the storage reports the append as unconfirmed and refuses every append after it.</summary>
[Immutable]
public sealed class GroupLogAppendLatchTests : ServerUnitTestBase
{
    private const string GroupId = "grp-latch";
    private const int AppendUnconfirmedEventId = 4077;
    private const string LeaderId = "leader-1";
    private const ulong Term = 1UL;

    /// <summary>A failure after the frame was written fails the append unconfirmed with its cause, and latches the log with one error line.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task FrameWriteFailureLatchesLog(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-latch-write");
        var hooks = new FailingFrameWriteHooks();
        var events = new EventRecordingLogger();
        await using var log = Create(dir, events, hooks);
        await log.OpenSeededAsync(cancellationToken);
        _ = await log.AppendAsync(Append(1UL, "a"), cancellationToken);

        hooks.Arm();
        var failure = await NodeAsyncAssert.ThrowsAsync<GroupLogAppendUnconfirmedException>(log.AppendAsync(Append(2UL, "b"), cancellationToken));

        _ = await Assert.That(failure.InnerException).IsTypeOf<IOException>();
        _ = await Assert.That(log.Readiness).IsEqualTo(FollowerLogReadiness.Failed);
        _ = await Assert.That(events.Count(AppendUnconfirmedEventId)).IsEqualTo(1);
        _ = await Assert.That(events.Find(AppendUnconfirmedEventId)?.Level).IsEqualTo(LogLevel.Error);
        _ = await Assert.That(events.FindMessage(AppendUnconfirmedEventId)).Contains(GroupId);
        _ = await Assert.That(FollowerLogTestKit.GetLogLength(GroupLogFile.ActivePath(dir, GroupId))).IsGreaterThan(0L);
    }

    /// <summary>A failure of the flush that follows the written frame latches the log the same way.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task FlushFailureLatchesLog(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-latch-flush");
        var hooks = new FailingFrameWriteHooks();
        await using var log = Create(dir, new EventRecordingLogger(), hooks);
        await log.OpenSeededAsync(cancellationToken);

        hooks.ArmFlush();
        _ = await NodeAsyncAssert.ThrowsAsync<GroupLogAppendUnconfirmedException>(log.AppendAsync(Append(1UL, "a"), cancellationToken));

        _ = await Assert.That(log.Readiness).IsEqualTo(FollowerLogReadiness.Failed);
    }

    /// <summary>
    /// A latched log refuses the next append as not ready, writes no frame for it even when the storage recovered, and logs nothing more; the
    /// entry whose append failed is in the file after a restart.
    /// </summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task LatchedLogRefusesNextAppend(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-latch-refuse");
        var hooks = new FailingFrameWriteHooks();
        var events = new EventRecordingLogger();
        long lengthAtLatch;
        await using (var log = Create(dir, events, hooks))
        {
            await log.OpenSeededAsync(cancellationToken);
            hooks.Arm();
            _ = await NodeAsyncAssert.ThrowsAsync<GroupLogAppendUnconfirmedException>(log.AppendAsync(Append(1UL, "a"), cancellationToken));
            hooks.Disarm();
            lengthAtLatch = FollowerLogTestKit.GetLogLength(GroupLogFile.ActivePath(dir, GroupId));

            var next = await log.AppendAsync(Append(1UL, "a"), cancellationToken);

            _ = await Assert.That(next.Success).IsFalse();
            _ = await Assert.That(next.RefusalCode).IsEqualTo(FollowerLogRefusal.NotReady);
            _ = await Assert.That(FollowerLogTestKit.GetLogLength(GroupLogFile.ActivePath(dir, GroupId))).IsEqualTo(lengthAtLatch);
            _ = await Assert.That(events.Count(AppendUnconfirmedEventId)).IsEqualTo(1);
        }

        await using var reopened = Create(dir, new EventRecordingLogger(), new FailingFrameWriteHooks());
        await reopened.OpenAsync(cancellationToken);
        var status = await reopened.GetStatusAsync(cancellationToken);

        _ = await Assert.That(status.Readiness).IsEqualTo(FollowerLogReadiness.Ready);
        _ = await Assert.That(status.LastLogIndex).IsEqualTo(1UL);
    }

    private static FollowerLogAppendRequest Append(ulong index, string payload) => new(
        LeaderId,
        Term,
        index - 1,
        index == 1UL ? 0UL : Term,
        0UL,
        ReadOnlyMemory<FollowerLogEntry>.Of(new FollowerLogEntry(index, Term, Encoding.UTF8.GetBytes(payload))));

    private static FollowerLog Create(string dir, EventRecordingLogger logger, IFollowerLogFaultHooks hooks) =>
        new(
            dir,
            GroupId,
            GroupComposition.Create(GroupId),
            logger,
            new FollowerLogOptions { ShutdownTimeProvider = TimeProvider.System, TimeProvider = TimeProvider.System, FaultHooks = hooks });
}
