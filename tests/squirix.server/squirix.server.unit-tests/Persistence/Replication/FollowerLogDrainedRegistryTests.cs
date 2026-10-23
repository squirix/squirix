using System;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Squirix.Server.Attributes;
using Squirix.Server.Storage.Replication;
using Squirix.Server.TestKit;
using Squirix.Server.TestKit.Replication;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Persistence.Replication;

/// <summary>A follower log whose ack registry was closed by a dispose refuses a write as shutdown when no frame of it was scheduled.</summary>
[Immutable]
public sealed class FollowerLogDrainedRegistryTests : IsolatedStorageTestBase
{
    private const string GroupId = "grp-1";
    private const int AppendUnconfirmedEventId = 4077;
    private const int TruncateUnconfirmedEventId = 4078;

    /// <summary>An append whose frames the drained ack registry refuses to track is refused as shutdown, leaves the log ready, and writes nothing.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task AppendRefusedByDrainedRegistry(CancellationToken cancellationToken)
    {
        var logger = new EventRecordingLogger();
        await using (var log = await OpenLogAsync(new FollowerLogOptions { TimeProvider = TimeProvider.System, ShutdownTimeProvider = TimeProvider.System }, logger, cancellationToken))
        {
            DrainRegistry(log);

            _ = await NodeAsyncAssert.ThrowsAsync<ReplicaShutdownRefusedException>(log.AppendAsync(Append(1UL, "a"), cancellationToken));

            _ = await Assert.That(log.Readiness).IsEqualTo(FollowerLogReadiness.Ready);
            _ = await Assert.That(logger.Count(AppendUnconfirmedEventId)).IsEqualTo(0);
        }

        await AssertReopenedLastIndexAsync(0UL, cancellationToken);
    }

    /// <summary>A higher term whose metadata write the drained ack registry refuses is refused as shutdown before any frame is scheduled.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task TermWriteRefusedByDrainedRegistry(CancellationToken cancellationToken)
    {
        await using (var log = await OpenLogAsync(new FollowerLogOptions { TimeProvider = TimeProvider.System, ShutdownTimeProvider = TimeProvider.System }, new EventRecordingLogger(), cancellationToken))
        {
            DrainRegistry(log);
            var request = new FollowerLogAppendRequest(
                "leader-1",
                2UL,
                0UL,
                0UL,
                0UL,
                ReadOnlyMemory<FollowerLogEntry>.Of(new FollowerLogEntry(1UL, 2UL, Encoding.UTF8.GetBytes("a"))));

            _ = await NodeAsyncAssert.ThrowsAsync<ReplicaShutdownRefusedException>(log.AppendAsync(request, cancellationToken));

            _ = await Assert.That(log.Readiness).IsEqualTo(FollowerLogReadiness.Ready);
        }

        await AssertReopenedLastIndexAsync(0UL, cancellationToken);
    }

    /// <summary>A conflicting append whose truncate the drained ack registry refuses is refused as shutdown, and the log latches until a restart re-reads the file.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task TruncateRefusedByDrainedRegistry(CancellationToken cancellationToken)
    {
        var logger = new EventRecordingLogger();
        await using (var log = await OpenLogAsync(new FollowerLogOptions { TimeProvider = TimeProvider.System, ShutdownTimeProvider = TimeProvider.System }, logger, cancellationToken))
        {
            _ = await log.AppendAsync(Append(1UL, "a"), cancellationToken);
            _ = await log.AppendAsync(Append(2UL, "b"), cancellationToken);
            _ = await log.AppendAsync(new FollowerLogAppendRequest("leader-1", 2UL, 2UL, 1UL, 0UL, ReadOnlyMemory<FollowerLogEntry>.Empty), cancellationToken);
            DrainRegistry(log);
            var conflicting = new FollowerLogAppendRequest(
                "leader-1",
                2UL,
                1UL,
                1UL,
                0UL,
                ReadOnlyMemory<FollowerLogEntry>.Of(new FollowerLogEntry(2UL, 2UL, Encoding.UTF8.GetBytes("c"))));

            _ = await NodeAsyncAssert.ThrowsAsync<ReplicaShutdownRefusedException>(log.AppendAsync(conflicting, cancellationToken));

            _ = await Assert.That(log.Readiness).IsEqualTo(FollowerLogReadiness.Failed);
            _ = await Assert.That(logger.Count(TruncateUnconfirmedEventId)).IsEqualTo(1);
        }

        await using var reopened = new FollowerLog(Dir, GroupId, GroupComposition.Create(GroupId), NullLogger<FollowerLog>.Instance, TimeProvider.System);
        await reopened.OpenAsync(cancellationToken);
        var status = await reopened.GetStatusAsync(cancellationToken);

        _ = await Assert.That(status.LastLogIndex).IsEqualTo(2UL);
        _ = await Assert.That(await reopened.GetTermAtAsync(2UL, cancellationToken)).IsEqualTo(1UL);
    }

    /// <summary>A disposal raised by the scheduling hook, which runs where the frames are scheduled, stays a plain disposal: only the refusal of the tracking is a shutdown refusal.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task HookDisposalBeforeSchedulingStaysPlain(CancellationToken cancellationToken)
    {
        var faults = new IFollowerLogFaultHooksCreateExpectations();
        _ = faults.Setups.OnFramesScheduling().Callback(static () => throw new ObjectDisposedException("hook"));
        _ = faults.Setups.OnMetaPublishing();
        await using var log = await OpenLogAsync(
            new FollowerLogOptions { TimeProvider = TimeProvider.System, ShutdownTimeProvider = TimeProvider.System, FaultHooks = faults.Instance() },
            new EventRecordingLogger(),
            cancellationToken);

        _ = await NodeAsyncAssert.ThrowsAsync<ObjectDisposedException>(log.AppendAsync(Append(1UL, "a"), cancellationToken));
    }

    private static void DrainRegistry(FollowerLog log)
    {
        // Closing the ack registry refuses every later durable operation without disposing the log, as the window of a dispose does.
        IFollowerLogDurability durable = log;
        _ = durable.Acks.FaultAll(new ObjectDisposedException(nameof(FollowerLog)));
    }

    private static FollowerLogAppendRequest Append(ulong index, string payload) => new(
        "leader-1",
        1UL,
        index - 1,
        index == 1UL ? 0UL : 1UL,
        0UL,
        ReadOnlyMemory<FollowerLogEntry>.Of(new FollowerLogEntry(index, 1UL, Encoding.UTF8.GetBytes(payload))));

    private async Task AssertReopenedLastIndexAsync(ulong expected, CancellationToken cancellationToken)
    {
        await using var reopened = new FollowerLog(Dir, GroupId, GroupComposition.Create(GroupId), NullLogger<FollowerLog>.Instance, TimeProvider.System);
        await reopened.OpenAsync(cancellationToken);
        var status = await reopened.GetStatusAsync(cancellationToken);

        _ = await Assert.That(status.LastLogIndex).IsEqualTo(expected);
    }

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
}
