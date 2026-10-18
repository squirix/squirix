using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging.Abstractions;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.Errors;
using Squirix.Server.Node.Services;
using Squirix.Server.Storage.Replication;
using Squirix.Server.TestKit;
using Squirix.Server.TestKit.IO;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;
using static Squirix.Server.UnitTests.Node.Services.LedGroupsTestKit;
using static Squirix.Server.UnitTests.Node.Services.ReplicaOwnerTestKit;

namespace Squirix.Server.UnitTests.Node.Services;

/// <summary>
/// A leader write whose own group-log append fails after the frame was written is reported as an unknown commit outcome, and the log
/// latches, because a restart recovers the frame as an uncommitted tail that a majority can commit. Node n1 leads group n2 from slot 2;
/// its followers are n2 and n3.
/// </summary>
public sealed class LeaderFlushOutcomeTests : ServerUnitTestBase
{
    private const string CacheName = "cache";
    private const string GroupId = "n2";
    private const ulong LeaderTerm = 2UL;
    private const ulong PromotionIndex = 1UL;
    private const ulong WriteIndex = 2UL;
    private const int ReadLimit = 8;

    /// <summary>
    /// A failure after the frame was written is an unknown outcome, not a refusal, while the frame is already in the log file; the log is
    /// latched.
    /// </summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    /// <exception cref="InvalidOperationException">The group log is not open.</exception>
    [Test]
    public async Task FrameWriteFailureReportsOutcomeUnknown(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-flush-outcome");
        var hooks = new FailingFrameWriteHooks();
        long lengthBefore;
        await using (var registry = await OpenRegistryAsync(dir, Groups, Options(hooks), cancellationToken))
        {
            await using var committer = await LeadAsync(registry, cancellationToken);
            lengthBefore = LogFileLength(dir);

            hooks.Arm();
            var failure = await NodeAsyncAssert.ThrowsAsync<SquirixException>(committer.CommitSetAsync(NewOperationId(), CacheName, "b", Entry("b"), cancellationToken));

            if (!registry.TryGetLog(GroupId, out var log))
                throw new InvalidOperationException("The group log n2 is not open.");

            _ = await Assert.That(failure.Code).IsEqualTo(SquirixErrorCode.CommitOutcomeUnknown);
            _ = await Assert.That(log.Readiness).IsEqualTo(FollowerLogReadiness.Failed);
        }

        _ = await Assert.That(LogFileLength(dir)).IsGreaterThan(lengthBefore);
    }

    /// <summary>A failure of the flush that follows the written frame is an unknown outcome as well.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task FlushFailureReportsOutcomeUnknown(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-flush-failure");
        var hooks = new FailingFrameWriteHooks();
        await using var registry = await OpenRegistryAsync(dir, Groups, Options(hooks), cancellationToken);
        await using var committer = await LeadAsync(registry, cancellationToken);

        hooks.ArmFlush();
        var failure = await NodeAsyncAssert.ThrowsAsync<SquirixException>(committer.CommitSetAsync(NewOperationId(), CacheName, "b", Entry("b"), cancellationToken));

        _ = await Assert.That(failure.Code).IsEqualTo(SquirixErrorCode.CommitOutcomeUnknown);
        _ = await Assert.That(hooks.Failures).IsEqualTo(1);
    }

    /// <summary>After a restart without another append, the unknown write is an uncommitted tail of the log, and a commit index covers it.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    /// <exception cref="InvalidOperationException">The group log is not open.</exception>
    [Test]
    public async Task UnknownWriteSurvivesRestart(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-flush-restart");
        var hooks = new FailingFrameWriteHooks();
        await using (var registry = await OpenRegistryAsync(dir, Groups, Options(hooks), cancellationToken))
        {
            await using var committer = await LeadAsync(registry, cancellationToken);
            hooks.Arm();
            _ = await NodeAsyncAssert.ThrowsAsync<SquirixException>(committer.CommitSetAsync(NewOperationId(), CacheName, "b", Entry("b"), cancellationToken));
        }

        await using var reopened = await OpenRegistryAsync(dir, Groups, Options(new FailingFrameWriteHooks()), cancellationToken);
        if (!reopened.TryGetLog(GroupId, out var log))
            throw new InvalidOperationException("The group log n2 is not open.");

        var status = await log.GetStatusAsync(cancellationToken);
        var tail = await log.GetUncommittedTailAsync(cancellationToken);
        var committed = await log.AdvanceCommitAsync(WriteIndex, cancellationToken);
        var after = await log.GetStatusAsync(cancellationToken);

        _ = await Assert.That(status.LastLogIndex).IsEqualTo(WriteIndex);
        _ = await Assert.That(status.CommitIndex).IsEqualTo(PromotionIndex);
        _ = await Assert.That(tail.Count).IsEqualTo(1);
        _ = await Assert.That(committed.Success).IsTrue();
        _ = await Assert.That(after.CommitIndex).IsEqualTo(WriteIndex);
        _ = await Assert.That(await HoldsAsync(reopened, cancellationToken)).IsEqualTo(1);
    }

    private static long LogFileLength(string dir)
    {
        var groupDir = GroupStoragePaths.GetGroupDirectory(dir, GroupId);
        long length = 0;
        foreach (var file in Directory.EnumerateFiles(groupDir))
        {
            if (Path.GetFileName(file).StartsWith("group.log", StringComparison.OrdinalIgnoreCase))
                length += new FileInfo(file).Length;
        }

        return length;
    }

    private static async Task<int> HoldsAsync(ReplicaGroupRegistry registry, CancellationToken cancellationToken)
    {
        if (!registry.TryGetLog(GroupId, out var log))
            throw new InvalidOperationException("The group log n2 is not open.");

        var read = await log.ReadEntriesAsync(WriteIndex, ReadLimit, cancellationToken);
        return read.Entries.Count;
    }

    private static async Task<ReplicaGroupCommitter> LeadAsync(ReplicaGroupRegistry registry, CancellationToken cancellationToken)
    {
        if (!registry.TryGetLog(GroupId, out var log))
            throw new InvalidOperationException("The group log n2 is not open.");

        _ = await log.ObserveTermAsync(LeaderTerm, cancellationToken);
        var cache = new StubCache();
        var committer = CreateElectedBudgetCommitter(
            registry,
            GroupId,
            new ScriptedGateway(),
            (cache, new ReplicaGroupApplier(cache, NullLogger.Instance, GroupId, "n1")),
            TimeProvider.System);
        _ = await committer.PromoteAsync(LeaderTerm, cancellationToken);
        var state = registry.StateFor(GroupId);
        state.SetElectionDriven(true);
        _ = state.BecomeLeader(LeaderTerm);
        _ = state.GrantAuthority(LeaderTerm);
        return committer;
    }

    private static FollowerLogOptions Options(IFollowerLogFaultHooks hooks) =>
        new() { ShutdownTimeProvider = TimeProvider.System, TimeProvider = TimeProvider.System, FaultHooks = hooks };
}
