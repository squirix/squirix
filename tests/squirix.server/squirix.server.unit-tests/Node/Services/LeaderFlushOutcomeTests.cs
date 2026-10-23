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
using Squirix.Server.TestKit.Replication;
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
    private const ulong RestartTerm = 3UL;
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

    /// <summary>A retry of an unknown write with the same operation identifier is answered as unknown again and is not executed a second time.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task RetryOfUnknownWriteIsNotReExecuted(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-flush-retry");
        var hooks = new FailingFrameWriteHooks();
        var cache = new StubCache();
        await using var registry = await OpenRegistryAsync(dir, Groups, Options(hooks), cancellationToken);
        await using var committer = await LeadAsync(registry, cache, LeaderTerm, cancellationToken);
        var operationId = NewOperationId();
        hooks.Arm();
        _ = await NodeAsyncAssert.ThrowsAsync<SquirixException>(committer.CommitSetAsync(operationId, CacheName, "b", Entry("b"), cancellationToken));
        hooks.Disarm();
        var lengthAtFailure = LogFileLength(dir);

        var retry = await NodeAsyncAssert.ThrowsAsync<SquirixException>(committer.CommitSetAsync(operationId, CacheName, "b", Entry("b"), cancellationToken));

        _ = await Assert.That(retry.Code).IsEqualTo(SquirixErrorCode.CommitOutcomeUnknown);
        _ = await Assert.That(hooks.Failures).IsEqualTo(1);
        _ = await Assert.That(LogFileLength(dir)).IsEqualTo(lengthAtFailure);
        _ = await Assert.That(cache.Applied.Count).IsEqualTo(0);
    }

    /// <summary>
    /// After a restart the frame of an unknown write is a tail that the next leadership commits; the retry with the same operation identifier
    /// is answered from the committed entry and is applied once.
    /// </summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task UnknownWriteCommitsAfterRestart(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-flush-commit");
        var hooks = new FailingFrameWriteHooks();
        var operationId = NewOperationId();
        await using (var registry = await OpenRegistryAsync(dir, Groups, Options(hooks), cancellationToken))
        {
            await using var committer = await LeadAsync(registry, cancellationToken);
            hooks.Arm();
            _ = await NodeAsyncAssert.ThrowsAsync<SquirixException>(committer.CommitSetAsync(operationId, CacheName, "b", Entry("b"), cancellationToken));
        }

        var cache = new StubCache();
        await using var reopened = await OpenRegistryAsync(dir, Groups, Options(new FailingFrameWriteHooks()), cancellationToken);
        await using var restarted = await LeadAsync(reopened, cache, RestartTerm, cancellationToken);

        await restarted.CommitSetAsync(operationId, CacheName, "b", Entry("b"), cancellationToken);

        await SequenceAssert.EqualAsync<string>(["b"], cache.Applied.ToArray(), StringComparer.Ordinal);
    }

    /// <summary>
    /// After a restart that lost the frame of an unknown write, the retry with the same operation identifier runs as a new write and is
    /// applied once.
    /// </summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task LostUnknownWriteRunsOnceAfterRestart(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-flush-lost");
        var hooks = new FailingFrameWriteHooks();
        var operationId = NewOperationId();
        long lengthBefore;
        await using (var registry = await OpenRegistryAsync(dir, Groups, Options(hooks), cancellationToken))
        {
            await using var committer = await LeadAsync(registry, cancellationToken);
            lengthBefore = LogFileLength(dir);
            hooks.Arm();
            _ = await NodeAsyncAssert.ThrowsAsync<SquirixException>(committer.CommitSetAsync(operationId, CacheName, "b", Entry("b"), cancellationToken));
        }

        TruncateLog(dir, lengthBefore);
        var cache = new StubCache();
        await using var reopened = await OpenRegistryAsync(dir, Groups, Options(new FailingFrameWriteHooks()), cancellationToken);
        await using var restarted = await LeadAsync(reopened, cache, RestartTerm, cancellationToken);

        await restarted.CommitSetAsync(operationId, CacheName, "b", Entry("b"), cancellationToken);

        await SequenceAssert.EqualAsync<string>(["b"], cache.Applied.ToArray(), StringComparer.Ordinal);
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

    private static Task<ReplicaGroupCommitter> LeadAsync(ReplicaGroupRegistry registry, CancellationToken cancellationToken) =>
        LeadAsync(registry, new StubCache(), LeaderTerm, cancellationToken);

    private static async Task<ReplicaGroupCommitter> LeadAsync(ReplicaGroupRegistry registry, StubCache cache, ulong term, CancellationToken cancellationToken)
    {
        if (!registry.TryGetLog(GroupId, out var log))
            throw new InvalidOperationException("The group log n2 is not open.");

        _ = await log.ObserveTermAsync(term, cancellationToken);
        var committer = CreateElectedBudgetCommitter(
            registry,
            GroupId,
            new ScriptedGateway(),
            (cache, new ReplicaGroupApplier(cache, NullLogger.Instance, GroupId, "n1")),
            TimeProvider.System);
        _ = await committer.PromoteAsync(term, cancellationToken);
        var state = registry.StateFor(GroupId);
        state.SetElectionDriven(true);
        _ = state.BecomeLeader(term);
        _ = state.GrantAuthority(term);
        return committer;
    }

    private static void TruncateLog(string dir, long length)
    {
        using var handle = File.OpenHandle(GroupLogFile.ActivePath(dir, GroupId), FileMode.Open, FileAccess.Write, FileShare.None);
        RandomAccess.SetLength(handle, length);
    }

    private static FollowerLogOptions Options(IFollowerLogFaultHooks hooks) =>
        new() { ShutdownTimeProvider = TimeProvider.System, TimeProvider = TimeProvider.System, FaultHooks = hooks };
}
