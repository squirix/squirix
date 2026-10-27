using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging.Abstractions;
using Squirix.Server.Cluster.Replication;
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
/// A leader whose own group log keeps failing the append refuses every write but keeps its authority, so nothing but a deadline can hand
/// the group to a member that can append. Node n1 leads group n2 from slot 2; its followers are n2 and n3.
/// </summary>
public sealed class LeaderAppendHoldTests : ServerUnitTestBase
{
    private const string CacheName = "cache";
    private const ulong FailedWrites = 3UL;

    /// <summary>
    /// Three writes that each fail the group-log append leave the leader with its authority, no unapplied entry, a read that still
    /// completes, and a log without a partial or skipped entry.
    /// </summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    /// <exception cref="InvalidOperationException">The group log is not open.</exception>
    [Test]
    public async Task FailingAppendLeavesNoGap(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-append-hold");
        var hooks = new FailingFrameWriteHooks();
        await using var registry = await OpenQuorumReadRegistryAsync(dir, hooks, cancellationToken);
        if (!registry.TryGetLog("n2", out var log))
            throw new InvalidOperationException("The group log n2 is not open.");

        _ = await log.ObserveTermAsync(2UL, cancellationToken);
        var cache = new StubCache();
        await using var committer = CreateElectedCommitter(registry, "n2", new ScriptedGateway(), cache, new ReplicaGroupApplier(cache, NullLogger.Instance, "n2", "n1"));
        _ = await Assert.That(await committer.PromoteAsync(2UL, cancellationToken)).IsTrue();
        var state = registry.StateFor("n2");
        state.SetElectionDriven(true);
        _ = state.BecomeLeader(2UL);
        _ = state.GrantAuthority(2UL);
        var before = (await log.GetStatusAsync(cancellationToken)).LastLogIndex;

        hooks.Arm();
        for (var i = 0UL; i < FailedWrites; i++)
            _ = await NodeAsyncAssert.ThrowsAnyAsync<Exception>(committer.CommitSetAsync(NewOperationId(), CacheName, "b", Entry("b"), cancellationToken));

        hooks.Disarm();
        await committer.ConfirmReadAsync(cancellationToken);

        _ = await Assert.That(committer.AppendProgress().Failed).IsEqualTo(FailedWrites);
        _ = await Assert.That(state.HasAuthority).IsTrue();
        _ = await Assert.That(committer.ApplyProgress().IsBehind).IsFalse();
        _ = await Assert.That((await log.GetStatusAsync(cancellationToken)).LastLogIndex).IsEqualTo(before);
    }

    private static async Task<ReplicaGroupRegistry> OpenQuorumReadRegistryAsync(string dir, IFollowerLogFaultHooks hooks, CancellationToken cancellationToken)
    {
        await SeededLogs.SeedGroupsAsync(dir, Groups, cancellationToken);
        var options = new FollowerLogOptions { ShutdownTimeProvider = TimeProvider.System, TimeProvider = TimeProvider.System, FaultHooks = hooks };
        var registry = new ReplicaGroupRegistry(dir, Groups, 3, Fingerprint, 1, NullLoggerFactory.Instance, options) { ElectionClock = TimeProvider.System, QuorumReads = true };
        try
        {
            await registry.OpenAsync(cancellationToken);
        }
        catch
        {
            await registry.DisposeAsync();
            throw;
        }

        return registry;
    }
}
