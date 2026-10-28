using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.Errors;
using Squirix.Server.Storage.Replication;
using Squirix.Server.TestKit;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;
using static Squirix.Server.UnitTests.Node.Services.ReplicaCommitBatchKit;
using static Squirix.Server.UnitTests.Node.Services.ReplicaCommitterDoubles;

namespace Squirix.Server.UnitTests.Node.Services;

/// <summary>Writes of a group that give up, fail, or meet a full idempotency state while others queue for the same batch.</summary>
public sealed class ReplicaCommitBatchFailureTests : IsolatedStorageTestBase
{
    /// <summary>A caller that gives up while its write is queued leaves the queue: nothing of the write is appended and the other writes commit.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task CanceledQueuedWriteIsNeverAppended(CancellationToken cancellationToken)
    {
        var local = RecoveredCache();
        var gateway = new ParkingGateway();
        gateway.ArmForEntries();
        await using var registry = await OpenRegistryAsync(DefaultOptions(), cancellationToken);
        await using var committer = CreateCommitter(registry, local, gateway, TimeProvider.System);
        var first = SetAsync(committer, "k0", cancellationToken);
        await gateway.Entered.WaitAsync(StallTimeout, TimeProvider.System, cancellationToken);
        using var giveUp = new CancellationTokenSource();
        var canceled = SetAsync(committer, "gone", giveUp.Token);
        var kept = SetAsync(committer, "k1", cancellationToken);
        await giveUp.CancelAsync();

        _ = await NodeAsyncAssert.ThrowsAnyAsync<OperationCanceledException>(canceled.WaitAsync(StallTimeout, TimeProvider.System, cancellationToken));
        gateway.Release();
        await Task.WhenAll(first, kept).WaitAsync(StallTimeout, TimeProvider.System, cancellationToken);

        _ = await Assert.That(Applied(local)).IsEqualTo("k0,k1");
        _ = await Assert.That((await GetOwnedLog(registry).GetStatusAsync(cancellationToken)).LastLogIndex).IsEqualTo(2UL);
    }

    /// <summary>A caller that gives up after a leader took its write does not stop the write: the commit is bounded by its budget only and the caller learns the outcome.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task CancelAfterTheWriteIsTakenDoesNotStopIt(CancellationToken cancellationToken)
    {
        var local = RecoveredCache();
        var gateway = new ParkingGateway();
        gateway.ArmForEntries();
        await using var registry = await OpenRegistryAsync(DefaultOptions(), cancellationToken);
        await using var committer = CreateCommitter(registry, local, gateway, TimeProvider.System);
        using var giveUp = new CancellationTokenSource();
        var leader = SetAsync(committer, "k0", giveUp.Token);
        await gateway.Entered.WaitAsync(StallTimeout, TimeProvider.System, cancellationToken);
        await giveUp.CancelAsync();
        gateway.Release();

        await leader.WaitAsync(StallTimeout, TimeProvider.System, cancellationToken);

        _ = await Assert.That(Applied(local)).IsEqualTo("k0");
    }

    /// <summary>A leader whose caller gives up while it waits for the commit gate hands the leadership to the next write, which then commits.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task LeaderCanceledOnTheGateHandsOver(CancellationToken cancellationToken)
    {
        var local = RecoveredCache();
        await using var registry = await OpenRegistryAsync(DefaultOptions(), cancellationToken);
        await using var committer = CreateCommitter(registry, local, new AcceptingGateway(), TimeProvider.System);
        var held = await committer.Gate.LockAsync(cancellationToken);
        using var giveUp = new CancellationTokenSource();
        var leader = SetAsync(committer, "gone", giveUp.Token);
        var next = SetAsync(committer, "k1", cancellationToken);
        await giveUp.CancelAsync();

        _ = await NodeAsyncAssert.ThrowsAnyAsync<OperationCanceledException>(leader.WaitAsync(StallTimeout, TimeProvider.System, cancellationToken));
        held.Dispose();
        await next.WaitAsync(StallTimeout, TimeProvider.System, cancellationToken);

        _ = await Assert.That(Applied(local)).IsEqualTo("k1");
    }

    /// <summary>A write whose prepare fails is refused alone and takes no log index: the writes around it commit and the log stays dense.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task PrepareFailureRefusesOnlyThatWrite(CancellationToken cancellationToken)
    {
        var local = RecoveredCache();
        var gateway = new ParkingGateway();
        gateway.ArmForEntries();
        await using var registry = await OpenRegistryAsync(DefaultOptions(), cancellationToken);
        await using var committer = CreateCommitter(registry, local, gateway, TimeProvider.System);
        var first = SetAsync(committer, "k0", cancellationToken);
        await gateway.Entered.WaitAsync(StallTimeout, TimeProvider.System, cancellationToken);
        var before = SetAsync(committer, "k1", cancellationToken);
        var failing = PrepareWriteAsync(committer, "bad", static (_, _, _) => ValueTask.FromException<PreparedReplicaMutation>(new InvalidOperationException("Injected prepare failure.")), cancellationToken);
        var after = SetAsync(committer, "k2", cancellationToken);
        gateway.Release();

        await Task.WhenAll(first, before, after).WaitAsync(StallTimeout, TimeProvider.System, cancellationToken);
        var refused = await NodeAsyncAssert.ThrowsAsync<InvalidOperationException>(failing.WaitAsync(StallTimeout, TimeProvider.System, cancellationToken));

        _ = await Assert.That(refused.Message).IsEqualTo("Injected prepare failure.");
        _ = await Assert.That(Applied(local)).IsEqualTo("k0,k1,k2");
        _ = await Assert.That((await GetOwnedLog(registry).GetStatusAsync(cancellationToken)).LastLogIndex).IsEqualTo(3UL);
    }

    /// <summary>The prepare of a write runs on the commit budget, not on the token of its caller, so a caller that gives up during it cannot leave a batch half prepared.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task PrepareRunsOnTheBudgetToken(CancellationToken cancellationToken)
    {
        var local = RecoveredCache();
        await using var registry = await OpenRegistryAsync(DefaultOptions(), cancellationToken);
        await using var committer = CreateCommitter(registry, local, new AcceptingGateway(), TimeProvider.System);
        using var giveUp = new CancellationTokenSource();
        var seen = CancellationToken.None;

        _ = await PrepareWriteAsync(
            committer,
            "k0",
            (factory, index, token) =>
            {
                seen = token;
                return new ValueTask<PreparedReplicaMutation>(factory.PrepareSet(NewOperationId(), Cache, "k0", Entry(), index));
            },
            giveUp.Token).WaitAsync(StallTimeout, TimeProvider.System, cancellationToken);

        _ = await Assert.That(seen.CanBeCanceled).IsTrue();
        _ = await Assert.That(seen).IsNotEqualTo(giveUp.Token);
    }

    /// <summary>
    /// When the idempotency capacity holds only some writes of the wave, the batch ends where it no longer fits: the writes that fit commit and each of
    /// the others is refused on its own, instead of the whole batch being refused.
    /// </summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task CapacityShortfallKeepsTheBatch(CancellationToken cancellationToken)
    {
        var local = RecoveredCache();
        var gateway = new ParkingGateway();
        await using var registry = await OpenRegistryAsync(DefaultOptions(3), cancellationToken);
        await using var committer = CreateCommitter(registry, local, gateway, TimeProvider.System);
        await SetAsync(committer, "k0", cancellationToken).WaitAsync(StallTimeout, TimeProvider.System, cancellationToken);
        gateway.ArmForEntries();
        var parked = SetAsync(committer, "p0", cancellationToken);
        await gateway.Entered.WaitAsync(StallTimeout, TimeProvider.System, cancellationToken);
        var queued = QueueSets(committer, 4, cancellationToken);
        gateway.Release();
        await parked.WaitAsync(StallTimeout, TimeProvider.System, cancellationToken);

        var outcomes = new List<string>();
        foreach (var write in queued)
        {
            try
            {
                await write.WaitAsync(StallTimeout, TimeProvider.System, cancellationToken);
                outcomes.Add("ok");
            }
            catch (SquirixException refused) when (refused.Code == SquirixErrorCode.TooManyRequests)
            {
                outcomes.Add("refused");
            }
        }

        // The capacity holds three operations: k0 and p0 are committed, so one queued write fits and the other three are refused one by one.
        _ = await Assert.That(string.Join(',', outcomes)).IsEqualTo("ok,refused,refused,refused");
        _ = await Assert.That(Applied(local)).IsEqualTo("k0,p0,k1");
    }

    /// <summary>
    /// When the memory apply of a batch fails part-way, the entries applied before it are decided with their outcome and the others as an unknown outcome,
    /// each from the idempotency record of its own operation, instead of all of them as unknown.
    /// </summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task PartialBatchDecidesFromRecords(CancellationToken cancellationToken)
    {
        var local = RecoveredCache(4);
        var gateway = new ParkingGateway();
        await using var registry = await OpenRegistryAsync(DefaultOptions(), cancellationToken);
        await using var committer = CreateCommitter(registry, local, gateway, TimeProvider.System);
        await SetAsync(committer, "k0", cancellationToken).WaitAsync(StallTimeout, TimeProvider.System, cancellationToken);
        gateway.ArmForEntries();
        var parked = SetAsync(committer, "p0", cancellationToken);
        await gateway.Entered.WaitAsync(StallTimeout, TimeProvider.System, cancellationToken);
        var queued = QueueSets(committer, 3, cancellationToken);
        gateway.Release();

        await parked.WaitAsync(StallTimeout, TimeProvider.System, cancellationToken);
        await queued[0].WaitAsync(StallTimeout, TimeProvider.System, cancellationToken);
        var second = await NodeAsyncAssert.ThrowsAsync<SquirixException>(queued[1].WaitAsync(StallTimeout, TimeProvider.System, cancellationToken));
        var third = await NodeAsyncAssert.ThrowsAsync<SquirixException>(queued[2].WaitAsync(StallTimeout, TimeProvider.System, cancellationToken));

        _ = await Assert.That(second.Code).IsEqualTo(SquirixErrorCode.CommitOutcomeUnknown);
        _ = await Assert.That(third.Code).IsEqualTo(SquirixErrorCode.CommitOutcomeUnknown);
        _ = await Assert.That(Applied(local)).IsEqualTo("k0,p0,k1");
    }

    private Task<ReplicaGroupRegistry> OpenRegistryAsync(FollowerLogOptions options, CancellationToken cancellationToken) => ReplicaCommitBatchKit.OpenRegistryAsync(Dir, options, cancellationToken);
}
