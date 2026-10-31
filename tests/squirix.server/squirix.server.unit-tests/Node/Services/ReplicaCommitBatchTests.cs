using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics.Metrics;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.Errors;
using Squirix.Server.Node.Observability;
using Squirix.Server.Node.Services;
using Squirix.Server.Storage.Replication;
using Squirix.Server.TestKit;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;
using static Squirix.Server.UnitTests.Node.Services.ReplicaCommitBatchKit;
using static Squirix.Server.UnitTests.Node.Services.ReplicaCommitterDoubles;

namespace Squirix.Server.UnitTests.Node.Services;

/// <summary>Concurrent writes of one replica group are committed as batches: one append and one majority for the writes that queued while a commit was running.</summary>
public sealed class ReplicaCommitBatchTests : IsolatedStorageTestBase
{
    /// <summary>A write that arrives while the leader is committing waits for the leadership, and the writes that queue behind it are committed by it as one batch.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ConcurrentWritesCommitAsOneBatch(CancellationToken cancellationToken)
    {
        var local = RecoveredCache();
        var gateway = new ParkingGateway();
        gateway.ArmForEntries();
        await using var registry = await OpenRegistryAsync(DefaultOptions(), cancellationToken);
        await using var committer = CreateCommitter(registry, local, gateway, TimeProvider.System);

        var first = SetAsync(committer, "k0", cancellationToken);
        await gateway.Entered.WaitAsync(StallTimeout, TimeProvider.System, cancellationToken);
        var queued = QueueSets(committer, 8, cancellationToken);
        gateway.Release();
        await Task.WhenAll([first, .. queued]).WaitAsync(StallTimeout, TimeProvider.System, cancellationToken);

        _ = await Assert.That(committer.CommitRoundTotals).IsEqualTo((2L, 9L));
        _ = await Assert.That(Applied(local)).IsEqualTo("k0,k1,k2,k3,k4,k5,k6,k7,k8");
        _ = await Assert.That((await GetOwnedLog(registry).GetStatusAsync(cancellationToken)).LastLogIndex).IsEqualTo(9UL);
    }

    /// <summary>A batch is flushed to the log once and its commit index is published once, however many writes it holds.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task BatchFlushesTheLogOnce(CancellationToken cancellationToken)
    {
        var local = RecoveredCache();
        var gateway = new ParkingGateway();
        gateway.ArmForEntries();
        await using var registry = await OpenRegistryAsync(DefaultOptions(), cancellationToken);
        await using var committer = CreateCommitter(registry, local, gateway, TimeProvider.System);
        var first = SetAsync(committer, "k0", cancellationToken);
        await gateway.Entered.WaitAsync(StallTimeout, TimeProvider.System, cancellationToken);
        var queued = QueueSets(committer, 8, cancellationToken);
        var flushesBefore = GetOwnedLog(registry).FlushCount;
        gateway.Release();
        await Task.WhenAll([first, .. queued]).WaitAsync(StallTimeout, TimeProvider.System, cancellationToken);

        // The first write is flushed and published on its own, the eight queued ones together: two flushes each.
        _ = await Assert.That(GetOwnedLog(registry).FlushCount - flushesBefore).IsLessThanOrEqualTo(4L);
        _ = await Assert.That(committer.CommitRoundTotals.Rounds).IsEqualTo(2L);
    }

    /// <summary>A write alone commits as a round of one entry, as before batching.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task LoneWriteCommitsAlone(CancellationToken cancellationToken)
    {
        await using var registry = await OpenRegistryAsync(DefaultOptions(), cancellationToken);
        await using var committer = CreateCommitter(registry, RecoveredCache(), new AcceptingGateway(), TimeProvider.System);

        await SetAsync(committer, "k0", cancellationToken).WaitAsync(StallTimeout, TimeProvider.System, cancellationToken);
        await SetAsync(committer, "k1", cancellationToken).WaitAsync(StallTimeout, TimeProvider.System, cancellationToken);

        _ = await Assert.That(committer.CommitRoundTotals).IsEqualTo((2L, 2L));
    }

    /// <summary>A batch holds at most the entries one follower acknowledgement buffer takes; the writes beyond it are the next batch.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task BatchEndsAtTheEntryLimit(CancellationToken cancellationToken)
    {
        var local = RecoveredCache();
        var gateway = new ParkingGateway();
        gateway.ArmForEntries();
        await using var registry = await OpenRegistryAsync(DefaultOptions(), cancellationToken);
        await using var committer = CreateCommitter(registry, local, gateway, TimeProvider.System);
        var first = SetAsync(committer, "k0", cancellationToken);
        await gateway.Entered.WaitAsync(StallTimeout, TimeProvider.System, cancellationToken);
        var queued = QueueSets(committer, ReplicaGroupCommitterBatches.MaxBatchEntries + 7, cancellationToken);
        gateway.Release();
        await Task.WhenAll([first, .. queued]).WaitAsync(StallTimeout, TimeProvider.System, cancellationToken);

        _ = await Assert.That(committer.CommitRoundTotals).IsEqualTo((3L, long.CreateChecked(ReplicaGroupCommitterBatches.MaxBatchEntries) + 8L));
    }

    /// <summary>A write of a key the batch already holds ends the batch and commits in the next one, so it decides from the effect of the earlier write.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task RepeatedKeyEndsTheBatch(CancellationToken cancellationToken)
    {
        var local = RecoveredCache();
        var gateway = new ParkingGateway();
        gateway.ArmForEntries();
        await using var registry = await OpenRegistryAsync(DefaultOptions(), cancellationToken);
        await using var committer = CreateCommitter(registry, local, gateway, TimeProvider.System);
        var first = SetAsync(committer, "k0", cancellationToken);
        await gateway.Entered.WaitAsync(StallTimeout, TimeProvider.System, cancellationToken);
        var queued = new[] { SetAsync(committer, "k1", cancellationToken), SetAsync(committer, "k2", cancellationToken), SetAsync(committer, "k1", cancellationToken), SetAsync(committer, "k3", cancellationToken) };
        gateway.Release();
        await Task.WhenAll([first, .. queued]).WaitAsync(StallTimeout, TimeProvider.System, cancellationToken);

        // k1 and k2 together, then the repeated k1 with k3 behind it.
        _ = await Assert.That(committer.CommitRoundTotals).IsEqualTo((3L, 5L));
        _ = await Assert.That(Applied(local)).IsEqualTo("k0,k1,k2,k1,k3");
    }

    /// <summary>
    /// A write that repeats the operation identity of another write of the wave, under another key, does not fail the batch: it is committed alone
    /// after it and refused as an identifier reused with another request, while the other writes commit.
    /// </summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task RepeatedIdentityDoesNotFailTheBatch(CancellationToken cancellationToken)
    {
        var local = RecoveredCache();
        var gateway = new ParkingGateway();
        gateway.ArmForEntries();
        await using var registry = await OpenRegistryAsync(DefaultOptions(), cancellationToken);
        await using var committer = CreateCommitter(registry, local, gateway, TimeProvider.System);
        var first = SetAsync(committer, "k0", cancellationToken);
        await gateway.Entered.WaitAsync(StallTimeout, TimeProvider.System, cancellationToken);
        var shared = NewOperationId();
        var firstUse = SetAsync(committer, "k1", shared, cancellationToken);
        var reuse = SetAsync(committer, "k2", shared, cancellationToken);
        var after = SetAsync(committer, "k3", cancellationToken);
        gateway.Release();

        await Task.WhenAll(first, firstUse, after).WaitAsync(StallTimeout, TimeProvider.System, cancellationToken);
        _ = await NodeAsyncAssert.ThrowsAsync<ServerOpIdMismatchException>(reuse.WaitAsync(StallTimeout, TimeProvider.System, cancellationToken));

        _ = await Assert.That(Applied(local)).IsEqualTo("k0,k1,k3");
        _ = await Assert.That((await GetOwnedLog(registry).GetStatusAsync(cancellationToken)).LastLogIndex).IsEqualTo(3UL);
    }

    /// <summary>A retry of an operation the group retains in the wave is answered from its record and appends nothing, and the writes around it commit.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task RetainedOperationReplaysInTheWave(CancellationToken cancellationToken)
    {
        var local = RecoveredCache();
        var gateway = new ParkingGateway();
        await using var registry = await OpenRegistryAsync(DefaultOptions(), cancellationToken);
        await using var committer = CreateCommitter(registry, local, gateway, TimeProvider.System);
        var committed = NewOperationId();
        await SetAsync(committer, "k0", committed, cancellationToken).WaitAsync(StallTimeout, TimeProvider.System, cancellationToken);

        gateway.ArmForEntries();
        var parked = SetAsync(committer, "k1", cancellationToken);
        await gateway.Entered.WaitAsync(StallTimeout, TimeProvider.System, cancellationToken);
        var before = SetAsync(committer, "k2", cancellationToken);
        var retry = SetAsync(committer, "k0", committed, cancellationToken);
        var after = SetAsync(committer, "k3", cancellationToken);
        gateway.Release();
        await Task.WhenAll(parked, before, retry, after).WaitAsync(StallTimeout, TimeProvider.System, cancellationToken);

        _ = await Assert.That(Applied(local)).IsEqualTo("k0,k1,k2,k3");
        _ = await Assert.That((await GetOwnedLog(registry).GetStatusAsync(cancellationToken)).LastLogIndex).IsEqualTo(4UL);
    }

    /// <summary>Each commit round records the number of writes it carried: a write that commits alone records one, a batch records its size.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task RoundsRecordTheirSize(CancellationToken cancellationToken)
    {
        using var meter = new Meter("squirix-batch-size");
        var sizes = new ConcurrentQueue<long>();
        using var listener = new MeterListener();
        listener.InstrumentPublished = (instrument, target) =>
        {
            if (ReferenceEquals(instrument.Meter, meter))
                target.EnableMeasurementEvents(instrument);
        };
        listener.SetMeasurementEventCallback<long>((_, value, _, _) => sizes.Enqueue(value));
        listener.Start();
        var gateway = new ParkingGateway();
        gateway.ArmForEntries();
        await using var registry = await OpenRegistryAsync(DefaultOptions(), cancellationToken);
        await using var committer = CreateCommitter(registry, RecoveredCache(), gateway, (TimeProvider.System, new ReplicationMetrics(meter)));
        var first = SetAsync(committer, "k0", cancellationToken);
        await gateway.Entered.WaitAsync(StallTimeout, TimeProvider.System, cancellationToken);
        var queued = QueueSets(committer, 3, cancellationToken);
        gateway.Release();

        await Task.WhenAll([first, .. queued]).WaitAsync(StallTimeout, TimeProvider.System, cancellationToken);

        var seen = new List<string>();
        foreach (var size in sizes)
            seen.Add(size.ToString(System.Globalization.CultureInfo.InvariantCulture));

        _ = await Assert.That(string.Join(',', seen)).IsEqualTo("1,3");
    }

    private Task<ReplicaGroupRegistry> OpenRegistryAsync(FollowerLogOptions options, CancellationToken cancellationToken) => ReplicaCommitBatchKit.OpenRegistryAsync(Dir, options, cancellationToken);
}
