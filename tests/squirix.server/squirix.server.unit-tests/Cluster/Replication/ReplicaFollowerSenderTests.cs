using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;
using static Squirix.Server.UnitTests.Cluster.Replication.ReplicaSenderTestKit;

namespace Squirix.Server.UnitTests.Cluster.Replication;

/// <summary>Ordering and batching of the appends one <see cref="ReplicaFollowerSender" /> sends to its follower.</summary>
[Immutable]
public sealed class ReplicaFollowerSenderTests : ServerUnitTestBase
{
    /// <summary>A batch ends at the first entry that does not continue the one before it, so a request never spans a gap.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task BatchCutsAtGap(CancellationToken cancellationToken)
    {
        var gateway = new ParkingFollowerGateway();
        var sender = CreateSender(gateway);
        try
        {
            _ = EnqueueAsync(sender, 1);
            var first = await BoundedAsync(gateway.CallAsync(0), cancellationToken);
            _ = EnqueueAsync(sender, 2);
            _ = EnqueueAsync(sender, 4, prevIndex: 3);
            first.Accept();

            var second = await BoundedAsync(gateway.CallAsync(1), cancellationToken);
            second.Accept();
            var third = await BoundedAsync(gateway.CallAsync(2), cancellationToken);

            _ = await Assert.That(second.Count).IsEqualTo(1);
            _ = await Assert.That(third.FirstIndex).IsEqualTo(4UL);
            _ = await Assert.That(third.PrevLogIndex).IsEqualTo(3UL);
        }
        finally
        {
            gateway.ReleaseAll();
            await sender.DisposeAsync();
        }
    }

    /// <summary>A batch ends where the term changes.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task BatchCutsAtTermChange(CancellationToken cancellationToken)
    {
        var gateway = new ParkingFollowerGateway();
        var sender = CreateSender(gateway);
        try
        {
            _ = EnqueueAsync(sender, 1);
            var first = await BoundedAsync(gateway.CallAsync(0), cancellationToken);
            _ = EnqueueAsync(sender, 2);
            _ = EnqueueAsync(sender, 3, 2, prevTerm: 1);
            first.Accept();

            var second = await BoundedAsync(gateway.CallAsync(1), cancellationToken);
            second.Accept();
            var third = await BoundedAsync(gateway.CallAsync(2), cancellationToken);

            _ = await Assert.That(second.Count).IsEqualTo(1);
            _ = await Assert.That(third.FirstIndex).IsEqualTo(3UL);
        }
        finally
        {
            gateway.ReleaseAll();
            await sender.DisposeAsync();
        }
    }

    /// <summary>A request carries at most the configured number of entries; the rest follow in later requests, in order.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task EntryCapSplitsBatches(CancellationToken cancellationToken)
    {
        var capped = new ParkingFollowerGateway();
        var small = new ReplicaFollowerSender(capped, "n2", in Header, 0, 0, HangGuard) { MaxBatchEntries = 2 };
        try
        {
            _ = EnqueueAsync(small, 1);
            var head = await BoundedAsync(capped.CallAsync(0), cancellationToken);
            for (ulong index = 2; index <= 6; index++)
                _ = EnqueueAsync(small, index);

            head.Accept();
            var second = await BoundedAsync(capped.CallAsync(1), cancellationToken);
            second.Accept();
            var third = await BoundedAsync(capped.CallAsync(2), cancellationToken);
            third.Accept();
            var fourth = await BoundedAsync(capped.CallAsync(3), cancellationToken);

            _ = await Assert.That(second.Count).IsEqualTo(2);
            _ = await Assert.That(third.FirstIndex).IsEqualTo(4UL);
            _ = await Assert.That(third.Count).IsEqualTo(2);
            _ = await Assert.That(fourth.FirstIndex).IsEqualTo(6UL);
            _ = await Assert.That(fourth.Count).IsEqualTo(1);
        }
        finally
        {
            capped.ReleaseAll();
            await small.DisposeAsync();
        }
    }

    /// <summary>A request stops before the entry that would take its canonical payload past the byte cap.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ByteCapSplitsBatches(CancellationToken cancellationToken)
    {
        var gateway = new ParkingFollowerGateway();
        var sender = new ReplicaFollowerSender(gateway, "n2", in Header, 0, 0, HangGuard) { MaxBatchBytes = 10 };
        try
        {
            _ = EnqueueAsync(sender, 1, payloadBytes: 4);
            var head = await BoundedAsync(gateway.CallAsync(0), cancellationToken);
            for (ulong index = 2; index <= 5; index++)
                _ = EnqueueAsync(sender, index, payloadBytes: 4);

            head.Accept();
            var second = await BoundedAsync(gateway.CallAsync(1), cancellationToken);
            second.Accept();
            var third = await BoundedAsync(gateway.CallAsync(2), cancellationToken);

            _ = await Assert.That(second.FirstIndex).IsEqualTo(2UL);
            _ = await Assert.That(second.Count).IsEqualTo(2);
            _ = await Assert.That(third.FirstIndex).IsEqualTo(4UL);
            _ = await Assert.That(third.Count).IsEqualTo(2);
        }
        finally
        {
            gateway.ReleaseAll();
            await sender.DisposeAsync();
        }
    }

    /// <summary>An entry enqueued while the previous request is unanswered is not sent before it is.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task NextAppendWaitsForPrevious(CancellationToken cancellationToken)
    {
        var gateway = new ParkingFollowerGateway();
        var sender = CreateSender(gateway);
        try
        {
            var first = EnqueueAsync(sender, 1);
            var second = EnqueueAsync(sender, 2);

            _ = await Assert.That(gateway.CallCount).IsEqualTo(1);
            _ = await Assert.That(second.IsCompleted).IsFalse();

            (await BoundedAsync(gateway.CallAsync(0), cancellationToken)).Accept();
            var request = await BoundedAsync(gateway.CallAsync(1), cancellationToken);
            request.Accept();

            _ = await Assert.That((await BoundedAsync(first, cancellationToken)).LogIndex).IsEqualTo(1UL);
            _ = await Assert.That((await BoundedAsync(second, cancellationToken)).LogIndex).IsEqualTo(2UL);
            _ = await Assert.That(request.PrevLogIndex).IsEqualTo(1UL);
        }
        finally
        {
            gateway.ReleaseAll();
            await sender.DisposeAsync();
        }
    }

    /// <summary>Entries that pile up behind an unanswered request go out together, naming the entry before them and the commit index of the last one.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task PendingEntriesGoInOneBatch(CancellationToken cancellationToken)
    {
        var gateway = new ParkingFollowerGateway();
        var sender = CreateSender(gateway);
        try
        {
            _ = EnqueueAsync(sender, 1);
            var head = await BoundedAsync(gateway.CallAsync(0), cancellationToken);
            var acknowledgements = new[] { EnqueueAsync(sender, 2), EnqueueAsync(sender, 3), EnqueueAsync(sender, 4) };

            head.Accept();
            var batch = await BoundedAsync(gateway.CallAsync(1), cancellationToken);
            batch.Accept();
            var results = await BoundedAsync(Task.WhenAll(acknowledgements), cancellationToken);

            _ = await Assert.That(batch.Count).IsEqualTo(3);
            _ = await Assert.That(batch.PrevLogIndex).IsEqualTo(1UL);
            _ = await Assert.That(batch.FirstIndex).IsEqualTo(2UL);
            _ = await Assert.That(batch.CommitIndex).IsEqualTo(3UL);
            _ = await Assert.That(batch.NodeId).IsEqualTo("n2");
            _ = await Assert.That(results[2].LogIndex).IsEqualTo(4UL);
            _ = await Assert.That(gateway.CallCount).IsEqualTo(2);
        }
        finally
        {
            gateway.ReleaseAll();
            await sender.DisposeAsync();
        }
    }
}
