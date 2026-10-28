using System;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.Node.Services;
using Squirix.Server.TestKit;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Node.Services;

/// <summary>The leadership and the cancellation rules of the writes that wait for a commit batch.</summary>
public sealed class ReplicaCommitQueueTests : ServerUnitTestBase
{
    private static readonly TimeSpan StallTimeout = TimeSpan.FromSeconds(10);

    /// <summary>The first write leads and is not queued; the next ones queue behind it.</summary>
    [Test]
    public async Task FirstWriteLeadsAndOthersQueue()
    {
        var queue = new ReplicaCommitQueue();

        var first = queue.Enter(Request("a"));
        var second = queue.Enter(Request("b"));

        _ = await Assert.That(first).IsTrue();
        _ = await Assert.That(second).IsFalse();
        _ = await Assert.That(queue.PeekLive()).IsNotNull();
    }

    /// <summary>Handing over promotes the first queued write that is not canceled, and its caller learns it leads.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task HandOverPromotesFirstLiveWrite(CancellationToken cancellationToken)
    {
        var queue = new ReplicaCommitQueue();
        _ = queue.Enter(Request("a"));
        var canceled = Request("b");
        var live = Request("c");
        _ = queue.Enter(canceled);
        _ = queue.Enter(live);
        _ = canceled.TryCancel();

        queue.HandOver();

        _ = await Assert.That(await live.AwaitLeadershipAsync(cancellationToken)).IsTrue();
        _ = await Assert.That(queue.PeekLive()).IsNull();
    }

    /// <summary>Handing over with nothing queued ends the leadership, so the next write leads.</summary>
    [Test]
    public async Task HandOverWithoutQueueEndsLeadership()
    {
        var queue = new ReplicaCommitQueue();
        _ = queue.Enter(Request("a"));

        queue.HandOver();

        _ = await Assert.That(queue.Enter(Request("b"))).IsTrue();
    }

    /// <summary>A write is taken or canceled, never both: the first move from the queue wins.</summary>
    [Test]
    public async Task TakeAndCancelExcludeEachOther()
    {
        var taken = Request("a");
        var canceled = Request("b");

        var takenWins = taken.TryTake();
        var cancelAfterTake = taken.TryCancel();
        var canceledWins = canceled.TryCancel();
        var takeAfterCancel = canceled.TryTake();

        _ = await Assert.That(takenWins).IsTrue();
        _ = await Assert.That(cancelAfterTake).IsFalse();
        _ = await Assert.That(canceledWins).IsTrue();
        _ = await Assert.That(takeAfterCancel).IsFalse();
    }

    /// <summary>Removing the head removes only the request the leader took: another head, or an empty queue, is left as it is.</summary>
    [Test]
    public async Task RemoveHeadRemovesOnlyTheExpectedRequest()
    {
        var queue = new ReplicaCommitQueue();
        var head = Request("b");
        var other = Request("c");
        _ = queue.Enter(Request("a"));
        _ = queue.Enter(head);
        _ = queue.Enter(other);

        queue.RemoveHead(other);
        var keptHead = queue.PeekLive();
        queue.RemoveHead(head);
        var next = queue.PeekLive();
        queue.RemoveHead(other);
        queue.RemoveHead(other);

        _ = await Assert.That(keptHead).IsSameReferenceAs(head);
        _ = await Assert.That(next).IsSameReferenceAs(other);
        _ = await Assert.That(queue.PeekLive()).IsNull();
    }

    /// <summary>A request a leader took is not returned by the disposal that takes the queue, and the leader may neither hand it back nor remove it.</summary>
    [Test]
    public async Task TakenRequestStaysWithItsLeader()
    {
        var queue = new ReplicaCommitQueue();
        var took = Request("b");
        var waiting = Request("c");
        _ = queue.Enter(Request("a"));
        _ = queue.Enter(took);
        _ = queue.Enter(waiting);
        _ = took.TryTake();

        var all = queue.TakeAll();
        var restored = queue.Restore(took);
        queue.RemoveHead(took);

        _ = await Assert.That(all.Count).IsEqualTo(1);
        _ = await Assert.That(all[0]).IsSameReferenceAs(waiting);
        _ = await Assert.That(restored).IsFalse();
        _ = await Assert.That(queue.Enter(Request("d"))).IsTrue();
    }

    /// <summary>A request handed back to the head of the queue is queued again, so the next batch or a disposal decides it.</summary>
    [Test]
    public async Task RestoredRequestIsQueuedAgain()
    {
        var queue = new ReplicaCommitQueue();
        var back = Request("b");
        _ = queue.Enter(Request("a"));
        _ = queue.Enter(back);
        _ = back.TryTake();

        var restored = queue.Restore(back);
        var all = queue.TakeAll();

        _ = await Assert.That(restored).IsTrue();
        _ = await Assert.That(all.Count).IsEqualTo(1);
        _ = await Assert.That(all[0]).IsSameReferenceAs(back);
    }

    /// <summary>A caller that gives up while its write is queued leaves the queue with a cancellation.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task QueuedWriteCancelsWithItsCaller(CancellationToken cancellationToken)
    {
        var request = Request("a");
        using var giveUp = new CancellationTokenSource();
        var waiting = request.AwaitLeadershipAsync(giveUp.Token);

        await giveUp.CancelAsync();

        _ = await NodeAsyncAssert.ThrowsAnyAsync<OperationCanceledException>(waiting.WaitAsync(StallTimeout, TimeProvider.System, cancellationToken));
        _ = await Assert.That(request.IsCanceled()).IsTrue();
    }

    /// <summary>A caller that gives up after a leader took its write keeps waiting for the leader's decision instead of leaving.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task TakenWriteIgnoresItsCallerCancellation(CancellationToken cancellationToken)
    {
        var request = Request("a");
        using var giveUp = new CancellationTokenSource();
        var waiting = request.AwaitLeadershipAsync(giveUp.Token);
        _ = request.TryTake();

        await giveUp.CancelAsync();
        request.Succeed(ReadOnlyMemory<byte>.Empty);

        _ = await Assert.That(await waiting.WaitAsync(StallTimeout, TimeProvider.System, cancellationToken)).IsFalse();
        _ = await Assert.That(request.IsCanceled()).IsFalse();
    }

    private static ReplicaStateCommitRequest<int> Request(string key) => new(
        ("cache", "op-" + key, key),
        0,
        static _ => [],
        static (_, _, _, _) => ValueTask.FromException<PreparedReplicaMutation>(new InvalidOperationException("The request is never prepared.")));
}
