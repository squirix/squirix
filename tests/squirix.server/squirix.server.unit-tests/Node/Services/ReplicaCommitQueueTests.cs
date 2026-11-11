using System;
using System.Runtime.CompilerServices;
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

        _ = await NodeAsyncAssert.ThrowsAnyAsync<OperationCanceledException>(waiting, StallTimeout, TimeProvider.System, cancellationToken);
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

    /// <summary>Canceled payloads are released before the leading commit finishes.</summary>
    [Test]
    public async Task CanceledPayloadsAreReleasedTest()
    {
        var queue = new ReplicaCommitQueue();
        _ = queue.Enter(Request("leader"));
        var references = QueueCanceledPayloads(queue);
#pragma warning disable S1215 // Forced collection checks retained payloads while the queue stays alive.
        GC.Collect(GC.MaxGeneration, GCCollectionMode.Forced, true, true);
#pragma warning restore S1215
        var retained = CountAlive(references);
        _ = await Assert.That(retained).IsEqualTo(0);
        queue.HandOver();
#pragma warning disable S1215 // Forced collection checks payload release after the queue is drained.
        GC.Collect(GC.MaxGeneration, GCCollectionMode.Forced, true, true);
#pragma warning restore S1215
        _ = await Assert.That(CountAlive(references)).IsEqualTo(0);
        GC.KeepAlive(queue);
    }

    /// <summary>Removing a canceled middle request preserves the order of live requests.</summary>
    [Test]
    public async Task UnlinkPreservesLiveOrderTest()
    {
        var queue = new ReplicaCommitQueue();
        _ = queue.Enter(Request("leader"));
        var first = Request("first");
        var canceled = Request("canceled");
        var last = Request("last");
        _ = queue.Enter(first);
        _ = queue.Enter(canceled);
        _ = queue.Enter(last);
        _ = canceled.TryCancel();
        queue.RemoveCanceled(canceled);
        queue.RemoveCanceled(canceled);
        _ = await Assert.That(canceled.QueueNode).IsNull();
        _ = await Assert.That(queue.PeekLive()).IsSameReferenceAs(first);
        queue.RemoveHead(first);
        _ = await Assert.That(queue.PeekLive()).IsSameReferenceAs(last);
        _ = last.TryTake();
        queue.RemoveCanceled(last);
        _ = await Assert.That(queue.PeekLive()).IsSameReferenceAs(last);
    }

    /// <summary>Cancellation racing promotion leaves no queued node and preserves the winning state.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task CancelAndPromoteRaceTest(CancellationToken cancellationToken)
    {
        for (var i = 0; i < 128; i++)
        {
            var queue = new ReplicaCommitQueue();
            _ = queue.Enter(Request("leader"));
            var request = Request("next");
            _ = queue.Enter(request);
            CancelAndPromote(queue, request);
            _ = await Assert.That(request.QueueNode).IsNull();
            if (!request.IsCanceled())
                _ = await Assert.That(await request.AwaitLeadershipAsync(cancellationToken)).IsTrue();
        }
    }

    [MethodImpl(MethodImplOptions.NoInlining)]
    private static WeakReference[] QueueCanceledPayloads(ReplicaCommitQueue queue)
    {
        var references = new WeakReference[64];
        for (var i = 0; i < references.Length; i++)
        {
            var payload = new byte[4096];
            references[i] = new WeakReference(payload);
            var request = new ReplicaStateCommitRequest<byte[]>(
                ("cache", $"operation-{i}", "key"),
                payload,
                static _ => [],
                static (_, _, _, _) => ValueTask.FromException<PreparedReplicaMutation>(new InvalidOperationException("Not executed.")));
            _ = queue.Enter(request);
            _ = request.TryCancel();
            queue.RemoveCanceled(request);
        }

        return references;
    }

    private static void CancelAndPromote(ReplicaCommitQueue queue, ReplicaCommitRequest request) => Parallel.Invoke(
        () =>
        {
            _ = request.TryCancel();
            queue.RemoveCanceled(request);
        },
        queue.HandOver);

    [MethodImpl(MethodImplOptions.NoInlining)]
    private static int CountAlive(WeakReference[] references)
    {
        var count = 0;
        foreach (var reference in references)
        {
            if (reference.IsAlive)
                count++;
        }

        return count;
    }

    private static ReplicaStateCommitRequest<int> Request(string key) => new(
        ("cache", "op-" + key, key),
        0,
        static _ => [],
        static (_, _, _, _) => ValueTask.FromException<PreparedReplicaMutation>(new InvalidOperationException("The request is never prepared.")));
}
