using System;
using System.Threading;
using System.Threading.Tasks;
using Grpc.Core;
using Squirix.Server.IntegrationTests.Support;
using Squirix.Server.TestKit;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.IntegrationTests.Cluster.Replication;

/// <summary>Exercises ordered exchanges and pool shutdown over real HTTP/2 connections.</summary>
public sealed class ReplicaAppendStreamTests
{
    /// <summary>Repeated messages reuse the call and return their own terms.</summary>
    /// <param name="cancellationToken">The cancellationToken for this test.</param>
    [Test]
    public async Task ReusesCallWithFreshReplies(CancellationToken cancellationToken)
    {
        await using var peer = new AppendStreamPeer();
        await peer.StartAsync(cancellationToken);
        for (var term = 1UL; term <= 20; term++)
        {
            var reply = await peer.SendAsync("a", term, cancellationToken);
            _ = await Assert.That(reply.Term).IsEqualTo(term);
        }

        _ = await Assert.That(peer.Streams).IsEqualTo(1);
        _ = await Assert.That(peer.Unary).IsEqualTo(0);
    }

    /// <summary>Idle EOF, errors and unsolicited replies release the request without forcing server shutdown.</summary>
    /// <param name="kind">One ends the response, two fails it, and three sends an unsolicited next reply.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    [Arguments(1)]
    [Arguments(2)]
    [Arguments(3)]
    public async Task IdlePeerClosureDoesNotDelayStop(int kind, CancellationToken cancellationToken)
    {
        await using var peer = new AppendStreamPeer { AfterReplyKind = kind };
        await peer.StartAsync(cancellationToken);
        var reply = await peer.SendAsync("a", 7, cancellationToken);
        _ = await Assert.That(reply.Term).IsEqualTo(7UL);
        _ = peer.ResumeAfterReply.TrySetResult();
        using var budget = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        budget.CancelAfter(TimeSpan.FromSeconds(5));

        await peer.StopAsync(budget.Token);

        _ = await Assert.That(budget.IsCancellationRequested).IsFalse();
        _ = await Assert.That(peer.Ended.Task.IsCompletedSuccessfully).IsTrue();
    }

    /// <summary>A preemptive reply cannot be retained as acknowledgement of the next request.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task UnsolicitedReplyCannotAckNextCall(CancellationToken cancellationToken)
    {
        await using var peer = new AppendStreamPeer { AfterReplyKind = 3 };
        await peer.StartAsync(cancellationToken);
        var first = await peer.SendAsync("a", 7, cancellationToken);
        _ = peer.ResumeAfterReply.TrySetResult();
        await peer.Ended.Task.WaitAsync(TimeSpan.FromSeconds(5), TimeProvider.System, cancellationToken);

        var second = await peer.SendAsync("a", 8, cancellationToken);

        _ = await Assert.That((first.Term, second.Term)).IsEqualTo((7UL, 8UL));
        _ = await Assert.That(peer.Streams).IsEqualTo(2);
        _ = await Assert.That(peer.Unary).IsEqualTo(0);
    }

    /// <summary>A bounded call rotates after its final matching acknowledgement.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task RotationKeepsReplyAssociation(CancellationToken cancellationToken)
    {
        await using var peer = new AppendStreamPeer();
        await peer.StartAsync(cancellationToken);
        for (var term = 1UL; term <= 4097; term++)
        {
            var reply = await peer.SendAsync("a", term, cancellationToken);
            _ = await Assert.That(reply.Term).IsEqualTo(term);
        }

        _ = await Assert.That(peer.Streams).IsEqualTo(2);
    }

    /// <summary>A cancelled queued exchange never invalidates another caller's active exchange.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task QueuedCancellationKeepsActiveCall(CancellationToken cancellationToken)
    {
        await using var peer = new AppendStreamPeer { BlockFirst = true };
        await peer.StartAsync(cancellationToken);
        var first = peer.SendAsync("a", 7, cancellationToken);
        await peer.Arrived.Task.WaitAsync(cancellationToken);
        using var budget = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        var queued = peer.SendAsync("a", 8, budget.Token);
        await budget.CancelAsync();
        _ = await NodeAsyncAssert.ThrowsAnyAsync<OperationCanceledException>(queued, TimeSpan.FromSeconds(15), TimeProvider.System, cancellationToken);
        _ = peer.Release.TrySetResult();
        var reply = await first;
        var next = await peer.SendAsync("a", 9, cancellationToken);

        _ = await Assert.That((reply.Term, next.Term, peer.Streams)).IsEqualTo((7UL, 9UL, 1));
    }

    /// <summary>Only an explicitly unsupported stream retries through the older unary endpoint.</summary>
    /// <param name="failure">The failure for this test.</param>
    /// <param name="fallback">The fallback for this test.</param>
    /// <param name="cancellationToken">The cancellationToken for this test.</param>
    [Test]
    [Arguments(StatusCode.Unimplemented, true)]
    [Arguments(StatusCode.Unavailable, false)]
    [Arguments(StatusCode.Unauthenticated, false)]
    public async Task FallbackOnlyForOldPeer(StatusCode failure, bool fallback, CancellationToken cancellationToken)
    {
        await using var peer = new AppendStreamPeer { Failure = failure };
        await peer.StartAsync(cancellationToken);
        if (fallback)
        {
            var first = await peer.SendAsync("a", 7, cancellationToken);
            var second = await peer.SendAsync("a", 8, cancellationToken);
            _ = await Assert.That((first.Term, second.Term)).IsEqualTo((7UL, 8UL));
            _ = await Assert.That(peer.Unary).IsEqualTo(2);
            _ = await Assert.That(peer.Streams).IsEqualTo(1);
        }
        else
        {
            var error = await NodeAsyncAssert.ThrowsAsync<RpcException>(peer.SendAsync("a", 7, cancellationToken), TimeSpan.FromSeconds(15), TimeProvider.System, cancellationToken);
            _ = await Assert.That(error.StatusCode).IsEqualTo(failure);
            _ = await Assert.That(peer.Unary).IsEqualTo(0);
        }
    }

    /// <summary>Wrong, missing or absent replies are never counted; the next exchange uses a new call.</summary>
    /// <param name="kind">The kind for this test.</param>
    /// <param name="expected">The expected for this test.</param>
    /// <param name="cancellationToken">The cancellationToken for this test.</param>
    [Test]
    [Arguments(1, StatusCode.DataLoss)]
    [Arguments(2, StatusCode.DataLoss)]
    [Arguments(3, StatusCode.Unavailable)]
    public async Task InvalidReplyForcesReconnect(int kind, StatusCode expected, CancellationToken cancellationToken)
    {
        await using var peer = new AppendStreamPeer { ResponseKind = kind };
        await peer.StartAsync(cancellationToken);
        var error = await NodeAsyncAssert.ThrowsAsync<RpcException>(peer.SendAsync("a", 7, cancellationToken), TimeSpan.FromSeconds(15), TimeProvider.System, cancellationToken);
        _ = await Assert.That(error.StatusCode).IsEqualTo(expected);
        peer.ResponseKind = 0;

        var reply = await peer.SendAsync("a", 8, cancellationToken);

        _ = await Assert.That(reply.Term).IsEqualTo(8UL);
        _ = await Assert.That(peer.Streams).IsEqualTo(2);
        _ = await Assert.That(peer.Unary).IsEqualTo(0);
    }

    /// <summary>Cancelled in-flight replies cannot satisfy a later exchange, and other groups remain independent.</summary>
    /// <param name="cancellationToken">The cancellationToken for this test.</param>
    [Test]
    public async Task CancelledCallCannotLeakReply(CancellationToken cancellationToken)
    {
        await using var peer = new AppendStreamPeer { BlockFirst = true };
        await peer.StartAsync(cancellationToken);
        using var budget = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        var blocked = peer.SendAsync("a", 7, budget.Token);
        await peer.Arrived.Task.WaitAsync(cancellationToken);

        var independent = await peer.SendAsync("b", 8, cancellationToken);
        _ = await Assert.That(independent.Term).IsEqualTo(8UL);
        await budget.CancelAsync();
        _ = await NodeAsyncAssert.ThrowsAnyAsync<RpcException>(blocked, TimeSpan.FromSeconds(15), TimeProvider.System, cancellationToken);
        _ = peer.Release.TrySetResult();
        var fresh = await peer.SendAsync("a", 9, cancellationToken);

        _ = await Assert.That(fresh.Term).IsEqualTo(9UL);
        _ = await Assert.That(peer.Streams).IsEqualTo(3);
    }

    /// <summary>The pool can close both idle and active streams without retaining leases forever.</summary>
    /// <param name="active">The active for this test.</param>
    /// <param name="cancellationToken">The cancellationToken for this test.</param>
    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task PoolShutdownDrainsStreams(bool active, CancellationToken cancellationToken)
    {
        await using var peer = new AppendStreamPeer { BlockFirst = active };
        await peer.StartAsync(cancellationToken);
        var pending = peer.SendAsync("a", 7, cancellationToken);
        await peer.Arrived.Task.WaitAsync(cancellationToken);
        if (!active)
            _ = await pending;

        await peer.Pool.DisposeAsync().AsTask().WaitAsync(cancellationToken);
        if (active)
            _ = await NodeAsyncAssert.ThrowsAnyAsync<RpcException>(pending, TimeSpan.FromSeconds(15), TimeProvider.System, cancellationToken);

        peer.Transport.Dispose();
        peer.Transport.Dispose();
        _ = NodeExceptionAssert.For<ObjectDisposedException>().Throws((peer, cancellationToken), static state => _ = state.peer.SendAsync("a", 8, state.cancellationToken));
    }
}
