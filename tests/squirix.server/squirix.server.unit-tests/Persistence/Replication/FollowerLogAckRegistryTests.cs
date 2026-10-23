using System;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.Storage.Replication;
using Squirix.Server.TestKit;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Persistence.Replication;

/// <summary>The in-flight ack registry of a follower log: the pool thread owns the outcome, the shutdown drain only faults.</summary>
[Immutable]
public sealed class FollowerLogAckRegistryTests : ServerUnitTestBase
{
    /// <summary>A drained registry refuses a later ack with the recorded reason instead of parking its caller.</summary>
    [Test]
    public async Task TrackAfterFaultAllFailsFast()
    {
        var registry = new FollowerLogAckRegistry();
        var reason = new ObjectDisposedException(nameof(FollowerLog));
        _ = registry.FaultAll(reason);

        var thrown = NodeExceptionAssert.For<ObjectDisposedException>().Throws(registry, NewAck(), static (acks, ack) => acks.Track(ack));

        _ = await Assert.That(thrown).IsSameReferenceAs(reason);
    }

    /// <summary>The drain faults every ack still in flight, skips the ones whose outcome was written, and counts only the ones it faulted.</summary>
    [Test]
    public async Task FaultAllCountsInFlightAcks()
    {
        var registry = new FollowerLogAckRegistry();
        var first = NewAck();
        var second = NewAck();
        var settled = NewAck();
        var released = NewAck();
        registry.Track(first);
        registry.Track(second);
        registry.Track(settled);
        registry.Track(released);
        _ = settled.TrySetResult();
        _ = released.TrySetResult();
        registry.Complete(released);

        var faulted = registry.FaultAll(new ObjectDisposedException(nameof(FollowerLog)));

        _ = await Assert.That(faulted).IsEqualTo(2);
        _ = await Assert.That(first.Task.Exception?.InnerException).IsTypeOf<ObjectDisposedException>();
        _ = await Assert.That(second.Task.Exception?.InnerException).IsTypeOf<ObjectDisposedException>();
        _ = await Assert.That(settled.Task.IsCompletedSuccessfully).IsTrue();
        _ = await Assert.That(registry.FaultAll(new ObjectDisposedException(nameof(FollowerLog)))).IsEqualTo(0);
    }

    /// <summary>The late outcome of an op the drain already faulted neither throws nor replaces the shutdown fault.</summary>
    [Test]
    public async Task CompleteAfterFaultIsNoOp()
    {
        var registry = new FollowerLogAckRegistry();
        var ack = NewAck();
        registry.Track(ack);
        _ = registry.FaultAll(new ObjectDisposedException(nameof(FollowerLog)));

        var lateOutcome = ack.TrySetResult();
        registry.Complete(ack);

        _ = await Assert.That(lateOutcome).IsFalse();
        _ = await Assert.That(ack.Task.Exception?.InnerException).IsTypeOf<ObjectDisposedException>();
    }

    private static TaskCompletionSource NewAck() => new(TaskCreationOptions.RunContinuationsAsynchronously);
}
