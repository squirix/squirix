using System;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Storage.Journaling;
using Squirix.Server.TestKit;
using Xunit;

namespace Squirix.Server.UnitTests.Persistence.Journaling;

/// <summary>Registration and terminal-failure draining semantics of <see cref="DurabilityAckRegistry" />.</summary>
public sealed class DurabilityAckRegistryTests
{
    /// <summary>Ack registration succeeds while the journal has not failed; failure drains registered acks.</summary>
    [Fact]
    public void TryRegisterRegistersAckWhenJournalHasNotFailed()
    {
        var registry = new DurabilityAckRegistry();
        var ack = DurabilityAck.Rent();
        try
        {
            Assert.True(registry.TryRegister(ack));

            var drained = registry.Fail(new InvalidOperationException("journal failed"));
            _ = Assert.Single(drained);
            Assert.Same(ack, drained[0]);
        }
        finally
        {
            ack.ReturnToPool();
        }
    }

    /// <summary>A registration arriving after the terminal failure is rejected and its wait carries the recorded failure.</summary>
    [Fact]
    public async Task LateRegistrationCompletesWithJournalFailure()
    {
        var registry = new DurabilityAckRegistry();
        var failure = new InvalidOperationException("journal failed");
        _ = registry.Fail(failure);

        var ack = DurabilityAck.Rent();
        try
        {
            var waitTask = ack.AwaitAsync(CancellationToken.None);

            // Late registration after the terminal failure: registration is rejected and the
            // wait carries the recorded failure instead of hanging forever.
            Assert.False(registry.TryRegister(ack));
            var observed = await NodeAsyncAssert.ThrowsAsync<InvalidOperationException>(waitTask);

            Assert.Same(failure, observed);
        }
        finally
        {
            ack.ReturnToPool();
        }
    }

    /// <summary>Failure drains every currently registered ack in one batch.</summary>
    [Fact]
    public void FailDrainsAllRegisteredAcks()
    {
        var registry = new DurabilityAckRegistry();
        var first = DurabilityAck.Rent();
        var second = DurabilityAck.Rent();
        try
        {
            Assert.True(registry.TryRegister(first));
            Assert.True(registry.TryRegister(second));

            Assert.Equal(2, registry.Fail(new OperationCanceledException()).Count);
        }
        finally
        {
            first.ReturnToPool();
            second.ReturnToPool();
        }
    }
}
