using System;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.Storage.Journaling;
using Squirix.Server.TestKit;
using Squirix.Server.UnitTests.Support;
using Xunit;

namespace Squirix.Server.UnitTests.Persistence.Journaling;

/// <summary>Shutdown gate admission, rejection, and bounded drain.</summary>
[Immutable]
public sealed class JournalProducerGateTests : ServerUnitTestBase
{
    /// <summary>Entered and exited work drains immediately within any sane timeout.</summary>
    [Fact]
    public async Task EnterExitDrainsImmediately()
    {
        var gate = new JournalProducerGate();
        gate.Enter();
        gate.Exit();

        Assert.True(await gate.WaitAsync(TimeSpan.FromSeconds(1)));
    }

    /// <summary>Work held past the timeout fails the drain instead of hanging disposal.</summary>
    [Fact]
    public async Task HeldEnterTimesOutDrain()
    {
        var gate = new JournalProducerGate();
        gate.Enter();
        try
        {
            Assert.False(await gate.WaitAsync(TimeSpan.FromMilliseconds(1)));
        }
        finally
        {
            gate.Exit();
        }
    }

    /// <summary>Initiated shutdown rejects new work with ObjectDisposedException.</summary>
    [Fact]
    public void InitiateShutdownRejectsNewWork()
    {
        var gate = new JournalProducerGate();
        gate.InitiateShutdown();

        _ = NodeExceptionAssert.For<ObjectDisposedException>().Throws(gate, static g => g.ThrowIfShutdownInitiated());
    }
}
