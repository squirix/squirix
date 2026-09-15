using System;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.Storage.Journaling;
using Squirix.Server.TestKit;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Persistence.Journaling;

/// <summary>Shutdown gate admission, rejection, and bounded drain.</summary>
[Immutable]
public sealed class JournalProducerGateTests : ServerUnitTestBase
{
    /// <summary>Entered and exited work drains immediately within any sane timeout.</summary>
    [Test]
    public async Task EnterExitDrainsImmediately()
    {
        var gate = new JournalProducerGate();
        gate.Enter();
        gate.Exit();

        _ = await Assert.That(await gate.WaitAsync(TimeSpan.FromSeconds(1))).IsTrue();
    }

    /// <summary>Work held past the timeout fails the drain instead of hanging disposal.</summary>
    [Test]
    public async Task HeldEnterTimesOutDrain()
    {
        var gate = new JournalProducerGate();
        gate.Enter();
        try
        {
            _ = await Assert.That(await gate.WaitAsync(TimeSpan.FromMilliseconds(1))).IsFalse();
        }
        finally
        {
            gate.Exit();
        }
    }

    /// <summary>Initiated shutdown rejects new work with ObjectDisposedException.</summary>
    [Test]
    public void InitiateShutdownRejectsNewWork()
    {
        var gate = new JournalProducerGate();
        gate.InitiateShutdown();

        _ = NodeExceptionAssert.For<ObjectDisposedException>().Throws(gate, static g => g.ThrowIfShutdownInitiated());
    }
}
