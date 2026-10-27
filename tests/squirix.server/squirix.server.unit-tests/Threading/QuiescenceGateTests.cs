using System;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.TestKit;
using Squirix.Server.Threading;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Threading;

/// <summary>Verifies the quiescence gate counts in-flight operations and refuses new ones while it is closed.</summary>
public sealed class QuiescenceGateTests : ServerUnitTestBase
{
    private static readonly TimeSpan StallTimeout = TimeSpan.FromSeconds(10);

    /// <summary>An open gate admits an operation and tracks it until it exits.</summary>
    /// <returns>An asynchronous operation.</returns>
    [Test]
    public async Task OpenGateAdmitsAndTracks()
    {
        var gate = new QuiescenceGate();

        _ = await Assert.That(gate.TryEnter()).IsTrue();
        _ = await Assert.That(gate.HasPending).IsTrue();

        gate.Exit();

        _ = await Assert.That(gate.HasPending).IsFalse();
    }

    /// <summary>A closed gate refuses admission and does not count the refused operation.</summary>
    /// <returns>An asynchronous operation.</returns>
    [Test]
    public async Task ClosedGateRefusesAdmission()
    {
        var gate = new QuiescenceGate();
        gate.Close();

        _ = await Assert.That(gate.TryEnter()).IsFalse();
        _ = await Assert.That(gate.HasPending).IsFalse();

        gate.Open();

        _ = await Assert.That(gate.TryEnter()).IsTrue();
        gate.Exit();
    }

    /// <summary>The unconditional enter still counts while the gate is closed, and the drain wait completes once it exits.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task EnterIgnoresClosedGate(CancellationToken cancellationToken)
    {
        var gate = new QuiescenceGate();
        gate.Close();
        gate.Enter();
        var drained = gate.WaitAsync(cancellationToken);

        _ = await Assert.That(drained.IsCompleted).IsFalse();

        gate.Exit();
        await drained.AsTask().WaitAsync(StallTimeout, TimeProvider.System, cancellationToken);
        gate.Open();
    }

    /// <summary>Closings nest: the gate admits again only after every closer opened it.</summary>
    /// <returns>An asynchronous operation.</returns>
    [Test]
    public async Task ClosingsNest()
    {
        var gate = new QuiescenceGate();
        gate.Close();
        gate.Close();
        gate.Open();

        _ = await Assert.That(gate.TryEnter()).IsFalse();

        gate.Open();

        _ = await Assert.That(gate.TryEnter()).IsTrue();
        gate.Exit();
    }

    /// <summary>Waiting for the gate to open completes at once when it is open and when it reopens.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task WaitOpenCompletesOnOpen(CancellationToken cancellationToken)
    {
        var gate = new QuiescenceGate();

        await gate.WaitOpenAsync(cancellationToken);

        gate.Close();
        var first = gate.WaitOpenAsync(cancellationToken);
        var second = gate.WaitOpenAsync(cancellationToken);

        _ = await Assert.That(first.IsCompleted).IsFalse();

        gate.Open();
        await first.AsTask().WaitAsync(StallTimeout, TimeProvider.System, cancellationToken);
        await second.AsTask().WaitAsync(StallTimeout, TimeProvider.System, cancellationToken);
    }

    /// <summary>A cancelled wait for the gate to open faults with the cancellation and leaves the gate closed.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task WaitOpenIsCancellable(CancellationToken cancellationToken)
    {
        var gate = new QuiescenceGate();
        gate.Close();
        using var waitCancellation = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        var wait = gate.WaitOpenAsync(waitCancellation.Token);

        await waitCancellation.CancelAsync();

        _ = await NodeAsyncAssert.ThrowsAnyAsync<OperationCanceledException>(wait.AsTask().WaitAsync(StallTimeout, TimeProvider.System, cancellationToken));
        _ = await Assert.That(gate.TryEnter()).IsFalse();
        gate.Open();
    }

    /// <summary>Opening a gate that is not closed is a programming error.</summary>
    /// <returns>An asynchronous operation.</returns>
    [Test]
    public async Task OpenWithoutCloseThrows()
    {
        var gate = new QuiescenceGate();

        _ = NodeExceptionAssert.For<InvalidOperationException>().Throws(gate, static g => g.Open());

        _ = await Assert.That(gate.TryEnter()).IsTrue();
        gate.Exit();
    }
}
