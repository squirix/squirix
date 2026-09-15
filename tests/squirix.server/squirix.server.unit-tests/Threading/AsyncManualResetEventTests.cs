using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Threading;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Threading;

/// <summary>Unit tests for <see cref="AsyncManualResetEvent" /> readiness-latch semantics.</summary>
public sealed class AsyncManualResetEventTests : ServerUnitTestBase
{
    /// <summary>Constructing with the ready state yields a set gate.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task InitialStateTrueIsSet(CancellationToken cancellationToken)
    {
        var gate = new AsyncManualResetEvent(true);
        _ = await Assert.That(gate.IsSet).IsTrue();

        await gate.WaitAsync(cancellationToken);
    }

    /// <summary>A gate opened before the first wait completes the wait immediately.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task SetBeforeWaitCompletesImmediately(CancellationToken cancellationToken)
    {
        var gate = new AsyncManualResetEvent();
        gate.Set();

        await gate.WaitAsync(cancellationToken);
        _ = await Assert.That(gate.IsSet).IsTrue();
    }

    /// <summary>Repeated <see cref="AsyncManualResetEvent.Set" /> calls are idempotent.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task SetIsIdempotent(CancellationToken cancellationToken)
    {
        var gate = new AsyncManualResetEvent();
        gate.Set();
        gate.Set();

        _ = await Assert.That(gate.IsSet).IsTrue();
        await gate.WaitAsync(cancellationToken);
        _ = await Assert.That(gate.IsSet).IsTrue();
    }

    /// <summary>A closed gate blocks waiters until <see cref="AsyncManualResetEvent.Set" /> is called.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task StartsUnsetAndBlocksWaitUntilSet(CancellationToken cancellationToken)
    {
        var gate = new AsyncManualResetEvent();
        _ = await Assert.That(gate.IsSet).IsFalse();

        var waitTask = gate.WaitAsync(cancellationToken);
        _ = await Assert.That(waitTask.IsCompleted).IsFalse();

        gate.Set();
        await waitTask;
        _ = await Assert.That(gate.IsSet).IsTrue();
    }
}
