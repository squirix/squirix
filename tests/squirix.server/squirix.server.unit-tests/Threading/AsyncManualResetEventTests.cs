using System.Threading.Tasks;
using Squirix.Server.Threading;
using Squirix.Server.UnitTests.Support;
using Xunit;

namespace Squirix.Server.UnitTests.Threading;

/// <summary>Unit tests for <see cref="AsyncManualResetEvent"/> readiness-latch semantics.</summary>
public sealed class AsyncManualResetEventTests : ServerUnitTestBase
{
    /// <summary>A closed gate blocks waiters until <see cref="AsyncManualResetEvent.Set"/> is called.</summary>
    [Fact]
    public async Task StartsUnsetAndBlocksWaitUntilSet()
    {
        var gate = new AsyncManualResetEvent();
        Assert.False(gate.IsSet);

        var waitTask = gate.WaitAsync(DefaultCancellationToken);
        Assert.False(waitTask.IsCompleted);

        gate.Set();
        await waitTask;
        Assert.True(gate.IsSet);
    }

    /// <summary>A gate opened before the first wait completes the wait immediately.</summary>
    [Fact]
    public async Task SetBeforeWaitCompletesImmediately()
    {
        var gate = new AsyncManualResetEvent();
        gate.Set();

        await gate.WaitAsync(DefaultCancellationToken);
        Assert.True(gate.IsSet);
    }

    /// <summary>Constructing with the ready state yields a set gate.</summary>
    [Fact]
    public async Task InitialStateTrueIsSet()
    {
        var gate = new AsyncManualResetEvent(true);
        Assert.True(gate.IsSet);

        await gate.WaitAsync(DefaultCancellationToken);
    }

    /// <summary>Repeated <see cref="AsyncManualResetEvent.Set"/> calls are idempotent.</summary>
    [Fact]
    public async Task SetIsIdempotent()
    {
        var gate = new AsyncManualResetEvent();
        gate.Set();
        gate.Set();

        Assert.True(gate.IsSet);
        await gate.WaitAsync(DefaultCancellationToken);
        Assert.True(gate.IsSet);
    }
}
