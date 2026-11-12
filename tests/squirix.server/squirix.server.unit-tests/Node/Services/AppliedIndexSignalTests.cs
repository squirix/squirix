using System;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.Node.Services;
using Squirix.Server.TestKit;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Node.Services;

/// <summary>Targeted apply notifications share work and release canceled registrations.</summary>
[Immutable]
public sealed class AppliedIndexSignalTests
{
    /// <summary>Different targets survive smaller advances; a jump completes every covered target.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task NotifyCompletesOnlyReachedTargets(CancellationToken cancellationToken)
    {
        var signal = new AppliedIndexSignal();
        var first = signal.WaitAsync(10UL, cancellationToken);
        var shared = signal.WaitAsync(10UL, cancellationToken);
        var later = signal.WaitAsync(20UL, cancellationToken);
        _ = await Assert.That(signal.PendingTargets).IsEqualTo(2);
        signal.Notify(9UL);
        _ = await Assert.That(first.IsCompleted || shared.IsCompleted || later.IsCompleted).IsFalse();
        signal.Notify(10UL);
        await Task.WhenAll(first, shared).WaitAsync(TimeSpan.FromSeconds(30), TimeProvider.System, cancellationToken);
        _ = await Assert.That(later.IsCompleted).IsFalse();
        _ = await Assert.That(signal.PendingTargets).IsEqualTo(1);
        signal.Notify(30UL);
        await later.WaitAsync(TimeSpan.FromSeconds(30), TimeProvider.System, cancellationToken);
        _ = await Assert.That(signal.PendingTargets).IsEqualTo(0);
    }

    /// <summary>Canceling one reader leaves its peer alive; canceling the last reader removes its target.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task CancellationRemovesOnlyUnusedTargets(CancellationToken cancellationToken)
    {
        var signal = new AppliedIndexSignal();
        using var source = new CancellationTokenSource();
        var canceled = signal.WaitAsync(10UL, source.Token);
        var peer = signal.WaitAsync(10UL, cancellationToken);
        await source.CancelAsync();
        _ = await NodeAsyncAssert.ThrowsAnyAsync<OperationCanceledException>(canceled, TimeSpan.FromSeconds(30), TimeProvider.System, cancellationToken);
        _ = await Assert.That((peer.IsCompleted, signal.PendingTargets)).IsEqualTo((false, 1));
        signal.Notify(10UL);
        await peer.WaitAsync(TimeSpan.FromSeconds(30), TimeProvider.System, cancellationToken);

        var alone = signal.WaitAsync(20UL, source.Token);
        _ = await NodeAsyncAssert.ThrowsAnyAsync<OperationCanceledException>(alone, TimeSpan.FromSeconds(30), TimeProvider.System, cancellationToken);
        _ = await Assert.That(signal.PendingTargets).IsEqualTo(0);
    }

    /// <summary>A reset downward makes a previously reached target wait again.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ResetDoesNotReuseEarlierCompletion(CancellationToken cancellationToken)
    {
        var signal = new AppliedIndexSignal();
        signal.Notify(20UL);
        _ = await Assert.That(signal.WaitAsync(10UL, cancellationToken).IsCompletedSuccessfully).IsTrue();
        signal.Notify(5UL);
        var wait = signal.WaitAsync(10UL, cancellationToken);
        _ = await Assert.That(wait.IsCompleted).IsFalse();
        signal.Notify(10UL);
        await wait.WaitAsync(TimeSpan.FromSeconds(30), TimeProvider.System, cancellationToken);
        _ = await Assert.That(signal.PendingTargets).IsEqualTo(0);
    }
}
