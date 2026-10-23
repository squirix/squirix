using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Time.Testing;
using Squirix.Transport.Grpc;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Cluster;

/// <summary>Unit tests for the single-source timeout and parent-forwarding scope used by the call policies.</summary>
public sealed class TimedCancellationTests
{
    private static readonly TimeSpan Timeout = TimeSpan.FromSeconds(5);

    /// <summary>Ensures an already canceled parent cancels the source before the constructor returns.</summary>
    [Test]
    public async Task AlreadyCanceledParentCancelsAtOnce()
    {
        using var parent = new CancellationTokenSource();
        await parent.CancelAsync();
        using var scope = new TimedCancellation(Timeout, new FakeTimeProvider(), parent.Token);

        _ = await Assert.That(scope.Token.IsCancellationRequested).IsTrue();
    }

    /// <summary>Ensures disposal stops the timer and unregisters from the parent, so a later parent cancellation or clock advance does nothing.</summary>
    [Test]
    public async Task DisposeStopsTimerAndUnregisters()
    {
        var clock = new CountingTimeProvider();
        using var parent = new CancellationTokenSource();
        var scope = new TimedCancellation(Timeout, clock, parent.Token);
        var token = scope.Token;
        scope.Dispose();

        await parent.CancelAsync();
        clock.Advance(Timeout);

        _ = await Assert.That(clock.Created).IsEqualTo(1);
        _ = await Assert.That(clock.Disposed).IsEqualTo(1);
        _ = await Assert.That(token.IsCancellationRequested).IsFalse();
    }

    /// <summary>Ensures a parent that cannot cancel still gets the timeout, with a single timer.</summary>
    [Test]
    public async Task NonCancelableParentStillTimesOut()
    {
        var clock = new CountingTimeProvider();
        using var scope = new TimedCancellation(Timeout, clock, CancellationToken.None);

        var before = scope.Token.IsCancellationRequested;
        clock.Advance(Timeout);

        _ = await Assert.That(before).IsFalse();
        _ = await Assert.That(scope.Token.IsCancellationRequested).IsTrue();
        _ = await Assert.That(clock.Created).IsEqualTo(1);
    }

    /// <summary>Ensures canceling the parent cancels the source, and the parent is not canceled by the source.</summary>
    [Test]
    public async Task ParentCancellationCancelsSource()
    {
        using var parent = new CancellationTokenSource();
        using var scope = new TimedCancellation(Timeout, new FakeTimeProvider(), parent.Token);

        var before = scope.Token.IsCancellationRequested;
        await parent.CancelAsync();

        _ = await Assert.That(before).IsFalse();
        _ = await Assert.That(scope.Token.IsCancellationRequested).IsTrue();
    }

    /// <summary>Ensures the timeout cancels the source without canceling the parent.</summary>
    [Test]
    public async Task TimeoutCancelsSourceNotParent()
    {
        var clock = new FakeTimeProvider();
        using var parent = new CancellationTokenSource();
        using var scope = new TimedCancellation(Timeout, clock, parent.Token);

        clock.Advance(Timeout - TimeSpan.FromMilliseconds(1));
        var early = scope.Token.IsCancellationRequested;
        clock.Advance(TimeSpan.FromMilliseconds(1));

        _ = await Assert.That(early).IsFalse();
        _ = await Assert.That(scope.Token.IsCancellationRequested).IsTrue();
        _ = await Assert.That(parent.IsCancellationRequested).IsFalse();
    }

    /// <summary>Ensures a zero timeout cancels the source at once.</summary>
    [Test]
    public async Task ZeroTimeoutCancelsAtOnce()
    {
        using var scope = new TimedCancellation(TimeSpan.Zero, new FakeTimeProvider(), CancellationToken.None);

        _ = await Assert.That(scope.Token.IsCancellationRequested).IsTrue();
    }

    /// <summary>A fake clock that counts the timers it created and the timers that were disposed.</summary>
    private sealed class CountingTimeProvider : TimeProvider
    {
        private readonly FakeTimeProvider _inner = new();
        private int _created;
        private int _disposed;

        internal int Created => Volatile.Read(ref _created);

        internal int Disposed => Volatile.Read(ref _disposed);

        public override ITimer CreateTimer(TimerCallback callback, object? state, TimeSpan dueTime, TimeSpan period)
        {
            _ = Interlocked.Increment(ref _created);
            return new CountingTimer(_inner.CreateTimer(callback, state, dueTime, period), this);
        }

        internal void Advance(TimeSpan delta) => _inner.Advance(delta);

        private sealed class CountingTimer : ITimer
        {
            private readonly CountingTimeProvider _owner;
            private readonly ITimer _timer;

            internal CountingTimer(ITimer timer, CountingTimeProvider owner)
            {
                _timer = timer;
                _owner = owner;
            }

            public bool Change(TimeSpan dueTime, TimeSpan period) => _timer.Change(dueTime, period);

            public void Dispose()
            {
                _ = Interlocked.Increment(ref _owner._disposed);
                _timer.Dispose();
            }

            public ValueTask DisposeAsync()
            {
                _ = Interlocked.Increment(ref _owner._disposed);
                return _timer.DisposeAsync();
            }
        }
    }
}
