using System;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Adapters.Endpoint;
using Squirix.Testing;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Hosting;

/// <summary>Checks the lifetimes of requests sharing one connection alarm.</summary>
public sealed class BinaryRequestDeadlinesTests
{
    /// <summary>A short deadline preempts a longer one, a completed call stays detached, and closing reaches the remaining call.</summary>
    [Test]
    public async Task ExpirationStaysWithItsRequest()
    {
        var clock = new FakeTimeProvider();
        using var closed = new CancellationTokenSource();
        using var deadlines = new BinaryRequestDeadlines(clock, closed.Token);
        using var longCall = deadlines.Start(TimeSpan.FromSeconds(15));
        using var completed = deadlines.Start(TimeSpan.FromSeconds(2));
        var completedToken = completed.Token;
        using var shortCall = deadlines.Start(TimeSpan.FromSeconds(4));
        using var survivor = deadlines.Start(TimeSpan.FromSeconds(30));
        completed.Dispose();

        clock.Advance(TimeSpan.FromSeconds(2));
        _ = await Assert.That(shortCall.IsCancellationRequested).IsFalse();
        clock.Advance(TimeSpan.FromSeconds(2));
        _ = await Assert.That(shortCall.IsCancellationRequested).IsTrue();
        _ = await Assert.That(longCall.IsCancellationRequested).IsFalse();
        clock.Advance(TimeSpan.FromSeconds(11));
        _ = await Assert.That(longCall.IsCancellationRequested).IsTrue();
        _ = await Assert.That(survivor.IsCancellationRequested).IsFalse();
        await closed.CancelAsync();
        _ = await Assert.That(survivor.IsCancellationRequested).IsTrue();
        _ = await Assert.That(completedToken.IsCancellationRequested).IsFalse();
    }

    /// <summary>Connection shutdown allows a request to release itself from its callback and cancels later requests immediately.</summary>
    [Test]
    public async Task CloseReachesExistingAndLaterCalls()
    {
        using var closed = new CancellationTokenSource();
        using var deadlines = new BinaryRequestDeadlines(new FakeTimeProvider(), closed.Token);
        using var active = deadlines.Start(TimeSpan.FromMinutes(1));
        var token = active.Token;
        var callbacks = 0;
        await using var registration = token.Register(() =>
        {
            _ = Interlocked.Increment(ref callbacks);
            active.Dispose();
        });

        await closed.CancelAsync();
        using var late = deadlines.Start(TimeSpan.FromMinutes(1));
        _ = await Assert.That(callbacks).IsEqualTo(1);
        _ = await Assert.That(token.IsCancellationRequested).IsTrue();
        _ = await Assert.That(late.Token.IsCancellationRequested).IsTrue();
    }

    /// <summary>A failing callback leaves the remaining requests eligible for cancellation.</summary>
    [Test]
    public async Task CloseVisitsSiblingAfterCallbackThrows()
    {
        using var closed = new CancellationTokenSource();
        using var deadlines = new BinaryRequestDeadlines(new FakeTimeProvider(), closed.Token);
        using var first = deadlines.Start(TimeSpan.FromMinutes(1));
        using var second = deadlines.Start(TimeSpan.FromMinutes(1));
        var reached = 0;
        await using var good = first.Token.Register(() => Interlocked.Increment(ref reached));
        var expected = new InvalidOperationException("Callback failure");
        await using var bad = second.Token.Register(() => throw expected);
        AggregateException? seen = null;
        try
        {
            await closed.CancelAsync();
        }
        catch (AggregateException error)
        {
            seen = error.Flatten();
        }

        _ = await Assert.That(seen?.InnerExceptions.Count).IsEqualTo(1);
        _ = await Assert.That(seen?.InnerExceptions[0]).IsEqualTo(expected);
        _ = await Assert.That(reached).IsEqualTo(1);
        _ = await Assert.That(first.IsCancellationRequested).IsTrue();
        _ = await Assert.That(second.IsCancellationRequested).IsTrue();
    }

    /// <summary>The last outstanding request still observes a connection close after its timer is disposed.</summary>
    [Test]
    public async Task CloseStillReachesCallAfterAlarmDispose()
    {
        using var closed = new CancellationTokenSource();
        using var deadlines = new BinaryRequestDeadlines(new FakeTimeProvider(), closed.Token);
        using var finished = deadlines.Start(TimeSpan.FromSeconds(1));
        using var active = deadlines.Start(TimeSpan.FromSeconds(1));
        var detached = finished.Token;
        finished.Dispose();
        deadlines.Dispose();
        await closed.CancelAsync();
        _ = await Assert.That(active.Token.IsCancellationRequested).IsTrue();
        _ = await Assert.That(detached.IsCancellationRequested).IsFalse();
    }
}
