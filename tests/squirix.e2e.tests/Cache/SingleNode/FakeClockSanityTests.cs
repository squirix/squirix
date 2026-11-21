using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.TestKit;
using Squirix.Testing;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.E2ETests.Cache.SingleNode;

/// <summary>Sanity checks for FakeTimeProvider semantics used by expiration tests.</summary>
public sealed class FakeClockSanityTests
{
    /// <summary>Verifies Advance accumulates forward, rejects negative deltas, and SetUtcNow permits forward jumps only.</summary>
    [Test]
    public async Task AdvanceAndResetSemantics()
    {
        var f = new FakeTimeProvider();
        var start = f.GetUtcNow();

        f.Advance(TimeSpan.FromSeconds(1));
        _ = await Assert.That(f.GetUtcNow()).IsEqualTo(start.AddSeconds(1));

        // simulate accumulation across tests
        f.Advance(TimeSpan.FromSeconds(50));
        _ = await Assert.That(f.GetUtcNow()).IsEqualTo(start.AddSeconds(51));

        // time only moves forward through Advance
        _ = NodeExceptionAssert.For<ArgumentOutOfRangeException>().Throws(f, static clock => clock.Advance(TimeSpan.FromSeconds(-1)));

        // SetUtcNow jumps forward but also refuses to travel into the past
        f.SetUtcNow(start.AddMinutes(2));
        _ = await Assert.That(f.GetUtcNow()).IsEqualTo(start.AddMinutes(2));
        _ = NodeExceptionAssert.For<ArgumentOutOfRangeException>().Throws(f, static clock => clock.SetUtcNow(clock.GetUtcNow().AddSeconds(-1)));
    }

    /// <summary>Periodic callbacks catch up in deadline order until a one-shot callback cancels the schedule.</summary>
    /// <returns>The asynchronous assertion.</returns>
    [Test]
    public async Task AdvanceRunsDueTimersInOrder()
    {
        var clock = new FakeTimeProvider();
        var calls = new List<string>();
        var stopped = false;
        var second = TimeSpan.FromSeconds(1);
        await using var periodic = clock.CreateTimer(_ => calls.Add("tick"), null, second, second);
        await using var stop = clock.CreateTimer(
            _ =>
            {
                calls.Add("stop");
                stopped = periodic.Change(Timeout.InfiniteTimeSpan, Timeout.InfiniteTimeSpan);
            },
            null,
            TimeSpan.FromMilliseconds(2500),
            Timeout.InfiniteTimeSpan);

        clock.Advance(second * 5);

        _ = await Assert.That(stopped).IsTrue();
        _ = await Assert.That(string.Join(',', calls)).IsEqualTo("tick,tick,stop");
    }

    /// <summary>A periodic timer due at the final representable instant can fire and schedule its next tick.</summary>
    /// <returns>The asynchronous assertion.</returns>
    [Test]
    public async Task PeriodicTimerFiresAtLastInstant()
    {
        var tick = TimeSpan.FromTicks(1);
        var clock = new FakeTimeProvider(DateTimeOffset.MaxValue - tick);
        var calls = 0;
        await using var timer = clock.CreateTimer(_ => calls++, null, tick, tick);
        clock.Advance(tick);
        _ = await Assert.That(calls).IsEqualTo(1);
    }

    /// <summary>Zero-delay timers fire immediately and a disposed timer cannot be rearmed.</summary>
    /// <returns>The asynchronous assertions.</returns>
    [Test]
    public async Task DisposedTimerCannotBeRearmed()
    {
        var clock = new FakeTimeProvider();
        var calls = 0;
        var timer = clock.CreateTimer(_ => calls++, null, TimeSpan.Zero, Timeout.InfiniteTimeSpan);
        _ = await Assert.That(calls).IsEqualTo(1);

        _ = timer.Change(TimeSpan.FromSeconds(1), Timeout.InfiniteTimeSpan);
        await timer.DisposeAsync();
        _ = await Assert.That(timer.Change(TimeSpan.Zero, Timeout.InfiniteTimeSpan)).IsFalse();
        clock.Advance(TimeSpan.FromSeconds(1));
        _ = await Assert.That(calls).IsEqualTo(1);
    }
}
