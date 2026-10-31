using System;
using System.Threading.Tasks;
using Squirix.Attributes;
using Squirix.Internal.Cluster.Observability;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.UnitTests;

/// <summary>Covers monotonic operation budgets and forwarded transport deadlines.</summary>
[Immutable]
public sealed class DeadlineBudgetTests
{
    /// <summary>Wall-clock corrections do not renew or consume the monotonic budget.</summary>
    /// <param name="wallClockHours">The wall-clock correction in hours.</param>
    [Test]
    [Arguments(-2)]
    [Arguments(2)]
    public async Task WallClockStepsPreserveBudget(int wallClockHours)
    {
        var initial = new DateTimeOffset(2026, 1, 1, 0, 0, 0, TimeSpan.Zero);
        var clock = new AdjustableClock(initial);
        var budget = DeadlineBudget.Start(initial.UtcDateTime.AddSeconds(30), clock);
        clock.AdvanceTimestamp(TimeSpan.FromSeconds(12));
        clock.SetUtcNow(initial.AddHours(wallClockHours));

        _ = await Assert.That(budget.Remaining).IsEqualTo(TimeSpan.FromSeconds(18));
        _ = await Assert.That(budget.ForwardDeadlineUtc).IsEqualTo(clock.GetUtcNow().UtcDateTime.AddSeconds(18));
    }

    /// <summary>A copied budget keeps its original start when passed to another attempt.</summary>
    [Test]
    public async Task CopiedBudgetKeepsElapsedTime()
    {
        var clock = new AdjustableClock(new DateTimeOffset(2026, 1, 1, 0, 0, 0, TimeSpan.Zero));
        var budget = DeadlineBudget.Start(clock.GetUtcNow().UtcDateTime.AddSeconds(30), clock);
        clock.AdvanceTimestamp(TimeSpan.FromSeconds(12));
        var nextAttempt = budget;
        clock.AdvanceTimestamp(TimeSpan.FromSeconds(20));

        _ = await Assert.That(nextAttempt.Remaining).IsEqualTo(TimeSpan.FromSeconds(-2));
        _ = await Assert.That(nextAttempt.ForwardDeadlineUtc).IsEqualTo(clock.GetUtcNow().UtcDateTime);
    }

    private sealed class AdjustableClock : TimeProvider
    {
        private DateTimeOffset _utcNow;
        private long _timestamp;

        internal AdjustableClock(DateTimeOffset utcNow)
        {
            _utcNow = utcNow;
        }

        public override long TimestampFrequency => TimeSpan.TicksPerSecond;

        public override DateTimeOffset GetUtcNow() => _utcNow;

        public override long GetTimestamp() => _timestamp;

        internal void AdvanceTimestamp(TimeSpan elapsed) => _timestamp += elapsed.Ticks;

        internal void SetUtcNow(DateTimeOffset utcNow) => _utcNow = utcNow;
    }
}
