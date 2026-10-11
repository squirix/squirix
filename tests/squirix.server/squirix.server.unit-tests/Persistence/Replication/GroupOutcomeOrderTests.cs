using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Microsoft.Extensions.Time.Testing;
using Squirix.Server.Storage.Replication;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Persistence.Replication;

/// <summary>The order of resolved group outcomes pops expired heads only and bounds its stale entries.</summary>
public sealed class GroupOutcomeOrderTests : ServerUnitTestBase
{
    private static readonly TimeSpan Retention = TimeSpan.FromMinutes(10);

    /// <summary>Outcomes inserted out of birth order expire oldest first.</summary>
    [Test]
    public async Task OutOfOrderInsertsExpireOldestFirst()
    {
        var table = new Table(Retention);
        table.Put("young", TimeSpan.FromMinutes(1));
        table.Put("oldest", TimeSpan.FromMinutes(5));
        table.Put("middle", TimeSpan.FromMinutes(3));
        table.Clock.Advance(TimeSpan.FromMinutes(4));
        var none = table.Order.TryTakeExpired(out _);
        table.Clock.Advance(TimeSpan.FromMinutes(1));

        _ = await Assert.That(none).IsFalse();
        _ = await Assert.That(table.Order.TryTakeExpired(out var first)).IsTrue();
        _ = await Assert.That(first).IsEqualTo("oldest");
        _ = await Assert.That(table.Order.TryTakeExpired(out _)).IsFalse();
        table.Clock.Advance(TimeSpan.FromMinutes(2));
        _ = await Assert.That(table.Order.TryTakeExpired(out var second)).IsTrue();
        _ = await Assert.That(second).IsEqualTo("middle");
        _ = await Assert.That(table.Order.TryTakeExpired(out _)).IsFalse();
        table.Clock.Advance(TimeSpan.FromMinutes(3));
        _ = await Assert.That(table.Order.TryTakeExpired(out var last)).IsTrue();
        _ = await Assert.That(last).IsEqualTo("young");
    }

    /// <summary>A removed or replaced record leaves an entry that is skipped, not reported.</summary>
    [Test]
    public async Task StaleEntriesAreSkipped()
    {
        var table = new Table(Retention);
        table.Put("removed", TimeSpan.FromMinutes(2));
        table.Put("replaced", TimeSpan.FromMinutes(2));
        table.Put("kept", TimeSpan.Zero);
        table.Remove("removed");
        table.Put("replaced", TimeSpan.Zero);
        table.Clock.Advance(Retention - TimeSpan.FromMinutes(1));

        _ = await Assert.That(table.Order.TryTakeExpired(out _)).IsFalse();
        _ = await Assert.That(table.Order.EntryCount).IsEqualTo(2);
        table.Clock.Advance(TimeSpan.FromMinutes(1));
        _ = await Assert.That(table.Order.TryTakeExpired(out var first)).IsTrue();
        _ = await Assert.That(table.Order.TryTakeExpired(out var second)).IsTrue();
        _ = await Assert.That(table.Order.TryTakeExpired(out _)).IsFalse();
        var pair = $"{first}+{second}";
        _ = await Assert.That(string.Equals(pair, "kept+replaced", StringComparison.Ordinal) || string.Equals(pair, "replaced+kept", StringComparison.Ordinal)).IsTrue();
    }

    /// <summary>Compaction leaves only live entries once the stale ones pass the bound, and not before.</summary>
    [Test]
    public async Task CompactionBoundsStaleEntries()
    {
        var table = new Table(Retention);
        table.Put("live", TimeSpan.Zero);
        for (var i = 0; i < 200; i++)
            table.Put("churn", TimeSpan.Zero);

        table.Order.Compact(2);
        var entries = table.Order.EntryCount;
        var below = new Table(Retention);
        below.Put("live", TimeSpan.Zero);
        below.Put("live", TimeSpan.Zero);
        below.Order.Compact(1);

        _ = await Assert.That(entries).IsEqualTo(2);
        _ = await Assert.That(below.Order.EntryCount).IsEqualTo(2);
        table.Clock.Advance(Retention);
        _ = await Assert.That(table.Order.TryTakeExpired(out _)).IsTrue();
        _ = await Assert.That(table.Order.TryTakeExpired(out _)).IsTrue();
        _ = await Assert.That(table.Order.TryTakeExpired(out _)).IsFalse();
    }

    /// <summary>With unbounded retention nothing is tracked and nothing expires.</summary>
    [Test]
    public async Task UnboundedRetentionTracksNothing()
    {
        var table = new Table(TimeSpan.MaxValue);
        table.Put("a", TimeSpan.FromDays(30));
        table.Clock.Advance(TimeSpan.FromDays(365));

        _ = await Assert.That(table.Order.EntryCount).IsEqualTo(0);
        _ = await Assert.That(table.Order.TryTakeExpired(out _)).IsFalse();
    }

    /// <summary>Zero retention expires a record on the next take.</summary>
    [Test]
    public async Task ZeroRetentionExpiresAtOnce()
    {
        var table = new Table(TimeSpan.Zero);
        table.Put("a", TimeSpan.Zero);

        _ = await Assert.That(table.Order.TryTakeExpired(out var key)).IsTrue();
        _ = await Assert.That(key).IsEqualTo("a");
    }

    private sealed class Table
    {
        private readonly Dictionary<string, (long Sequence, long Anchor, TimeSpan Age)> _live;

        internal Table(TimeSpan retention)
        {
            _live = [with(StringComparer.Ordinal)];
            Clock = new FakeTimeProvider(new DateTimeOffset(2000, 1, 1, 0, 0, 0, TimeSpan.Zero));
            Order = new GroupOutcomeOrder<string>(Clock, retention, Lookup);
        }

        internal FakeTimeProvider Clock { get; }

        internal GroupOutcomeOrder<string> Order { get; }

        internal void Put(string key, TimeSpan ageAtAnchor)
        {
            var sequence = Order.NextSequence();
            var anchor = Clock.GetTimestamp();
            Order.Add(key, sequence, anchor, ageAtAnchor);
            _live[key] = (sequence, anchor, ageAtAnchor);
        }

        internal void Remove(string key) => _ = _live.Remove(key);

        private bool Lookup(string key, long sequence, out TimeSpan age)
        {
            var live = _live.TryGetValue(key, out var entry) && entry.Sequence == sequence;
            age = live ? entry.Age + Clock.GetElapsedTime(entry.Anchor) : TimeSpan.Zero;
            return live;
        }
    }
}
