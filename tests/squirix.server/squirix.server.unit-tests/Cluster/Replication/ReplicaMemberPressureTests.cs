using System;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Time.Testing;
using Squirix.Server.Attributes;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.TestKit;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Cluster.Replication;

/// <summary>A leader keeps the time of the last memory pressure flag of every member of its group, which lapses after its lifetime.</summary>
[Immutable]
public sealed class ReplicaMemberPressureTests : ServerUnitTestBase
{
    private static readonly TimeSpan Lifetime = TimeSpan.FromSeconds(1);

    /// <summary>No member is flagged before the first reply.</summary>
    [Test]
    public async Task StartsEmpty()
    {
        var pressure = Create(new FakeTimeProvider());

        _ = await Assert.That(pressure.IsAnyFresh()).IsFalse();
        _ = await Assert.That(pressure.NeedsRefresh()).IsFalse();
    }

    /// <summary>A flagged reply makes the group pressured until a reply of the same member clears it.</summary>
    [Test]
    public async Task ReplyWithoutFlagClears()
    {
        var pressure = Create(new FakeTimeProvider());

        pressure.Observe(1, true);
        var flagged = pressure.IsAnyFresh();
        pressure.Observe(1, false);

        _ = await Assert.That(flagged).IsTrue();
        _ = await Assert.That(pressure.IsAnyFresh()).IsFalse();
    }

    /// <summary>A flag lapses after its lifetime, so a member that stopped replying does not block the group.</summary>
    [Test]
    public async Task FlagLapsesAfterLifetime()
    {
        var clock = new FakeTimeProvider();
        var pressure = Create(clock);
        pressure.Observe(2, true);

        clock.Advance(Lifetime - TimeSpan.FromMilliseconds(1));
        var beforeLapse = pressure.IsAnyFresh();
        clock.Advance(TimeSpan.FromMilliseconds(1));

        _ = await Assert.That(beforeLapse).IsTrue();
        _ = await Assert.That(pressure.IsAnyFresh()).IsFalse();
    }

    /// <summary>A repeated flagged reply renews the flag.</summary>
    [Test]
    public async Task FlaggedReplyRenewsFlag()
    {
        var clock = new FakeTimeProvider();
        var pressure = Create(clock);
        pressure.Observe(1, true);
        clock.Advance(TimeSpan.FromMilliseconds(800));

        pressure.Observe(1, true);
        clock.Advance(TimeSpan.FromMilliseconds(800));

        _ = await Assert.That(pressure.IsAnyFresh()).IsTrue();
    }

    /// <summary>The flag of a slot is independent: clearing one member keeps the other flagged, and a lapsed one does not hide a fresh one.</summary>
    [Test]
    public async Task FlagsAreKeptPerSlot()
    {
        var clock = new FakeTimeProvider();
        var pressure = Create(clock);
        pressure.Observe(1, true);
        clock.Advance(TimeSpan.FromMilliseconds(600));
        pressure.Observe(2, true);

        pressure.Observe(1, false);
        var otherStays = pressure.IsAnyFresh();
        clock.Advance(Lifetime);
        var otherLapsed = pressure.IsAnyFresh();

        _ = await Assert.That(otherStays).IsTrue();
        _ = await Assert.That(otherLapsed).IsFalse();
    }

    /// <summary>A flag asks for a refresh once it is older than half its lifetime, and a younger flag of another slot defers it.</summary>
    [Test]
    public async Task OldFlagNeedsRefresh()
    {
        var clock = new FakeTimeProvider();
        var pressure = Create(clock);
        pressure.Observe(1, true);

        clock.Advance(TimeSpan.FromMilliseconds(499));
        var young = pressure.NeedsRefresh();
        clock.Advance(TimeSpan.FromMilliseconds(1));
        var half = pressure.NeedsRefresh();
        pressure.Observe(2, true);
        var otherYoung = pressure.NeedsRefresh();
        clock.Advance(Lifetime);

        _ = await Assert.That((young, half, otherYoung)).IsEqualTo((false, true, false));
        _ = await Assert.That(pressure.NeedsRefresh()).IsFalse().Because("A lapsed flag needs no refresh: the group fails open.");
    }

    /// <summary>A reply from a slot outside the group is ignored.</summary>
    [Test]
    public async Task SlotOutsideGroupIsIgnored()
    {
        var pressure = Create(new FakeTimeProvider());

        pressure.Observe(-1, true);
        pressure.Observe(PolicyOptions.MaxReplicaCount, true);

        _ = await Assert.That(pressure.IsAnyFresh()).IsFalse();
    }

    /// <summary>A lifetime that is not positive is refused.</summary>
    [Test]
    public Task NonPositiveLifetimeIsRefused()
    {
        _ = NodeExceptionAssert.For<ArgumentOutOfRangeException>().Throws(
            new FakeTimeProvider(),
            static clock => _ = new ReplicaMemberPressure("n1", TimeSpan.Zero, clock, NullLogger.Instance));
        return Task.CompletedTask;
    }

    private static ReplicaMemberPressure Create(FakeTimeProvider clock) => new("n1", Lifetime, clock, NullLogger.Instance);
}
