using System;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.Storage.Manifest;
using Squirix.Server.TestKit;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Persistence.Journaling;

/// <summary>The hand-off of segments deleted by retention to the journal thread.</summary>
[Immutable]
public sealed class JournalCapacityResyncSignalTests
{
    /// <summary>Everything recorded since the last take is handed over once.</summary>
    [Test]
    public async Task TakeReturnsRecordedTotalsOnce()
    {
        var signal = new JournalCapacityResyncSignal();
        signal.RecordDeleted(2, 300L);
        signal.RecordDeleted(1, 50L);

        _ = await Assert.That(signal.TryTake(out var segments, out var bytes)).IsTrue();
        _ = await Assert.That(segments).IsEqualTo(3);
        _ = await Assert.That(bytes).IsEqualTo(350L);
        _ = await Assert.That(signal.TryTake(out _, out _)).IsFalse();
    }

    /// <summary>A segment whose length could not be read still counts as freed, with zero bytes.</summary>
    [Test]
    public async Task UnknownLengthStillFreesSegment()
    {
        var signal = new JournalCapacityResyncSignal();
        signal.RecordDeleted(1, 0L);

        _ = await Assert.That(signal.TryTake(out var segments, out var bytes)).IsTrue();
        _ = await Assert.That(segments).IsEqualTo(1);
        _ = await Assert.That(bytes).IsEqualTo(0L);
    }

    /// <summary>A record wakes the journal thread after the totals are visible to it.</summary>
    [Test]
    public async Task RecordWakesAfterTotalsAreVisible()
    {
        var signal = new JournalCapacityResyncSignal();
        var taken = false;
        var seenSegments = 0;
        long seenBytes = 0;
        signal.AttachWake(() => taken = signal.TryTake(out seenSegments, out seenBytes));

        signal.RecordDeleted(2, 10L);

        _ = await Assert.That(taken).IsTrue();
        _ = await Assert.That(seenSegments).IsEqualTo(2);
        _ = await Assert.That(seenBytes).IsEqualTo(10L);
    }

    /// <summary>A record made before the wake is attached stays pending for the first take.</summary>
    [Test]
    public async Task RecordWithoutWakeStaysPending()
    {
        var signal = new JournalCapacityResyncSignal();
        signal.RecordDeleted(1, 5L);
        var woken = 0;
        signal.AttachWake(() => woken++);

        _ = await Assert.That(woken).IsEqualTo(0);
        _ = await Assert.That(signal.TryTake(out var segments, out _)).IsTrue();
        _ = await Assert.That(segments).IsEqualTo(1);
    }

    /// <summary>A record must name at least one segment and no negative length.</summary>
    [Test]
    public async Task RecordRejectsInvalidTotals()
    {
        var signal = new JournalCapacityResyncSignal();

        var noSegments = NodeExceptionAssert.For<ArgumentOutOfRangeException>().Throws(signal, static s => s.RecordDeleted(0, 1L));
        var negative = NodeExceptionAssert.For<ArgumentOutOfRangeException>().Throws(signal, static s => s.RecordDeleted(1, -1L));

        _ = await Assert.That(noSegments).IsNotNull();
        _ = await Assert.That(negative).IsNotNull();
    }
}
