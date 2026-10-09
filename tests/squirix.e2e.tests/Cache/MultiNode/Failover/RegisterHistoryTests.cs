using System;
using System.Threading.Tasks;
using Squirix.E2ETests.Fixtures;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.E2ETests.Cache.MultiNode.Failover;

/// <summary>The register history checker accepts linearizable single-writer histories and names each kind of violation.</summary>
public sealed class RegisterHistoryTests : EndToEndTestBase
{
    private const string Key = "k";

    /// <summary>Reads that overlap writes may see either the old or the new value, and the not-found register before the first write.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task ConcurrentReadsMaySeeOldOrNew()
    {
        RegisterWrite[] writes = [Write(1, 10, 20), Write(2, 30, 40)];
        RegisterRead[] reads = [Read(0, 15, 0), Read(5, 25, 1), Read(32, 38, 1), Read(33, 39, 2), Read(41, 50, 2)];

        _ = await Assert.That(RegisterHistory.Check(writes, reads)).IsEmpty();
    }

    /// <summary>A read that starts after a write was acknowledged and sees an older value has lost that write.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task StaleReadAfterAckIsViolation()
    {
        RegisterWrite[] writes = [Write(1, 10, 20), Write(2, 30, 40)];
        RegisterRead[] reads = [Read(41, 50, 1)];

        var violations = RegisterHistory.Check(writes, reads);

        _ = await Assert.That(violations).HasSingleItem();
        _ = await Assert.That(violations[0]).Contains("below 2, acknowledged before the read started", StringComparison.Ordinal);
    }

    /// <summary>A read that sees the value of a write started after the read returned has seen the future.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task ReadOfLaterWriteIsViolation()
    {
        RegisterWrite[] writes = [Write(1, 10, 20), Write(2, 30, 40)];
        RegisterRead[] reads = [Read(21, 25, 2)];

        var violations = RegisterHistory.Check(writes, reads);

        _ = await Assert.That(violations).HasSingleItem();
        _ = await Assert.That(violations[0]).Contains("a value whose write started after the read returned", StringComparison.Ordinal);
    }

    /// <summary>A read that sees a value no write wrote is a violation.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task UnwrittenValueIsViolation()
    {
        RegisterWrite[] writes = [Write(1, 10, 20)];
        RegisterRead[] reads = [Read(21, 25, 7)];

        var violations = RegisterHistory.Check(writes, reads);

        _ = await Assert.That(violations).HasSingleItem();
        _ = await Assert.That(violations[0]).Contains("a value no write wrote", StringComparison.Ordinal);
    }

    /// <summary>A failed write may be seen or not, but once a read saw it, no later read may go back to the acknowledged value before it.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task AmbiguousWriteMayBeSeenOnce()
    {
        RegisterWrite[] writes = [Write(1, 10, 20), Write(2, 30, 40, false)];
        RegisterRead[] clean = [Read(45, 50, 1), Read(51, 55, 2), Read(56, 60, 2)];
        RegisterRead[] backwards = [Read(45, 50, 2), Read(51, 55, 1)];

        var violations = RegisterHistory.Check(writes, backwards);

        _ = await Assert.That(RegisterHistory.Check(writes, clean)).IsEmpty();
        _ = await Assert.That(violations).HasSingleItem();
        _ = await Assert.That(violations[0]).Contains("below 2, seen by the read [45, 50]", StringComparison.Ordinal);
    }

    /// <summary>
    /// A failed write never completed, so it may take effect after the next acknowledged write: a read may see it after a read saw the
    /// newer value.
    /// </summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task AmbiguousWriteMayTakeEffectLate()
    {
        RegisterWrite[] writes = [Write(1, 10, 20), Write(2, 30, 40, false), Write(3, 50, 60)];
        RegisterRead[] reads = [Read(65, 70, 3), Read(75, 80, 2), Read(85, 90, 2)];

        _ = await Assert.That(RegisterHistory.Check(writes, reads)).IsEmpty();
    }

    /// <summary>An acknowledged value read after a newer write was acknowledged is a lost write, even with an ambiguous write in between.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task LostAckedWriteIsViolation()
    {
        RegisterWrite[] writes = [Write(1, 10, 20), Write(2, 30, 40, false), Write(3, 50, 60)];
        RegisterRead[] reads = [Read(65, 70, 1), Read(75, 80, 0)];

        var violations = RegisterHistory.Check(writes, reads);

        _ = await Assert.That(violations.Count).IsEqualTo(3);
        _ = await Assert.That(violations[0]).Contains("saw 1, a value below 3, acknowledged before the read started", StringComparison.Ordinal);
        _ = await Assert.That(violations[1]).Contains("saw 0, a value below 3, acknowledged before the read started", StringComparison.Ordinal);
        _ = await Assert.That(violations[2]).Contains("saw 0, a value below 1, seen by the read [65, 70]", StringComparison.Ordinal);
    }

    /// <summary>
    /// Equal timestamps overlap: a write that ended when a read started, a write that started when a read ended, and a read that ended
    /// when the next one started order nothing; one tick later they do.
    /// </summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task EqualTimestampsDoNotOrderCalls()
    {
        RegisterWrite[] first = [Write(1, 10, 20)];
        RegisterWrite[] writes = [Write(1, 10, 20), Write(2, 30, 60)];
        RegisterRead[] adjacent = [Read(35, 50, 2), Read(50, 55, 1)];
        RegisterRead[] apart = [Read(35, 50, 2), Read(51, 55, 1)];

        _ = await Assert.That(RegisterHistory.Check(first, [Read(20, 25, 0)])).IsEmpty();
        _ = await Assert.That(RegisterHistory.Check(first, [Read(5, 10, 1)])).IsEmpty();
        _ = await Assert.That(RegisterHistory.Check(first, [Read(21, 25, 0)])).HasSingleItem();
        _ = await Assert.That(RegisterHistory.Check(first, [Read(5, 9, 1)])).HasSingleItem();
        _ = await Assert.That(RegisterHistory.Check(writes, adjacent)).IsEmpty();
        _ = await Assert.That(RegisterHistory.Check(writes, apart)).HasSingleItem();
    }

    /// <summary>Overlapping reads may see decreasing values: the later-starting read may linearize first.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task OverlappingReadsMayDecrease()
    {
        RegisterWrite[] writes = [Write(1, 10, 20), Write(2, 30, 40)];
        RegisterRead[] reads = [Read(32, 45, 2), Read(35, 38, 1)];

        _ = await Assert.That(RegisterHistory.Check(writes, reads)).IsEmpty();
    }

    /// <summary>Writes of one key that do not increase, or that overlap, break the single-writer contract and stop the check of that key.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task BrokenWriterContractIsReported()
    {
        RegisterWrite[] writes = [Write(2, 10, 20), Write(1, 30, 40)];
        RegisterRead[] reads = [Read(41, 50, 9)];

        var violations = RegisterHistory.Check(writes, reads);

        _ = await Assert.That(violations).HasSingleItem();
        _ = await Assert.That(violations[0]).Contains("breaks the single writer contract", StringComparison.Ordinal);
    }

    /// <summary>The recorded history is checked per key, so the writes of one key never excuse the reads of another.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task RecordedHistoryIsCheckedPerKey()
    {
        var history = new RegisterHistory();
        history.RecordWrite(new RegisterWrite("a", 1, 10, 20, true));
        history.RecordWrite(new RegisterWrite("b", 1, 10, 20, false));
        history.RecordRead(new RegisterRead("a", 25, 30, 1));
        history.RecordRead(new RegisterRead("b", 25, 30, 0));
        history.RecordRead(new RegisterRead("c", 25, 30, 1));
        history.RecordFailedRead();

        var violations = history.Check();

        _ = await Assert.That(violations).HasSingleItem();
        _ = await Assert.That(violations[0]).StartsWith("key c:", StringComparison.Ordinal);
        _ = await Assert.That((history.AmbiguousWrites, history.FailedReads)).IsEqualTo((1, 1));
        _ = await Assert.That(history.Summary()).IsEqualTo("1 acknowledged and 1 failed writes, 3 successful and 1 failed reads");
    }

    /// <summary>Only acknowledged writes and successful reads that started at or after the point in time are counted.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task CountsCallsStartedAfterPoint()
    {
        var history = new RegisterHistory();
        history.RecordWrite(Write(1, 10, 20));
        history.RecordWrite(Write(2, 30, 40));
        history.RecordWrite(Write(3, 50, 60, false));
        history.RecordRead(Read(29, 35, 1));
        history.RecordRead(Read(30, 45, 2));
        history.RecordFailedRead();

        _ = await Assert.That(history.StartedAfter(30)).IsEqualTo((1, 1));
    }

    private static RegisterRead Read(long start, long end, long observed) => new(Key, start, end, observed);

    private static RegisterWrite Write(long value, long start, long end, bool acked = true) => new(Key, value, start, end, acked);
}
