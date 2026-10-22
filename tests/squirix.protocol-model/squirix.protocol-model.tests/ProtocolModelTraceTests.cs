using System.Threading.Tasks;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.ProtocolModel.Tests;

public sealed class ProtocolModelTraceTests
{
    private const int FirstTerm = 1;
    private const int SecondTerm = 2;
    private const int FirstSlot = 0;
    private const int SecondSlot = 1;
    private const int Everyone = 3;
    private const int Majority = 2;
    private const int Alone = 1;
    private const int FirstIndex = 1;
    private const int SecondIndex = 2;
    private const int RecoveredTailMaxInFlight = 1;
    private const int RecoveredTailStateBudget = 300_000;
    private const int NothingApplied = 0;
    private const int NothingCommitted = 0;
    private const int WrongReadIndex = 2;
    private const int NotObserved = ModelCommitTracePoint.NotObserved;

    [Test]
    public async Task ModelAcceptsMajorityCommitTrace()
    {
        _ = await Assert.That(ExploreRunner.AcceptsCommitTrace(CommitTrace(FirstTerm, FirstSlot, FirstTerm, Everyone, Majority))).IsTrue();
        _ = await Assert.That(ExploreRunner.AcceptsCommitTrace(CommitTrace(FirstTerm, FirstSlot, FirstTerm, Everyone, Everyone))).IsTrue();
    }

    [Test]
    public async Task ModelAcceptsMajorityReadTrace()
        => _ = await Assert.That(ExploreRunner.AcceptsCommitTrace(ReadTrace(FirstIndex, Majority))).IsTrue();

    [Test]
    public async Task ModelAcceptsOldShapeCommitTrace()
    {
        ModelCommitTracePoint[] trace =
        [
            new(FirstTerm, FirstIndex, NothingCommitted, NothingApplied),
            new(FirstTerm, FirstIndex, FirstIndex, NothingApplied),
            new(FirstTerm, FirstIndex, FirstIndex, FirstIndex),
        ];

        _ = await Assert.That(ExploreRunner.AcceptsCommitTrace(trace)).IsTrue();
    }

    [Test]
    public async Task ModelAcceptsTraceWithoutAppliedIndex()
    {
        ModelCommitTracePoint[] trace =
        [
            new(FirstTerm, FirstIndex, NothingCommitted, NotObserved),
            new(FirstTerm, FirstIndex, FirstIndex, NotObserved, FirstSlot, FirstTerm, Everyone, Majority),
        ];

        _ = await Assert.That(ExploreRunner.AcceptsCommitTrace(trace)).IsTrue();
    }

    [Test]
    public async Task ModelRejectsWrongReadIndexTrace()
        => _ = await Assert.That(ExploreRunner.AcceptsCommitTrace(ReadTrace(WrongReadIndex, Majority))).IsFalse();

    [Test]
    public async Task ModelRejectsReadAcksWithoutReadTrace()
    {
        ModelCommitTracePoint[] trace =
        [
            new(FirstTerm, FirstIndex, NothingCommitted, NothingApplied, FirstSlot, FirstTerm, Everyone, NotObserved, NotObserved, Everyone),
        ];

        _ = await Assert.That(ExploreRunner.AcceptsCommitTrace(trace)).IsFalse();
    }

    [Test]
    public async Task ModelRejectsTwoAcksPointsOneCommit()
    {
        ModelCommitTracePoint[] trace =
        [
            new(FirstTerm, FirstIndex, NothingCommitted, NothingApplied, FirstSlot, FirstTerm, Everyone),
            new(FirstTerm, FirstIndex, FirstIndex, NothingApplied, FirstSlot, FirstTerm, Everyone, Majority),
            new(FirstTerm, FirstIndex, FirstIndex, NothingApplied, FirstSlot, FirstTerm, Everyone, Majority),
        ];

        _ = await Assert.That(ExploreRunner.AcceptsCommitTrace(trace)).IsFalse();
    }

    [Test]
    public async Task ModelRejectsEarlierTermCommitTrace()
        => _ = await Assert.That(ExploreRunner.AcceptsCommitTrace(CommitTrace(SecondTerm, SecondSlot, FirstTerm, Everyone, Majority))).IsFalse();

    [Test]
    public async Task ModelRejectsLeaderSwitchTrace()
    {
        ModelCommitTracePoint[] trace =
        [
            new(FirstTerm, FirstIndex, NothingCommitted, NothingApplied, FirstSlot, FirstTerm, Everyone),
            new(FirstTerm, FirstIndex, FirstIndex, NothingApplied, SecondSlot, FirstTerm, Everyone, Majority),
        ];

        _ = await Assert.That(ExploreRunner.AcceptsCommitTrace(trace)).IsFalse();
    }

    [Test]
    public async Task ModelRejectsMinorityCommitTrace()
        => _ = await Assert.That(ExploreRunner.AcceptsCommitTrace(CommitTrace(FirstTerm, FirstSlot, FirstTerm, Everyone, Alone))).IsFalse();

    [Test]
    public async Task ModelRejectsMinorityLeaderTrace()
        => _ = await Assert.That(ExploreRunner.AcceptsCommitTrace(CommitTrace(FirstTerm, FirstSlot, FirstTerm, Alone, Majority))).IsFalse();

    [Test]
    public async Task ModelRejectsMinorityReadTrace()
        => _ = await Assert.That(ExploreRunner.AcceptsCommitTrace(ReadTrace(FirstIndex, Alone))).IsFalse();

    [Test]
    public async Task ModelRejectsUnappliedReadTrace()
    {
        ModelCommitTracePoint[] trace =
        [
            new(FirstTerm, FirstIndex, FirstIndex, NothingApplied, FirstSlot, FirstTerm, Everyone, NotObserved, FirstIndex, Everyone),
        ];

        _ = await Assert.That(ExploreRunner.AcceptsCommitTrace(trace)).IsFalse();
    }

    [Test]
    public async Task ModelRejectsStaleLeaderCommitTrace()
    {
        ModelCommitTracePoint[] trace =
        [
            new(FirstTerm, FirstIndex, NothingCommitted, NothingApplied, FirstSlot, FirstTerm, Everyone),
            new(SecondTerm, FirstIndex, NothingCommitted, NothingApplied, SecondSlot, FirstTerm, Majority),
            new(FirstTerm, FirstIndex, FirstIndex, NothingApplied, FirstSlot, FirstTerm, Everyone, Alone),
        ];

        _ = await Assert.That(ExploreRunner.AcceptsCommitTrace(trace)).IsFalse();
    }

    [Test]
    public async Task ModelAcceptsTailCommitWithOwnTermEntry()
    {
        ModelCommitTracePoint[] trace =
        [
            new(SecondTerm, SecondIndex, NothingCommitted, NotObserved, FirstSlot, SecondTerm),
            new(SecondTerm, SecondIndex, SecondIndex, NotObserved, FirstSlot, SecondTerm, NotObserved, Majority),
        ];

        _ = await Assert.That(ExploreRunner.AcceptsCommitTrace(trace, RecoveredTailProfile())).IsTrue();
    }

    [Test]
    public async Task ModelAcceptsTailCommitInOwnTerm()
    {
        ModelCommitTracePoint[] trace =
        [
            new(FirstTerm, FirstIndex, NothingCommitted, NotObserved, FirstSlot, FirstTerm),
            new(FirstTerm, FirstIndex, FirstIndex, NotObserved, FirstSlot, FirstTerm, NotObserved, Majority),
        ];

        _ = await Assert.That(ExploreRunner.AcceptsCommitTrace(trace, RecoveredTailProfile())).IsTrue();
    }

    [Test]
    public async Task ModelRejectsEarlierTermTailTrace()
    {
        ModelCommitTracePoint[] trace =
        [
            new(SecondTerm, FirstIndex, NothingCommitted, NotObserved, FirstSlot, FirstTerm),
            new(SecondTerm, FirstIndex, FirstIndex, NotObserved, FirstSlot, FirstTerm, NotObserved, Majority),
        ];

        _ = await Assert.That(ExploreRunner.AcceptsCommitTrace(trace, RecoveredTailProfile())).IsFalse();
    }

    private static ExploreProfile RecoveredTailProfile() => ExploreProfile.ForLogRules(SecondTerm, SecondIndex, RecoveredTailMaxInFlight, RecoveredTailStateBudget);

    private static ModelCommitTracePoint[] CommitTrace(int term, int leader, int lastLogTerm, int votes, int acks) =>
    [
        new(term, FirstIndex, NothingCommitted, NothingApplied, leader, lastLogTerm, votes),
        new(term, FirstIndex, FirstIndex, NothingApplied, leader, lastLogTerm, votes, acks),
        new(term, FirstIndex, FirstIndex, FirstIndex, leader, lastLogTerm, votes),
    ];

    private static ModelCommitTracePoint[] ReadTrace(int readIndex, int readAcks) =>
    [
        new(FirstTerm, FirstIndex, NothingCommitted, NothingApplied, FirstSlot, FirstTerm, Everyone),
        new(FirstTerm, FirstIndex, FirstIndex, NothingApplied, FirstSlot, FirstTerm, Everyone, Majority),
        new(FirstTerm, FirstIndex, FirstIndex, FirstIndex, FirstSlot, FirstTerm, Everyone),
        new(FirstTerm, FirstIndex, FirstIndex, FirstIndex, FirstSlot, FirstTerm, Everyone, NotObserved, readIndex, readAcks),
    ];
}
