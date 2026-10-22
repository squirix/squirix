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
    private const int NothingApplied = 0;
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
    public async Task ModelRejectsEarlierTermCommitTrace()
        => _ = await Assert.That(ExploreRunner.AcceptsCommitTrace(CommitTrace(SecondTerm, SecondSlot, FirstTerm, Everyone, Majority))).IsFalse();

    [Test]
    public async Task ModelRejectsLeaderSwitchTrace()
    {
        ModelCommitTracePoint[] trace =
        [
            new(FirstTerm, FirstIndex, 0, NothingApplied, FirstSlot, FirstTerm, Everyone),
            new(FirstTerm, FirstIndex, FirstIndex, NothingApplied, SecondSlot, FirstTerm, Everyone, Majority),
        ];

        _ = await Assert.That(ExploreRunner.AcceptsCommitTrace(trace)).IsFalse();
    }

    [Test]
    public async Task ModelRejectsMinorityCommitTrace()
        => _ = await Assert.That(ExploreRunner.AcceptsCommitTrace(CommitTrace(FirstTerm, FirstSlot, FirstTerm, Everyone, Alone))).IsFalse();

    [Test]
    public async Task ModelRejectsMinorityLeaderTrace()
        => _ = await Assert.That(ExploreRunner.AcceptsCommitTrace(CommitTrace(SecondTerm, SecondSlot, SecondTerm, Alone, Majority))).IsFalse();

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
            new(FirstTerm, FirstIndex, 0, NothingApplied, FirstSlot, FirstTerm, Everyone),
            new(SecondTerm, FirstIndex, 0, NothingApplied, SecondSlot, FirstTerm, Majority),
            new(FirstTerm, FirstIndex, FirstIndex, NothingApplied, FirstSlot, FirstTerm, Everyone, Alone),
        ];

        _ = await Assert.That(ExploreRunner.AcceptsCommitTrace(trace)).IsFalse();
    }

    private static ModelCommitTracePoint[] CommitTrace(int term, int leader, int lastLogTerm, int votes, int acks) =>
    [
        new(term, FirstIndex, 0, NothingApplied, leader, lastLogTerm, votes),
        new(term, FirstIndex, FirstIndex, NothingApplied, leader, lastLogTerm, votes, acks),
        new(term, FirstIndex, FirstIndex, FirstIndex, leader, lastLogTerm, votes),
    ];

    private static ModelCommitTracePoint[] ReadTrace(int readIndex, int readAcks) =>
    [
        new(FirstTerm, FirstIndex, 0, NothingApplied, FirstSlot, FirstTerm, Everyone),
        new(FirstTerm, FirstIndex, FirstIndex, NothingApplied, FirstSlot, FirstTerm, Everyone, Majority),
        new(FirstTerm, FirstIndex, FirstIndex, FirstIndex, FirstSlot, FirstTerm, Everyone),
        new(FirstTerm, FirstIndex, FirstIndex, FirstIndex, FirstSlot, FirstTerm, Everyone, NotObserved, readIndex, readAcks),
    ];
}
