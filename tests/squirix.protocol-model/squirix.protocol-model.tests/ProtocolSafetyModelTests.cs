using Xunit;

namespace Squirix.ProtocolModel.Tests;

public static class ProtocolSafetyModelTests
{
    [Fact]
    public static void StaticMembershipElectsAtMostOneLeaderPerTerm()
    {
        var result = ExploreRunner.Run(ExploreProfile.SmallElection(), BrokenMode.None);
        Assert.True(result.FixedPointReached);
        Assert.Null(result.Violation);
        Assert.True(result.StatesVisited > 1);
    }

    [Fact]
    public static void CommittedEntrySurvivesFutureLeaderSelection()
    {
        var result = ExploreRunner.Run(ExploreProfile.SmallCommit(), BrokenMode.None);
        Assert.True(result.FixedPointReached);
        Assert.Null(result.Violation);
    }

    [Fact]
    public static void OldTermEntryNeedsCurrentTermCommit()
    {
        var safe = ExploreRunner.Run(ExploreProfile.SmallCommit(), BrokenMode.None);
        Assert.True(safe.FixedPointReached);
        Assert.Null(safe.Violation);

        var broken = ExploreRunner.Run(ExploreProfile.SmallCommit(), BrokenMode.CurrentTermCommit);
        Assert.NotNull(broken.Violation);
        Assert.Equal("CurrentTermCommit", broken.Violation.Invariant);
    }

    [Fact]
    public static void QuorumReadRequiresCurrentTermMajority()
    {
        var safe = ExploreRunner.Run(ExploreProfile.SmallRead(), BrokenMode.None);
        Assert.True(safe.FixedPointReached);
        Assert.Null(safe.Violation);

        var broken = ExploreRunner.Run(ExploreProfile.SmallRead(), BrokenMode.ReadIndex);
        Assert.NotNull(broken.Violation);
        Assert.Equal("ReadIndex", broken.Violation.Invariant);
    }
}
