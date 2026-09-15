using System.Threading.Tasks;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.ProtocolModel.Tests;

public sealed class ProtocolSafetyModelTests
{
    [Test]
    public async Task EntrySurvivesFutureLeaderSelection()
    {
        var result = ExploreRunner.Run(ExploreProfile.SmallCommit(), BrokenMode.None);
        _ = await Assert.That(result.FixedPointReached).IsTrue();
        _ = await Assert.That(result.Violation).IsNull();
    }

    [Test]
    public async Task MembershipElectsAtMostOneLeaderPerTerm()
    {
        var result = ExploreRunner.Run(ExploreProfile.SmallElection(), BrokenMode.None);
        _ = await Assert.That(result.FixedPointReached).IsTrue();
        _ = await Assert.That(result.Violation).IsNull();
        _ = await Assert.That(result.StatesVisited > 1).IsTrue();
    }

    [Test]
    public async Task OldTermEntryNeedsCurrentTermCommit()
    {
        var safe = ExploreRunner.Run(ExploreProfile.SmallCommit(), BrokenMode.None);
        _ = await Assert.That(safe.FixedPointReached).IsTrue();
        _ = await Assert.That(safe.Violation).IsNull();

        var broken = ExploreRunner.Run(ExploreProfile.SmallCommit(), BrokenMode.CurrentTermCommit);
        _ = await Assert.That(broken.Violation).IsNotNull();
        _ = await Assert.That(broken.Violation.Invariant).IsEqualTo("CurrentTermCommit");
    }

    [Test]
    public async Task QuorumReadRequiresCurrentTermMajority()
    {
        var safe = ExploreRunner.Run(ExploreProfile.SmallRead(), BrokenMode.None);
        _ = await Assert.That(safe.FixedPointReached).IsTrue();
        _ = await Assert.That(safe.Violation).IsNull();

        var broken = ExploreRunner.Run(ExploreProfile.SmallRead(), BrokenMode.ReadIndex);
        _ = await Assert.That(broken.Violation).IsNotNull();
        _ = await Assert.That(broken.Violation.Invariant).IsEqualTo("ReadIndex");
    }
}
