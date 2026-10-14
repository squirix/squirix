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

    /// <summary>
    /// A member that lost its disk and is repaired with the term ceiling of a majority never helps elect a second leader in a term it voted
    /// in before the loss; repaired without the ceiling, it does.
    /// </summary>
    [Test]
    public async Task RepairAfterDiskLossNeedsTermCeiling()
    {
        var safe = ExploreRunner.Run(ExploreProfile.SmallDiskLoss(), BrokenMode.None);
        _ = await Assert.That(safe.Violation).IsNull();
        _ = await Assert.That(safe.FixedPointReached).IsTrue();

        var broken = ExploreRunner.Run(ExploreProfile.SmallDiskLoss(), BrokenMode.RepairCeiling);
        _ = await Assert.That(broken.Violation).IsNotNull();
        _ = await Assert.That(broken.Violation.Invariant).IsEqualTo("ElectionSafety");
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
