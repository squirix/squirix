using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.Storage.Replication;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Persistence.Replication;

/// <summary>
/// Guards the mirror between the wire refusal codes (<see cref="RefusalCodes" />) and their storage-side
/// twin (<see cref="FollowerLogRefusal" />). The transport adapter maps through the storage constants, so
/// every value must stay identical in both directions.
/// </summary>
[Immutable]
public sealed class RefusalMirrorTests : ServerUnitTestBase
{
    /// <summary>Already-voted marker matches across both namespaces.</summary>
    [Test]
    public async Task AlreadyVotedMirrors() => _ = await Assert.That(FollowerLogRefusal.AlreadyVoted).IsEqualTo(RefusalCodes.AlreadyVoted);

    /// <summary>Checksum-mismatch marker matches across both namespaces.</summary>
    [Test]
    public async Task ChecksumMismatchMirrors() => _ = await Assert.That(FollowerLogRefusal.ChecksumMismatch).IsEqualTo(RefusalCodes.ChecksumMismatch);

    /// <summary>Log-mismatch marker matches across both namespaces.</summary>
    [Test]
    public async Task LogMismatchMirrors() => _ = await Assert.That(FollowerLogRefusal.LogMismatch).IsEqualTo(RefusalCodes.LogMismatch);

    /// <summary>Not-member marker matches across both namespaces.</summary>
    [Test]
    public async Task NotMemberMirrors() => _ = await Assert.That(FollowerLogRefusal.NotMember).IsEqualTo(RefusalCodes.NotMember);

    /// <summary>Not-ready marker matches across both namespaces.</summary>
    [Test]
    public async Task NotReadyMirrors() => _ = await Assert.That(FollowerLogRefusal.NotReady).IsEqualTo(RefusalCodes.NotReady);

    /// <summary>Stale-log marker matches across both namespaces.</summary>
    [Test]
    public async Task StaleLogMirrors() => _ = await Assert.That(FollowerLogRefusal.StaleLog).IsEqualTo(RefusalCodes.StaleLog);

    /// <summary>Stale-term marker matches across both namespaces.</summary>
    [Test]
    public async Task StaleTermMirrors() => _ = await Assert.That(FollowerLogRefusal.StaleTerm).IsEqualTo(RefusalCodes.StaleTerm);

    /// <summary>Topology-mismatch marker matches across both namespaces.</summary>
    [Test]
    public async Task TopologyMismatchMirrors() => _ = await Assert.That(FollowerLogRefusal.TopologyMismatch).IsEqualTo(RefusalCodes.TopologyMismatch);
}
