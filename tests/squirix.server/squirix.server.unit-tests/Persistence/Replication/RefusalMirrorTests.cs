using Squirix.Server.Cluster.Replication;
using Squirix.Server.Storage.Replication;
using Squirix.Server.UnitTests.Support;
using Xunit;

namespace Squirix.Server.UnitTests.Persistence.Replication;

/// <summary>
/// Guards the mirror between the wire refusal codes (<see cref="RefusalCodes" />) and their storage-side
/// twin (<see cref="FollowerLogRefusal" />). The transport adapter maps through the storage constants, so
/// every value must stay identical in both directions.
/// </summary>
public sealed class RefusalMirrorTests : ServerUnitTestBase
{
    /// <summary>Stale-term marker matches across both namespaces.</summary>
    [Fact]
    public void StaleTermMirrors() => Assert.Equal(RefusalCodes.StaleTerm, FollowerLogRefusal.StaleTerm);

    /// <summary>Log-mismatch marker matches across both namespaces.</summary>
    [Fact]
    public void LogMismatchMirrors() => Assert.Equal(RefusalCodes.LogMismatch, FollowerLogRefusal.LogMismatch);

    /// <summary>Not-ready marker matches across both namespaces.</summary>
    [Fact]
    public void NotReadyMirrors() => Assert.Equal(RefusalCodes.NotReady, FollowerLogRefusal.NotReady);

    /// <summary>Checksum-mismatch marker matches across both namespaces.</summary>
    [Fact]
    public void ChecksumMismatchMirrors() => Assert.Equal(RefusalCodes.ChecksumMismatch, FollowerLogRefusal.ChecksumMismatch);

    /// <summary>Topology-mismatch marker matches across both namespaces.</summary>
    [Fact]
    public void TopologyMismatchMirrors() => Assert.Equal(RefusalCodes.TopologyMismatch, FollowerLogRefusal.TopologyMismatch);

    /// <summary>Not-member marker matches across both namespaces.</summary>
    [Fact]
    public void NotMemberMirrors() => Assert.Equal(RefusalCodes.NotMember, FollowerLogRefusal.NotMember);
}
