using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.Node.Observability;
using Squirix.Server.Node.Services;
using Squirix.Server.Storage.Replication;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Node.Services;

/// <summary>The order in which the readiness verdicts are evaluated for one replica-group snapshot.</summary>
[Immutable]
public sealed class ReplicaReadinessOrderTests : ServerUnitTestBase
{
    /// <summary>An unready log outranks a copy that is not complete, which outranks a topology mismatch and fencing.</summary>
    [Test]
    public async Task UnseededVerdictFollowsLogNotReady()
    {
        var leader = new ReplicaStatusSnapshot("node-a", "group-a", 3, 4, 4, 10, 7, 7, true, true, true, true, true, GroupMemberState.Unseeded);
        var unready = leader with { LogReady = false };
        var mismatched = leader with { FingerprintMatch = false, ObservedTerm = 9 };
        var fenced = leader with { ObservedTerm = 9 };
        var seeded = fenced with { MemberState = GroupMemberState.Complete };

        _ = await Assert.That(ReplicaReadiness.Evaluate(in unready)).IsEqualTo(ReplicaReadinessVerdict.LogNotReady);
        _ = await Assert.That(ReplicaReadiness.Evaluate(in mismatched)).IsEqualTo(ReplicaReadinessVerdict.Unseeded);
        _ = await Assert.That(ReplicaReadiness.Evaluate(in fenced)).IsEqualTo(ReplicaReadinessVerdict.Unseeded);
        _ = await Assert.That(ReplicaReadiness.Evaluate(in seeded)).IsEqualTo(ReplicaReadinessVerdict.StaleTerm);
    }
}
