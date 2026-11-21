using System.Threading.Tasks;
using Squirix.Server.Adapters.Wire.Replication;
using Squirix.Server.Attributes;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Architecture;

/// <summary>Architecture rules for the closed server-only replication wire contract.</summary>
[Immutable]
public sealed class ReplicationContractArchitectureTests : ServerUnitTestBase
{
    /// <summary>Ensures replication message types remain internal to the server assembly.</summary>
    [Test]
    public async Task ReplicationWireIsInternal()
    {
        _ = await Assert.That(typeof(EnvelopeHeader).IsVisible).IsFalse();
        _ = await Assert.That(typeof(TransferReplicaStateRequest).IsVisible).IsFalse();
    }
}
