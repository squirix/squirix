using System;
using System.IO;
using System.Threading;
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
    /// <summary>Ensures the replication wire exists only inside Squirix.Server and not in shared cache proto.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ReplicationWireIsServerOnly(CancellationToken cancellationToken)
    {
        var root = RepositoryPaths.FindRepositoryRoot();
        var sharedProto = await File.ReadAllTextAsync(Path.Join(root, "src", "shared", "Squirix", "Transport", "Wire", "Protos", "SquirixCache.proto"), cancellationToken);
        _ = await Assert.That(sharedProto).DoesNotContain("SquirixReplicationService", StringComparison.Ordinal);
        _ = await Assert.That(sharedProto).DoesNotContain("squirix.replication", StringComparison.Ordinal);
        var clientProjectPath = Path.Join(root, "src", "squirix", "Squirix.csproj");
        var clientProject = await File.ReadAllTextAsync(clientProjectPath, cancellationToken);
        _ = await Assert.That(clientProject).DoesNotContain("SquirixReplication.proto", StringComparison.Ordinal);
        _ = await Assert.That(typeof(ReplicationEnvelopeHeader).IsVisible).IsFalse();
        _ = await Assert.That(typeof(TransferReplicaStateRequest).IsVisible).IsFalse();
    }
}
