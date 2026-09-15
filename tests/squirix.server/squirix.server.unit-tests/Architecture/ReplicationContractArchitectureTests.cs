using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Adapters.Grpc.Replication;
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
    /// <summary>Product hosting must not enable FoundationOnly; only testkit may map the closed replication service.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ReleaseHostCannotEnableFoundationOnly(CancellationToken cancellationToken)
    {
        var root = RepositoryPaths.FindRepositoryRoot();
        var productHost = await File.ReadAllTextAsync(Path.Join(root, "src", "squirix.server", "AspNetCoreExtensions.cs"), cancellationToken);
        _ = await Assert.That(productHost).DoesNotContain("FoundationOnly", StringComparison.Ordinal);

        var optionsType = await File.ReadAllTextAsync(Path.Join(root, "src", "squirix.server", "SquirixServerOptions.cs"), cancellationToken);
        _ = await Assert.That(optionsType).DoesNotContain("FoundationOnly", StringComparison.Ordinal);
    }

    /// <summary>Ensures the replication wire exists only inside Squirix.Server and not in shared cache proto.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ReplicationWireIsServerOnly(CancellationToken cancellationToken)
    {
        var root = RepositoryPaths.FindRepositoryRoot();
        var sharedProto = await File.ReadAllTextAsync(Path.Join(root, "src", "shared", "Squirix", "Transport", "Grpc", "Protos", "SquirixCache.proto"), cancellationToken);
        _ = await Assert.That(sharedProto).DoesNotContain("SquirixReplicationService", StringComparison.Ordinal);
        _ = await Assert.That(sharedProto).DoesNotContain("squirix.replication", StringComparison.Ordinal);
        var serverProtobuf = await ServerArchitectureFixtures.GetServerProjectIndex().RequireIncludedElement("Protobuf", @"Adapters\Grpc\Replication\SquirixReplication.proto");
        _ = await Assert.That(serverProtobuf.GetAttribute("GrpcServices", string.Empty)).IsEqualTo("Server;Client");
        _ = await Assert.That(serverProtobuf.GetAttribute("ProtoRoot", string.Empty)).IsEqualTo(@"Adapters\Grpc\Replication");
        _ = await Assert.That(serverProtobuf.GetAttribute("Access", string.Empty)).IsEqualTo("Internal");
        var clientProjectPath = Path.Join(root, "src", "squirix", "Squirix.csproj");
        var clientProject = await File.ReadAllTextAsync(clientProjectPath, cancellationToken);
        _ = await Assert.That(clientProject).DoesNotContain("SquirixReplication.proto", StringComparison.Ordinal);
        _ = await Assert.That(typeof(SquirixReplicationService).IsVisible).IsFalse();
        _ = await Assert.That(typeof(SquirixReplicationService.SquirixReplicationServiceBase).IsVisible).IsFalse();
    }
}
