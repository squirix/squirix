using System;
using System.IO;
using System.Threading.Tasks;
using Squirix.Server.Adapters.Grpc.Replication;
using Squirix.Server.UnitTests.Support;
using Xunit;

namespace Squirix.Server.UnitTests.Architecture;

/// <summary>Architecture rules for the closed server-only replication wire contract.</summary>
public sealed class ReplicationContractArchitectureTests : ServerUnitTestBase
{
    /// <summary>Ensures the replication wire exists only inside Squirix.Server and not in shared cache proto.</summary>
    [Fact]
    public async Task ReplicationWireIsServerOnly()
    {
        var root = RepositoryPaths.FindRepositoryRoot();
        var sharedProto = await File.ReadAllTextAsync(
            Path.Join(root, "src", "shared", "Squirix", "Transport", "Grpc", "Protos", "SquirixCache.proto"),
            DefaultCancellationToken);
        Assert.DoesNotContain("SquirixReplicationService", sharedProto, StringComparison.Ordinal);
        Assert.DoesNotContain("squirix.replication", sharedProto, StringComparison.Ordinal);

        var serverProtobuf = ServerArchitectureFixtures.GetServerProjectIndex()
                                                       .RequireIncludedElement("Protobuf", @"Adapters\Grpc\Replication\SquirixReplication.proto");
        Assert.Equal("Server;Client", serverProtobuf.Attribute("GrpcServices")?.Value);
        Assert.Equal(@"Adapters\Grpc\Replication", serverProtobuf.Attribute("ProtoRoot")?.Value);
        Assert.Equal("Internal", serverProtobuf.Attribute("Access")?.Value);

        var clientProjectPath = Path.Join(root, "src", "squirix", "Squirix.csproj");
        var clientProject = await File.ReadAllTextAsync(clientProjectPath, DefaultCancellationToken);
        Assert.DoesNotContain("SquirixReplication.proto", clientProject, StringComparison.Ordinal);

        Assert.False(typeof(SquirixReplicationService).IsPublic);
        Assert.False(typeof(SquirixReplicationService.SquirixReplicationServiceBase).IsPublic);
    }

    /// <summary>Product hosting must not enable FoundationOnly; only testkit may map the closed replication service.</summary>
    [Fact]
    public async Task ReleaseHostCannotEnableFoundationOnly()
    {
        var root = RepositoryPaths.FindRepositoryRoot();
        var productHost = await File.ReadAllTextAsync(
            Path.Join(root, "src", "squirix.server", "AspNetCoreExtensions.cs"),
            DefaultCancellationToken);
        Assert.DoesNotContain("FoundationOnly", productHost, StringComparison.Ordinal);

        var optionsType = await File.ReadAllTextAsync(
            Path.Join(root, "src", "squirix.server", "SquirixServerOptions.cs"),
            DefaultCancellationToken);
        Assert.DoesNotContain("FoundationOnly", optionsType, StringComparison.Ordinal);
    }
}
