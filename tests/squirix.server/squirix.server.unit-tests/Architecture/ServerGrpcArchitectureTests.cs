using System;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.UnitTests.Support;
using Squirix.Transport.Grpc.Cache;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Architecture;

/// <summary>Architecture rules for shared gRPC compile includes, and generated contract visibility.</summary>
[Immutable]
public sealed class ServerGrpcArchitectureTests : ServerUnitTestBase
{
    /// <summary>Ensures the server project generates the basic KV and expiration transport contract from shared source.</summary>
    [Test]
    public async Task ProjectGeneratesNarrowContractSources()
    {
        var protobuf = await ServerArchitectureFixtures.GetServerProjectIndex().RequireIncludedElementAsync(
            "Protobuf",
            @"..\shared\Squirix\Transport\Grpc\Protos\SquirixCache.proto");

        _ = await Assert.That(protobuf.GetAttribute("GrpcServices", string.Empty)).IsEqualTo("Server;Client");
        _ = await Assert.That(protobuf.GetAttribute("ProtoRoot", string.Empty)).IsEqualTo(@"..\shared\Squirix\Transport\Grpc\Protos");
        _ = await Assert.That(protobuf.GetAttribute("Access", string.Empty)).IsEqualTo("Internal");
    }

    /// <summary>Ensures both packages compile the shared cache wire encoding from the same source.</summary>
    [Test]
    public async Task ProjectsShareCacheWireEncoding()
    {
        const string include = @"..\shared\Squirix\Transport\Grpc\CacheValueWire.cs";
        var serverIncludes = ServerArchitectureFixtures.GetServerProjectIndex().GetIncludes("Compile");
        var clientProject = await ServerArchitectureFixtures.LoadProjectAsync("src/squirix/Squirix.csproj");
        var clientIncludes = ServerArchitectureFixtures.ParseMsbuildProject(clientProject).GetIncludes("Compile");

        _ = await Assert.That(serverIncludes).IsNotNull();
        _ = await Assert.That(clientIncludes).IsNotNull();
        _ = await Assert.That(serverIncludes).Contains(include, StringComparer.Ordinal);
        _ = await Assert.That(clientIncludes).Contains(include, StringComparer.Ordinal);
    }

    /// <summary>Ensures the server assembly generates server-side gRPC service bases from the shared transport namespace.</summary>
    [Test]
    public async Task ServerGeneratesGrpcIntoSharedNamespace()
    {
        _ = await Assert.That(typeof(CacheEntryWire).IsPublic).IsFalse();
        _ = await Assert.That(typeof(SquirixCacheService).IsPublic).IsFalse();
        _ = await Assert.That(typeof(SquirixCacheService.SquirixCacheServiceBase).IsPublic).IsFalse();
    }
}
