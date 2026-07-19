using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using ArchUnitNET.xUnitV3;
using Squirix.Server.Node.Observability.Metrics;
using Squirix.Server.UnitTests.Support;
using Squirix.Transport.Grpc.Cache;
using Squirix.Transport.Grpc.Mappers;
using Xunit;

namespace Squirix.Server.UnitTests.Architecture;

/// <summary>Architecture rules for shared gRPC compile includes, mappers, storage isolation, and Prometheus ownership.</summary>
public sealed class ServerGrpcArchitectureTests : ServerUnitTestBase
{
    /// <summary>Ensures client and server projects compile the same shared gRPC transport mapper sources.</summary>
    [Fact]
    public void ClientAndServerProjectsShouldCompileSharedGrpcTransportMappersFromSameSources()
    {
        string[] expectedIncludes =
        [
            @"..\shared\Squirix\Transport\Grpc\Mappers\GrpcStaleOwnerMarkers.cs",
        ];

        var serverIncludes = ServerArchitectureFixtures.GetServerProjectIndex().GetIncludes("Compile");

        foreach (var include in expectedIncludes)
            Assert.Contains(include, serverIncludes, StringComparer.Ordinal);
    }

    /// <summary>Ensures the server assembly generates server-side gRPC service bases from the shared transport namespace.</summary>
    [Fact]
    public void ServerAssemblyShouldGenerateGrpcServiceBaseFromSharedTransportNamespace()
    {
        Assert.False(typeof(CacheEntryWire).IsPublic);
        Assert.False(typeof(SquirixCacheService).IsPublic);
        Assert.False(typeof(SquirixCacheService.SquirixCacheServiceBase).IsPublic);
    }

    /// <summary>Ensures the server project generates the basic KV and expiration transport contract from shared source.</summary>
    [Fact]
    public void ServerProjectShouldGenerateNarrowCacheGrpcTransportContractFromSharedSource()
    {
        var protobuf = ServerArchitectureFixtures.GetServerProjectIndex().RequireIncludedElement("Protobuf", @"..\shared\Squirix\Transport\Grpc\Protos\SquirixCache.proto");

        Assert.Equal("Server;Client", protobuf.Attribute("GrpcServices")?.Value);
        Assert.Equal(@"..\shared\Squirix\Transport\Grpc\Protos", protobuf.Attribute("ProtoRoot")?.Value);
        Assert.Equal("Internal", protobuf.Attribute("Access")?.Value);
    }

    /// <summary>Ensures Prometheus metrics endpoint mapping is owned by the server package.</summary>
    [Fact]
    public void ServerShouldOwnPrometheusMetricsEndpointMapping() => Assert.False(typeof(EndpointExtensions).IsPublic);

    /// <summary>Ensures shared stale-owner marker constants are compiled into the server build from shared source.</summary>
    [Fact]
    public void SharedGrpcStaleOwnerMarkerConstantsShouldBePresentInServerBuild()
    {
        var found = false;
        var entries = GrpcStaleOwnerMarkers.CreateStaleOwnerTrailers();
        for (var index = 0; index < entries.Count; index++)
        {
            var entry = entries[index];
            if (!string.Equals(entry.Key, "squirix-error-code", StringComparison.Ordinal) || !string.Equals(entry.Value, "stale-owner", StringComparison.Ordinal))
                continue;
            found = true;
            break;
        }

        Assert.True(found);
    }

    /// <summary>Ensures share-sourced gRPC transport mapper sources do not reference core internal runtime contracts.</summary>
    [Fact]
    public async Task SharedGrpcTransportMapperSourcesShouldNotDependOnCoreInternalRuntimeTypes()
    {
        var mapperDirectory = Path.Join(RepositoryPaths.FindRepositoryRoot(), "src", "shared", "Squirix", "Transport", "Grpc", "Mappers");
        Assert.True(Directory.Exists(mapperDirectory), $"Expected mapper directory at {mapperDirectory}.");

        var mapperPaths = new List<string>(Directory.GetFiles(mapperDirectory, "*.cs", SearchOption.TopDirectoryOnly));

        mapperPaths.Sort(StringComparer.Ordinal);
        for (var i = 0; i < mapperPaths.Count; i++)
        {
            var path = mapperPaths[i];
            var text = await File.ReadAllTextAsync(path, DefaultCancellationToken);
            for (var markerIndex = 0; markerIndex < ServerArchitectureFixtures.ForbiddenSharedGrpcTransportMapperRuntimeMarkers.Length; markerIndex++)
            {
                var marker = ServerArchitectureFixtures.ForbiddenSharedGrpcTransportMapperRuntimeMarkers[markerIndex];
                Assert.False(text.Contains(marker, StringComparison.Ordinal), $"{Path.GetFileName(path)}:{marker}");
            }
        }
    }

    /// <summary>Ensures share-sourced gRPC transport mappers use the shared mapper namespace.</summary>
    [Fact]
    public async Task SharedGrpcTransportMappersShouldUseGrpcMappersNamespace()
    {
        var mapperDirectory = Path.Join(RepositoryPaths.FindRepositoryRoot(), "src", "shared", "Squirix", "Transport", "Grpc", "Mappers");
        var mapperPaths = new List<string>(Directory.GetFiles(mapperDirectory, "*.cs", SearchOption.TopDirectoryOnly));

        mapperPaths.Sort(StringComparer.Ordinal);
        for (var i = 0; i < mapperPaths.Count; i++)
        {
            var path = mapperPaths[i];
            var text = await File.ReadAllTextAsync(path, DefaultCancellationToken);
            Assert.Contains("namespace Squirix.Transport.Grpc.Mappers;", text, StringComparison.Ordinal);
        }
    }

    /// <summary>Ensures storage types stay isolated from transport adapter concerns.</summary>
    [Fact]
    public void StorageShouldNotDependOnAdapters()
    {
        var rule = ServerArchitectureScope.Server.And().HaveFullNameContaining(ServerArchitectureNamespaces.Storage).Should().NotDependOnAnyTypesThat()
                                          .HaveFullNameContaining(ServerArchitectureNamespaces.Adapters);

        rule.Check(ServerArchitecture.Instance);
    }

    /// <summary>Ensures storage code does not take a dependency on hosting/DI composition details.</summary>
    [Fact]
    public void StorageShouldNotDependOnNodeHosting()
    {
        var rule = ServerArchitectureScope.Server.And().HaveFullNameContaining(ServerArchitectureNamespaces.Storage).Should().NotDependOnAnyTypesThat()
                                          .HaveFullNameContaining($"{ServerArchitectureNamespaces.Node}.Hosting");

        rule.Check(ServerArchitecture.Instance);
    }
}
