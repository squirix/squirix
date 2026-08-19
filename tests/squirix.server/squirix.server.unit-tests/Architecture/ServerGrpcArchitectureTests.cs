using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.Node.Observability.Metrics;
using Squirix.Server.UnitTests.Support;
using Squirix.Transport.Grpc.Cache;
using Squirix.Transport.Grpc.Mappers;
using Xunit;

namespace Squirix.Server.UnitTests.Architecture;

/// <summary>Architecture rules for shared gRPC compile includes, mappers, and Prometheus ownership.</summary>
[Immutable]
public sealed class ServerGrpcArchitectureTests : ServerUnitTestBase
{
    /// <summary>Ensures client and server projects compile the same shared gRPC transport mapper sources.</summary>
    [Fact]
    public void ClientServerProjectsCompileMappersSameSources()
    {
        string[] expectedIncludes =
        [
            @"..\shared\Squirix\Transport\Grpc\Mappers\GrpcStaleOwnerMarkers.cs",
        ];

        var serverIncludes = ServerArchitectureFixtures.GetServerProjectIndex().GetIncludes("Compile");
        Assert.NotNull(serverIncludes);

        foreach (var include in expectedIncludes)
            Assert.Contains(include, serverIncludes, StringComparer.Ordinal);
    }

    /// <summary>Ensures the server assembly generates server-side gRPC service bases from the shared transport namespace.</summary>
    [Fact]
    public void ServerAssemblyGenerateGrpcSharedTransportNamespace()
    {
        Assert.False(typeof(CacheEntryWire).IsPublic);
        Assert.False(typeof(SquirixCacheService).IsPublic);
        Assert.False(typeof(SquirixCacheService.SquirixCacheServiceBase).IsPublic);
    }

    /// <summary>Ensures the server project generates the basic KV and expiration transport contract from shared source.</summary>
    [Fact]
    public void ServerProjectGenerateNarrowContractSharedSource()
    {
        var protobuf = ServerArchitectureFixtures.GetServerProjectIndex().RequireIncludedElement("Protobuf", @"..\shared\Squirix\Transport\Grpc\Protos\SquirixCache.proto");

        Assert.Equal("Server;Client", protobuf.GetAttribute("GrpcServices", string.Empty));
        Assert.Equal(@"..\shared\Squirix\Transport\Grpc\Protos", protobuf.GetAttribute("ProtoRoot", string.Empty));
        Assert.Equal("Internal", protobuf.GetAttribute("Access", string.Empty));
    }

    /// <summary>Ensures Prometheus metrics endpoint mapping is owned by the server package.</summary>
    [Fact]
    public void ServerShouldOwnPrometheusMetricsEndpointMapping() => Assert.False(typeof(EndpointExtensions).IsPublic);

    /// <summary>Ensures shared stale-owner marker constants are compiled into the server build from shared source.</summary>
    [Fact]
    public void SharedGrpcStaleOwnerConstantsPresentServerBuild()
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
    public async Task SharedGrpcTransportMapperCoreInternalRuntimeTypes()
    {
        var mapperDirectory = Path.Join(RepositoryPaths.FindRepositoryRoot(), "src", "shared", "Squirix", "Transport", "Grpc", "Mappers");
        Assert.True(Directory.Exists(mapperDirectory));

        var mapperPaths = new List<string>(Directory.GetFiles(mapperDirectory, "*.cs", SearchOption.TopDirectoryOnly));

        mapperPaths.Sort(StringComparer.Ordinal);
        for (var i = 0; i < mapperPaths.Count; i++)
        {
            var path = mapperPaths[i];
            var text = await File.ReadAllTextAsync(path, DefaultCancellationToken);
            for (var markerIndex = 0; markerIndex < ServerArchitectureFixtures.ForbiddenSharedGrpcTransportMapperRuntimeMarkers.Length; markerIndex++)
            {
                var marker = ServerArchitectureFixtures.ForbiddenSharedGrpcTransportMapperRuntimeMarkers[markerIndex];
                Assert.False(text.Contains(marker, StringComparison.Ordinal));
            }
        }
    }

    /// <summary>Ensures share-sourced gRPC transport mappers use the shared mapper namespace.</summary>
    [Fact]
    public async Task SharedGrpcTransportMappersUseGrpcMappersNamespace()
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
}
