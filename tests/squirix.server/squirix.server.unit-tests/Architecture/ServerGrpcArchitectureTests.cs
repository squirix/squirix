using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.Node.Observability.Metrics;
using Squirix.Server.UnitTests.Support;
using Squirix.Transport.Grpc.Cache;
using Squirix.Transport.Grpc.Mappers;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Architecture;

/// <summary>Architecture rules for shared gRPC compile includes, mappers, and Prometheus ownership.</summary>
[Immutable]
public sealed class ServerGrpcArchitectureTests : ServerUnitTestBase
{
    /// <summary>Ensures the server project generates the basic KV and expiration transport contract from shared source.</summary>
    [Test]
    public async Task ProjectGeneratesNarrowContractSources()
    {
        var protobuf = await ServerArchitectureFixtures.GetServerProjectIndex().RequireIncludedElement("Protobuf", @"..\shared\Squirix\Transport\Grpc\Protos\SquirixCache.proto");

        _ = await Assert.That(protobuf.GetAttribute("GrpcServices", string.Empty)).IsEqualTo("Server;Client");
        _ = await Assert.That(protobuf.GetAttribute("ProtoRoot", string.Empty)).IsEqualTo(@"..\shared\Squirix\Transport\Grpc\Protos");
        _ = await Assert.That(protobuf.GetAttribute("Access", string.Empty)).IsEqualTo("Internal");
    }

    /// <summary>Ensures client and server projects compile the same shared gRPC transport mapper sources.</summary>
    [Test]
    public async Task ProjectsCompileMappersFromSameSources()
    {
        string[] expectedIncludes =
        [
            @"..\shared\Squirix\Transport\Grpc\Mappers\GrpcStaleOwnerMarkers.cs",
        ];

        var serverIncludes = ServerArchitectureFixtures.GetServerProjectIndex().GetIncludes("Compile");
        _ = await Assert.That(serverIncludes).IsNotNull();

        foreach (var include in expectedIncludes)
            _ = await Assert.That(serverIncludes).Contains(include, StringComparer.Ordinal);
    }

    /// <summary>Ensures the server assembly generates server-side gRPC service bases from the shared transport namespace.</summary>
    [Test]
    public async Task ServerGeneratesGrpcIntoSharedNamespace()
    {
        _ = await Assert.That(typeof(CacheEntryWire).IsPublic).IsFalse();
        _ = await Assert.That(typeof(SquirixCacheService).IsPublic).IsFalse();
        _ = await Assert.That(typeof(SquirixCacheService.SquirixCacheServiceBase).IsPublic).IsFalse();
    }

    /// <summary>Ensures Prometheus metrics endpoint mapping is owned by the server package.</summary>
    [Test]
    public async Task ServerOwnsMetricsEndpointMapping() => _ = await Assert.That(typeof(EndpointExtensions).IsPublic).IsFalse();

    /// <summary>Ensures shared stale-owner marker constants are compiled into the server build from shared source.</summary>
    [Test]
    public async Task StaleOwnerConstantsPresentInServerBuild()
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

        _ = await Assert.That(found).IsTrue();
    }

    /// <summary>Ensures share-sourced gRPC transport mapper sources do not reference core internal runtime contracts.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task TransportMapperRuntimeTypesStayInternal(CancellationToken cancellationToken)
    {
        var mapperDirectory = Path.Join(RepositoryPaths.FindRepositoryRoot(), "src", "shared", "Squirix", "Transport", "Grpc", "Mappers");
        _ = await Assert.That(Directory.Exists(mapperDirectory)).IsTrue();

        var mapperPaths = new List<string>(Directory.GetFiles(mapperDirectory, "*.cs", SearchOption.TopDirectoryOnly));

        mapperPaths.Sort(StringComparer.Ordinal);
        for (var i = 0; i < mapperPaths.Count; i++)
        {
            var path = mapperPaths[i];
            var text = await File.ReadAllTextAsync(path, cancellationToken);
            for (var markerIndex = 0; markerIndex < ServerArchitectureFixtures.ForbiddenGrpcTransportMapperMarkers.Length; markerIndex++)
            {
                var marker = ServerArchitectureFixtures.ForbiddenGrpcTransportMapperMarkers[markerIndex];
                _ = await Assert.That(text.Contains(marker, StringComparison.Ordinal)).IsFalse();
            }
        }
    }

    /// <summary>Ensures share-sourced gRPC transport mappers use the shared mapper namespace.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task TransportMappersUseMappersNamespace(CancellationToken cancellationToken)
    {
        var mapperDirectory = Path.Join(RepositoryPaths.FindRepositoryRoot(), "src", "shared", "Squirix", "Transport", "Grpc", "Mappers");
        var mapperPaths = new List<string>(Directory.GetFiles(mapperDirectory, "*.cs", SearchOption.TopDirectoryOnly));

        mapperPaths.Sort(StringComparer.Ordinal);
        for (var i = 0; i < mapperPaths.Count; i++)
        {
            var path = mapperPaths[i];
            var text = await File.ReadAllTextAsync(path, cancellationToken);
            _ = await Assert.That(text).Contains("namespace Squirix.Transport.Grpc.Mappers;", StringComparison.Ordinal);
        }
    }
}
