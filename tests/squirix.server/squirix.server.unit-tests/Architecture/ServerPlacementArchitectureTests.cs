using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.UnitTests.Support;
using Xunit;

namespace Squirix.Server.UnitTests.Architecture;

/// <summary>Architecture rules for Metrics/Options/Service namespace placement.</summary>
[Immutable]
public sealed class ServerPlacementArchitectureTests : ServerUnitTestBase
{
    /// <summary>Ensures metrics types stay centralized in the observability namespace.</summary>
    [Fact]
    public async Task MetricsTypesLiveInObservabilityNamespace()
    {
        var types = await ServerTypeCatalog.TypesWithNameEndingWithAsync(
            "Metrics",
            false,
            ["Squirix.Server.Storage.Manifest.NoOpManifestRetentionFailureMetrics"],
            DefaultCancellationToken);
        ServerTypeCatalog.AssertResideInNamespace(
            types,
            $"{ServerArchitectureNamespaces.Node}.Observability");
    }

    /// <summary>Ensures configuration option types live only in approved configuration namespaces.</summary>
    [Fact]
    public async Task OptionsTypesLiveInApprovedNamespaces()
    {
        var types = await ServerTypeCatalog.TypesWithNameEndingWithAsync("Options", true, cancellationToken: DefaultCancellationToken);
        ServerTypeCatalog.AssertResideInOneOfNamespaces(types, Allowlists.ServerOptionsTypeNamespaces);
    }

    /// <summary>Ensures service types stay in approved service namespaces.</summary>
    [Fact]
    public async Task ServiceTypesLiveInApprovedNamespaces()
    {
        var types = await ServerTypeCatalog.TypesWithNameEndingWithAsync("Service", true, cancellationToken: DefaultCancellationToken);
        ServerTypeCatalog.AssertResideInOneOfNamespaces(types, Allowlists.ServiceTypeNamespaces);
    }

    /// <summary>Centralized namespace allowlists for naming-convention architecture rules.</summary>
    private static class Allowlists
    {
        /// <summary>
        /// Exact namespaces where server <c>*Options</c> types are permitted to reside.
        /// </summary>
        internal static readonly string[] ServerOptionsTypeNamespaces =
        [
            ServerArchitectureNamespaces.Root,
            "Squirix.Server.Core",
            $"{ServerArchitectureNamespaces.Node}.Backpressure",
            $"{ServerArchitectureNamespaces.Node}.MemoryPressure",
            $"{ServerArchitectureNamespaces.Node}.Services",
            $"{ServerArchitectureNamespaces.Node}.Hosting",
            $"{ServerArchitectureNamespaces.Node}.Hosting.Security",
            $"{ServerArchitectureNamespaces.Node}.Observability.Metrics",
            $"{ServerArchitectureNamespaces.Node}.App.Decorators",
            "Squirix.Server.Runtime.Contracts",
            ServerArchitectureNamespaces.Storage,
            $"{ServerArchitectureNamespaces.Storage}.Snapshot",
            $"{ServerArchitectureNamespaces.Storage}.Journaling",
            $"{ServerArchitectureNamespaces.Storage}.Journaling.Compaction",
            ServerArchitectureNamespaces.Cluster,
            $"{ServerArchitectureNamespaces.Cluster}.Transport",
            $"{ServerArchitectureNamespaces.Cluster}.Replication",
        ];

        /// <summary>
        /// Exact namespaces where <c>*Service</c> types are permitted to reside.
        /// </summary>
        /// <remarks>
        /// <c>Squirix.Transport.Grpc</c> is intentionally omitted: placement discovery only scans
        /// <c>src/squirix.server</c> sources under <c>Squirix.Server*</c> namespaces, and the shared
        /// transport files linked into the server do not declare handwritten <c>*Service</c> types.
        /// Proto-generated gRPC service stubs are covered by dedicated gRPC architecture tests instead.
        /// </remarks>
        internal static readonly string[] ServiceTypeNamespaces =
        [
            $"{ServerArchitectureNamespaces.Node}.Services",
            ServerArchitectureNamespaces.Cluster,
            $"{ServerArchitectureNamespaces.Node}.Context",
        ];
    }
}
