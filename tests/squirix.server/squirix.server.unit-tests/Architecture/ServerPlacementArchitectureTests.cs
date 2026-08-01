using System.Threading.Tasks;
using Squirix.Server.UnitTests.Support;
using Xunit;

namespace Squirix.Server.UnitTests.Architecture;

/// <summary>Architecture rules for Filter/Handler/Metrics/Options/Service namespace placement.</summary>
public sealed class ServerPlacementArchitectureTests : ServerUnitTestBase
{
    /// <summary>Ensures filter types stay at the REST adapter boundary.</summary>
    [Fact]
    public async Task FilterTypesShouldLiveInAdaptersRestNamespace()
    {
        var types = await ServerTypeCatalog.TypesWithNameEndingWithAsync("Filter", true, cancellationToken: DefaultCancellationToken);
        ServerTypeCatalog.AssertResideInOneOfNamespaces(
            types,
            [$"{ServerArchitectureNamespaces.Adapters}.Rest", $"{ServerArchitectureNamespaces.Adapters}.Endpoint.Rest"]);
    }

    /// <summary>Ensures handler types stay in the hosting security boundary.</summary>
    [Fact]
    public async Task HandlerTypesLiveInNodeHostingSecurityNamespace()
    {
        var types = await ServerTypeCatalog.TypesWithNameEndingWithAsync("Handler", true, cancellationToken: DefaultCancellationToken);
        ServerTypeCatalog.AssertResideInNamespace(
            types,
            $"{ServerArchitectureNamespaces.Node}.Hosting.Security");
    }

    /// <summary>Ensures metrics types stay centralized in the observability namespace.</summary>
    [Fact]
    public async Task MetricsTypesShouldLiveInObservabilityNamespace()
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
    public async Task OptionsTypesShouldLiveInApprovedNamespaces()
    {
        var types = await ServerTypeCatalog.TypesWithNameEndingWithAsync("Options", true, cancellationToken: DefaultCancellationToken);
        ServerTypeCatalog.AssertResideInOneOfNamespaces(types, Allowlists.ServerOptionsTypeNamespaces);
    }

    /// <summary>Ensures service types stay in approved service namespaces.</summary>
    [Fact]
    public async Task ServiceTypesShouldLiveInApprovedNamespaces()
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
        ];

        /// <summary>
        /// Exact namespaces where <c>*Service</c> types are permitted to reside.
        /// </summary>
        internal static readonly string[] ServiceTypeNamespaces =
        [
            $"{ServerArchitectureNamespaces.Node}.Services",
            ServerArchitectureNamespaces.Cluster,
            $"{ServerArchitectureNamespaces.Node}.Context",
            "Squirix.Transport.Grpc",
        ];
    }
}
