using System;
using ArchUnitNET.Fluent.Syntax.Elements.Types;
using ArchUnitNET.xUnitV3;
using Squirix.Server.UnitTests.Support;
using Xunit;
using static ArchUnitNET.Fluent.ArchRuleDefinition;

namespace Squirix.Server.UnitTests.Architecture;

/// <summary>Architecture rules for Filter/Handler/Metrics/Options/Service namespace placement.</summary>
public sealed class ServerPlacementArchitectureTests : ServerUnitTestBase
{
    /// <summary>Ensures filter types stay at the REST adapter boundary.</summary>
    [Fact]
    public void FilterTypesShouldLiveInAdaptersRestNamespace()
    {
        RuleHelpers.AssertResideInOneOfNamespaces(
            ServerArchitectureScope.Server.And().HaveNameEndingWith("Filter"),
            [$"{ServerArchitectureNamespaces.Adapters}.Rest", $"{ServerArchitectureNamespaces.Adapters}.Endpoint.Rest"]);
    }

    /// <summary>Ensures handler types stay in the hosting security boundary.</summary>
    [Fact]
    public void HandlerTypesLiveInNodeHostingSecurityNamespace()
    {
        var rule = ServerArchitectureScope.Server.And().HaveNameEndingWith("Handler").Should().ResideInNamespace($"{ServerArchitectureNamespaces.Node}.Hosting.Security")
                                          .WithoutRequiringPositiveResults();

        rule.Check(ServerArchitecture.Instance);
    }

    /// <summary>Ensures metrics types stay centralized in the observability namespace.</summary>
    [Fact]
    public void MetricsTypesShouldLiveInObservabilityNamespace()
    {
        var rule = ServerArchitectureScope.Server.And().HaveNameEndingWith("Metrics").And().AreNot(Interfaces()).And()
                                          .DoNotHaveFullName("Squirix.Server.Storage.Manifest.NoOpManifestRetentionFailureMetrics").Should()
                                          .ResideInNamespace($"{ServerArchitectureNamespaces.Node}.Observability");

        rule.Check(ServerArchitecture.Instance);
    }

    /// <summary>Ensures configuration option types live only in approved configuration namespaces.</summary>
    [Fact]
    public void OptionsTypesShouldLiveInApprovedNamespaces() => RuleHelpers.AssertResideInOneOfNamespaces(
        ServerArchitectureScope.Server.And().HaveNameEndingWith("Options"),
        Allowlists.ServerOptionsTypeNamespaces);

    /// <summary>Ensures service types stay in approved service namespaces.</summary>
    [Fact]
    public void ServiceTypesShouldLiveInApprovedNamespaces() => RuleHelpers.AssertResideInOneOfNamespaces(
        ServerArchitectureScope.Server.And().HaveNameEndingWith("Service"),
        Allowlists.ServiceTypeNamespaces);

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

    /// <summary>ArchUnitNET composition helpers used across architecture tests.</summary>
    private static class RuleHelpers
    {
        /// <summary>
        /// Asserts every type matched by <paramref name="matchingTypes" /> resides in one of the given exact namespaces (disjunction).
        /// </summary>
        /// <param name="matchingTypes">The ArchUnitNET type predicate (for example name suffix <c>Options</c>).</param>
        /// <param name="exactNamespaces">Exact namespace names; a type passes if its namespace equals any entry.</param>
        /// <exception cref="ArgumentNullException"><paramref name="matchingTypes" /> or <paramref name="exactNamespaces" /> is <see langword="null" />.</exception>
        /// <exception cref="ArgumentException"><paramref name="exactNamespaces" /> is empty.</exception>
        internal static void AssertResideInOneOfNamespaces(GivenTypesConjunction matchingTypes, string[] exactNamespaces)
        {
            ArgumentNullException.ThrowIfNull(matchingTypes);
            ArgumentNullException.ThrowIfNull(exactNamespaces);
            if (exactNamespaces.Length is 0)
                throw new ArgumentException("At least one namespace is required.", nameof(exactNamespaces));

            foreach (var type in matchingTypes.GetObjects(ServerArchitecture.Instance))
            {
                var namespaceName = type.Namespace?.FullName ?? string.Empty;
                Assert.True(ResidesInOneOfExactNamespaces(namespaceName, exactNamespaces));
            }
        }

        private static bool ResidesInOneOfExactNamespaces(string typeNamespace, string[] exactNamespaces)
        {
            for (var namespaceIndex = 0; namespaceIndex < exactNamespaces.Length; namespaceIndex++)
            {
                if (string.Equals(typeNamespace, exactNamespaces[namespaceIndex], StringComparison.Ordinal))
                    return true;
            }

            return false;
        }
    }
}
