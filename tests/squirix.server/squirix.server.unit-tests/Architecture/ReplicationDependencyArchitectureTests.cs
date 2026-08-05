using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using Squirix.Server.UnitTests.Support;
using Xunit;

namespace Squirix.Server.UnitTests.Architecture;

/// <summary>
/// Non-NsDepCop architecture scans for replication composition hygiene.
/// Namespace DAG edges are enforced by <c>config.nsdepcop</c>, not duplicated here.
/// </summary>
public sealed class ReplicationDependencyArchitectureTests : ServerUnitTestBase
{
    /// <summary>Shared transport sources must not embed server replication logic.</summary>
    [Fact]
    public async Task SharedTransportContainsNoServerLogic()
    {
        var transportRoot = Path.Join(RepositoryPaths.FindRepositoryRoot(), "src", "shared", "Squirix", "Transport");
        Assert.True(Directory.Exists(transportRoot));

        string[] forbidden =
        [
            "Squirix.Server.Cluster.Replication",
            "SquirixReplicationService",
            "FoundationOnly",
            "AppendReplicaEntries",
        ];

        foreach (var path in Directory.GetFiles(transportRoot, "*.cs", SearchOption.AllDirectories))
        {
            var text = await File.ReadAllTextAsync(path, DefaultCancellationToken);
            for (var i = 0; i < forbidden.Length; i++)
                Assert.False(text.Contains(forbidden[i], StringComparison.Ordinal), path);
        }

        foreach (var path in Directory.GetFiles(transportRoot, "*.proto", SearchOption.AllDirectories))
        {
            var text = await File.ReadAllTextAsync(path, DefaultCancellationToken);
            Assert.DoesNotContain("SquirixReplicationService", text, StringComparison.Ordinal);
            Assert.DoesNotContain("squirix.replication", text, StringComparison.Ordinal);
        }
    }

    /// <summary>Hosting composition owns FoundationOnly mapping of the closed replication adapter.</summary>
    [Fact]
    public async Task HostingOwnsReplicationComposition()
    {
        var root = RepositoryPaths.FindRepositoryRoot();
        var mapping = await File.ReadAllTextAsync(
            Path.Join(root, "src", "squirix.server", "Node", "Hosting", "SquirixEndpointMapping.cs"),
            DefaultCancellationToken);
        var composition = await File.ReadAllTextAsync(
            Path.Join(root, "src", "squirix.server", "Node", "Hosting", "ServerHostingComposition.cs"),
            DefaultCancellationToken);

        Assert.Contains("FoundationOnly", mapping, StringComparison.Ordinal);
        Assert.Contains("SquirixReplicationServiceAdapter", mapping, StringComparison.Ordinal);
        Assert.Contains("FoundationOnly", composition, StringComparison.Ordinal);
        Assert.Contains("SquirixReplicationServiceAdapter", composition, StringComparison.Ordinal);
    }

    /// <summary>Cluster.Replication sources must not resolve services through a locator.</summary>
    [Fact]
    public Task ReplicationUsesNoServiceLocator() => AssertSourcesDoNotContainAsync(
        [
            ".GetService(",
            ".GetRequiredService(",
            "IServiceProvider",
            "IServiceScope",
        ],
        static path => !path.EndsWith("ServiceRegistration.cs", StringComparison.Ordinal));

    /// <summary>Cluster.Replication sources must not use reflection.</summary>
    [Fact]
    public Task ReplicationUsesNoReflection() => AssertSourcesDoNotContainAsync(
        [
            "System.Reflection",
            "Type.GetType(",
            "Activator.CreateInstance",
            "GetMethod(",
            "Invoke(",
        ]);

    /// <summary>Cluster.Replication sources must not introduce mutable static fields.</summary>
    [Fact]
    public Task ReplicationUsesNoMutableGlobalState() => AssertSourcesDoNotContainAsync(
        [
            "private static int ",
            "private static long ",
            "private static bool ",
            "private static object ",
            "internal static int ",
            "internal static long ",
            "internal static bool ",
            "internal static object ",
            "public static int ",
            "public static long ",
            "public static bool ",
            "public static object ",
        ]);

    /// <summary>Cluster.Replication sources must not import dumping or banned namespaces.</summary>
    [Fact]
    public Task ReplicationUsesNoDumpingNamespaces() => AssertSourcesDoNotContainAsync(
        [
            "using System.Linq;",
            "using Newtonsoft",
            "using System.Dynamic;",
            "Dump(",
            "Console.Write",
        ]);

    private static async Task AssertSourcesDoNotContainAsync(string[] forbidden, Func<string, bool>? includePath = null)
    {
        var root = Path.Join(RepositoryPaths.FindRepositoryRoot(), "src", "squirix.server", "Cluster", "Replication");
        var paths = new List<string>(Directory.GetFiles(root, "*.cs", SearchOption.TopDirectoryOnly));
        paths.Sort(StringComparer.Ordinal);

        for (var i = 0; i < paths.Count; i++)
        {
            var path = paths[i];
            if (includePath is not null && !includePath(path))
                continue;

            var text = await File.ReadAllTextAsync(path, DefaultCancellationToken);
            for (var markerIndex = 0; markerIndex < forbidden.Length; markerIndex++)
            {
                var marker = forbidden[markerIndex];
                Assert.False(text.Contains(marker, StringComparison.Ordinal), $"{path} contains '{marker}'");
            }
        }
    }
}
