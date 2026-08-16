using System;
using System.Collections.Generic;
using System.IO;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using System.Xml;
using Squirix.Attributes;
using Squirix.Server.UnitTests.Support;
using Xunit;

namespace Squirix.Server.UnitTests.Architecture;

/// <summary>
/// Non-NsDepCop architecture scans for replication composition hygiene.
/// Namespace DAG edges are enforced by <c>config.nsdepcop</c>, not duplicated here.
/// </summary>
[Immutable]
public sealed class ReplicationDependencyArchitectureTests : ServerUnitTestBase
{
    /// <summary>Hosting composition owns FoundationOnly mapping of the closed replication adapter.</summary>
    [Fact]
    public async Task HostingOwnsReplicationComposition()
    {
        var root = RepositoryPaths.FindRepositoryRoot();
        var mappingPath = Path.Join(root, "src", "squirix.server", "Node", "Hosting", "SquirixEndpointMapping.cs");
        var compositionPath = Path.Join(root, "src", "squirix.server", "Node", "Hosting", "ServerHostingComposition.cs");

        Assert.True(File.Exists(mappingPath), $"Expected the endpoint mapping to exist at '{mappingPath}'.");
        Assert.True(File.Exists(compositionPath), $"Expected the hosting composition to exist at '{compositionPath}'.");

        var mapping = await File.ReadAllTextAsync(mappingPath, DefaultCancellationToken);
        var composition = await File.ReadAllTextAsync(compositionPath, DefaultCancellationToken);

        // The endpoint mapping maps the closed adapter only on the internal host filter and only under FoundationOnly.
        AssertRegistrationGuarded(mapping, "app.MapGrpcService<ReplicationServiceAdapter>()", "featureState.FoundationOnly");
        Assert.DoesNotContain("MapGrpcService<SquirixReplicationServiceAdapter", mapping, StringComparison.Ordinal);

        // The hosting composition registers the adapter singleton only when FoundationOnly is enabled.
        AssertRegistrationGuarded(composition, "AddSingleton(static sp => new SquirixReplicationServiceAdapter(", "args.FoundationOnly");

        // The reverse direction must not exist: Storage.Replication never reaches into hosting composition types.
        var storageReplicationRoot = Path.Join(root, "src", "squirix.server", "Storage", "Replication");
        string[] forbidden =
        [
            "SquirixReplicationServiceAdapter",
            "ServerHostingComposition",
        ];
        foreach (var path in Directory.GetFiles(storageReplicationRoot, "*.cs", SearchOption.TopDirectoryOnly))
        {
            var text = await File.ReadAllTextAsync(path, DefaultCancellationToken);
            for (var i = 0; i < forbidden.Length; i++)
                Assert.DoesNotContain(forbidden[i], text, StringComparison.Ordinal);
        }
    }

    /// <summary>
    /// The NsDepCop policy mirrors the replication Namespace DAG without drift: only
    /// <c>Cluster.Replication → Storage.Replication</c> is allowed, all neighboring edges are rejected.
    /// </summary>
    [Fact]
    public async Task NsDepCopPolicyMatchesReplicationDag()
    {
        var config = await File.ReadAllTextAsync(Path.Join(RepositoryPaths.FindRepositoryRoot(), "src", "squirix.server", "config.nsdepcop"), DefaultCancellationToken);
        var policy = new XmlDocument();
        policy.LoadXml(config);

        // Canonical replacement for the broad Cluster → Storage ban: only Cluster.Replication → Storage.Replication survives.
        Assert.Contains(@"Squirix\.Server\.Cluster(?:\.(?!Replication(?:\.|$)).*)?$", config, StringComparison.Ordinal);
        Assert.Contains(@"Squirix\.Server\.Cluster\.Replication(?:\..*)?$", config, StringComparison.Ordinal);
        Assert.Contains(@"Squirix\.Server\.Storage(?:$|\.(?!Replication(?:\.|$)).*)$", config, StringComparison.Ordinal);

        // Storage.Replication must not depend upward on Node, Adapters, or Cluster.
        const string storageReplicationFrom = @"/^Squirix\.Server\.Storage\.Replication(?:\..*)?$/";
        AssertDisallowedEdge(policy, storageReplicationFrom, "Squirix.Server.Node.*");
        AssertDisallowedEdge(policy, storageReplicationFrom, "Squirix.Server.Adapters.*");
        AssertDisallowedEdge(policy, storageReplicationFrom, "Squirix.Server.Cluster.*");

        var edge = FindEdge(policy, storageReplicationFrom, "Squirix.Server.Cluster.*");
        Assert.NotNull(edge);
        var pattern = edge.GetAttribute("From");
        var storageReplicationFromRegex = new Regex(pattern[1..^1], RegexOptions.CultureInvariant, TimeSpan.FromSeconds(1));
        Assert.Matches(storageReplicationFromRegex, "Squirix.Server.Storage.Replication");
        Assert.Matches(storageReplicationFromRegex, "Squirix.Server.Storage.Replication.Subnamespace");
        Assert.DoesNotMatch(storageReplicationFromRegex, "Squirix.Server.Storage.Journaling");
        Assert.DoesNotMatch(storageReplicationFromRegex, "Squirix.Server.Storage.ReplicationX");

        // Cluster.Replication must stay free of adapters, hosting, Node.App, routing transport, and cache.
        AssertDisallowedEdge(policy, "Squirix.Server.Cluster.Replication.*", "Squirix.Server.Adapters.*");
        AssertDisallowedEdge(policy, "Squirix.Server.Cluster.Replication.*", "Squirix.Server.Node.Hosting.*");
        AssertDisallowedEdge(policy, "Squirix.Server.Cluster.Replication.*", "Squirix.Server.Node.App.*");
        AssertDisallowedEdge(policy, "Squirix.Server.Cluster.Replication.*", "Squirix.Server.Cluster.Transport.*");
        AssertDisallowedEdge(policy, "Squirix.Server.Cluster.Replication.*", "Squirix.Server.LocalCache.*");

        // Node.App must not bypass Cluster.Replication into Storage.Replication.
        AssertDisallowedEdge(policy, "Squirix.Server.Node.App.*", "Squirix.Server.Storage.Replication.*");
    }

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

    private static void AssertDisallowedEdge(XmlDocument policy, string from, string to)
    {
        var edge = FindEdge(policy, from, to);
        Assert.NotNull(edge);
        Assert.True(
            string.Equals(edge.LocalName, "Disallowed", StringComparison.Ordinal),
            $"Edge from '{from}' to '{to}' must be declared under a <Disallowed> element, found <{edge.LocalName}>.");
    }

    /// <summary>Asserts <paramref name="registration" /> appears inside a branch guarded by <paramref name="guard" />.</summary>
    /// <param name="source">The hosting source text.</param>
    /// <param name="registration">The registration expression that must be present.</param>
    /// <param name="guard">The guard condition that must wrap the registration.</param>
    private static void AssertRegistrationGuarded(string source, string registration, string guard)
    {
        var lines = source.Split('\n');
        var registrationIndex = -1;
        for (var i = 0; i < lines.Length; i++)
        {
            if (!lines[i].Contains(registration, StringComparison.Ordinal))
                continue;
            registrationIndex = i;
            break;
        }

        Assert.True(registrationIndex >= 0, $"Expected the registration '{registration}' to be present.");

        // Walk back to the nearest enclosing if guard and require the FoundationOnly condition on it.
        for (var i = registrationIndex - 1; i >= 0; i--)
        {
            var trimmed = lines[i].TrimStart();
            if (!trimmed.StartsWith("if (", StringComparison.Ordinal))
                continue;

            Assert.True(trimmed.Contains(guard, StringComparison.Ordinal), $"The registration '{registration}' must be guarded by '{guard}'.");
            return;
        }

        Assert.Fail($"The registration '{registration}' is not inside any guarded branch.");
    }

    private static XmlElement? FindEdge(XmlDocument policy, string from, string to)
    {
        foreach (XmlNode node in policy.GetElementsByTagName("*"))
        {
            if (node is not XmlElement element)
                continue;

            if (!string.Equals(element.GetAttribute("From"), from, StringComparison.Ordinal) || !string.Equals(element.GetAttribute("To"), to, StringComparison.Ordinal))
                continue;
            return element;
        }

        return null;
    }
}
