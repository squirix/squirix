using System;
using System.Collections.Generic;
using System.IO;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using System.Xml;
using Squirix.Server.Attributes;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Architecture;

/// <summary>
/// Non-NsDepCop architecture scans for replication composition hygiene.
/// Namespace DAG edges are enforced by <c language="csharp">config.nsdepcop</c>, not duplicated here.
/// </summary>
[Immutable]
public sealed class ReplicationDependencyArchitectureTests : ServerUnitTestBase
{
    /// <summary>Hosting composition owns FoundationOnly mapping of the closed replication adapter.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task HostingOwnsReplicationComposition(CancellationToken cancellationToken)
    {
        var root = RepositoryPaths.FindRepositoryRoot();
        var mappingPath = Path.Join(root, "src", "squirix.server", "Node", "Hosting", "SquirixEndpointMapping.cs");
        var compositionPath = Path.Join(root, "src", "squirix.server", "Node", "Hosting", "ServerHostingComposition.cs");

        _ = await Assert.That(File.Exists(mappingPath)).IsTrue().Because($"Expected the endpoint mapping to exist at '{mappingPath}'.");
        _ = await Assert.That(File.Exists(compositionPath)).IsTrue().Because($"Expected the hosting composition to exist at '{compositionPath}'.");

        var mapping = await File.ReadAllTextAsync(mappingPath, cancellationToken);
        var composition = await File.ReadAllTextAsync(compositionPath, cancellationToken);

        // The endpoint mapping maps the closed adapter only on the internal host filter and only under FoundationOnly.
        await AssertRegistrationGuarded(mapping, "app.MapGrpcService<ReplicationServiceAdapter>()", "featureState.FoundationOnly");
        _ = await Assert.That(mapping).DoesNotContain("MapGrpcService<SquirixReplicationServiceAdapter", StringComparison.Ordinal);

        // The hosting composition registers the adapter singleton only when FoundationOnly is enabled.
        await AssertRegistrationGuarded(composition, "AddSingleton(static sp => new SquirixReplicationServiceAdapter(", "args.FoundationOnly");

        // The reverse direction must not exist: Storage.Replication never reaches into hosting composition types.
        var storageReplicationRoot = Path.Join(root, "src", "squirix.server", "Storage", "Replication");
        string[] forbidden =
        [
            "SquirixReplicationServiceAdapter",
            "ServerHostingComposition",
        ];
        foreach (var path in Directory.GetFiles(storageReplicationRoot, "*.cs", SearchOption.TopDirectoryOnly))
        {
            var text = await File.ReadAllTextAsync(path, cancellationToken);
            for (var i = 0; i < forbidden.Length; i++)
                _ = await Assert.That(text).DoesNotContain(forbidden[i], StringComparison.Ordinal);
        }
    }

    /// <summary>
    /// The NsDepCop policy mirrors the replication Namespace DAG without drift: only
    /// <c language="csharp">Cluster.Replication → Storage.Replication</c> is allowed, all neighboring edges are rejected.
    /// </summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task NsDepCopPolicyMatchesReplicationDag(CancellationToken cancellationToken)
    {
        var config = await File.ReadAllTextAsync(Path.Join(RepositoryPaths.FindRepositoryRoot(), "src", "squirix.server", "config.nsdepcop"), cancellationToken);
        var policy = new XmlDocument();
        policy.LoadXml(config);

        // Canonical replacement for the broad Cluster → Storage ban: only Cluster.Replication → Storage.Replication survives.
        _ = await Assert.That(config).Contains(@"Squirix\.Server\.Cluster(?:\.(?!Replication(?:\.|$)).*)?$", StringComparison.Ordinal);
        _ = await Assert.That(config).Contains(@"Squirix\.Server\.Cluster\.Replication(?:\..*)?$", StringComparison.Ordinal);
        _ = await Assert.That(config).Contains(@"Squirix\.Server\.Storage(?:$|\.(?!Replication(?:\.|$)).*)$", StringComparison.Ordinal);

        // Storage.Replication must not depend upward on Node, Adapters, or Cluster.
        const string storageReplicationFrom = @"/^Squirix\.Server\.Storage\.Replication(?:\..*)?$/";
        await AssertDisallowedEdge(policy, storageReplicationFrom, "Squirix.Server.Node.*");
        await AssertDisallowedEdge(policy, storageReplicationFrom, "Squirix.Server.Adapters.*");
        await AssertDisallowedEdge(policy, storageReplicationFrom, "Squirix.Server.Cluster.*");

        var edge = FindEdge(policy, storageReplicationFrom, "Squirix.Server.Cluster.*");
        _ = await Assert.That(edge).IsNotNull();
        var pattern = edge.GetAttribute("From");
        var storageReplicationFromRegex = new Regex(pattern[1..^1], RegexOptions.CultureInvariant, TimeSpan.FromSeconds(1));
        _ = await Assert.That("Squirix.Server.Storage.Replication").Matches(storageReplicationFromRegex);
        _ = await Assert.That("Squirix.Server.Storage.Replication.Subnamespace").Matches(storageReplicationFromRegex);
        _ = await Assert.That("Squirix.Server.Storage.Journaling").DoesNotMatch(storageReplicationFromRegex);
        _ = await Assert.That("Squirix.Server.Storage.ReplicationX").DoesNotMatch(storageReplicationFromRegex);

        // Cluster.Replication must stay free of adapters, hosting, Node.App, routing transport, and cache.
        await AssertDisallowedEdge(policy, "Squirix.Server.Cluster.Replication.*", "Squirix.Server.Adapters.*");
        await AssertDisallowedEdge(policy, "Squirix.Server.Cluster.Replication.*", "Squirix.Server.Node.Hosting.*");
        await AssertDisallowedEdge(policy, "Squirix.Server.Cluster.Replication.*", "Squirix.Server.Node.App.*");
        await AssertDisallowedEdge(policy, "Squirix.Server.Cluster.Replication.*", "Squirix.Server.Cluster.Transport.*");
        await AssertDisallowedEdge(policy, "Squirix.Server.Cluster.Replication.*", "Squirix.Server.LocalCache.*");

        // Node.App must not bypass Cluster.Replication into Storage.Replication.
        await AssertDisallowedEdge(policy, "Squirix.Server.Node.App.*", "Squirix.Server.Storage.Replication.*");
    }

    /// <summary>Cluster.Replication sources must not import dumping or banned namespaces.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public Task ReplicationUsesNoDumpingNamespaces(CancellationToken cancellationToken) => AssertSourcesDoNotContainAsync(
        [
            "using Newtonsoft",
            "System.Dynamic;",
            "Dump(",
            "Console.Write",
        ],
        cancellationToken);

    /// <summary>Cluster.Replication sources must not introduce mutable static fields.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public Task ReplicationUsesNoMutableGlobalState(CancellationToken cancellationToken) => AssertSourcesDoNotContainAsync(
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
        ],
        cancellationToken);

    /// <summary>Cluster.Replication sources must not resolve services through a locator.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public Task ReplicationUsesNoServiceLocator(CancellationToken cancellationToken) => AssertSourcesDoNotContainAsync(
        [
            ".GetService(",
            ".GetRequiredService(",
            "IServiceProvider",
            "IServiceScope",
        ],
        cancellationToken,
        static path => !path.EndsWith("ServiceRegistration.cs", StringComparison.Ordinal));

    /// <summary>The RF1 journal mutation path remains free of replica-group WAL machinery.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task RfOnePipelineDoesNotReferenceGroupWal(CancellationToken cancellationToken)
    {
        var root = RepositoryPaths.FindRepositoryRoot();
        string[] paths =
        [
            Path.Join(root, "src", "squirix.server", "Node", "App", "DurableMutationExecutor.cs"),
            Path.Join(root, "src", "squirix.server", "Node", "App", "Decorators", "JournalLoggingCacheDecorator.cs"),
        ];

        for (var index = 0; index < paths.Length; index++)
        {
            _ = await Assert.That(File.Exists(paths[index])).IsTrue().Because($"Expected the guarded source to exist at '{paths[index]}'.");
            var source = await File.ReadAllTextAsync(paths[index], cancellationToken);
            _ = await Assert.That(source).DoesNotContain("Storage.Replication", StringComparison.Ordinal);
            _ = await Assert.That(source).DoesNotContain("ReplicaCommitCoordinator", StringComparison.Ordinal);
            _ = await Assert.That(source).DoesNotContain("FollowerLog", StringComparison.Ordinal);
        }
    }

    /// <summary>Shared transport sources must not embed server replication logic.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task SharedTransportContainsNoServerLogic(CancellationToken cancellationToken)
    {
        var transportRoot = Path.Join(RepositoryPaths.FindRepositoryRoot(), "src", "shared", "Squirix", "Transport");
        _ = await Assert.That(Directory.Exists(transportRoot)).IsTrue();

        string[] forbidden =
        [
            "Squirix.Server.Cluster.Replication",
            "SquirixReplicationService",
            "FoundationOnly",
            "AppendReplicaEntries",
        ];

        foreach (var path in Directory.GetFiles(transportRoot, "*.cs", SearchOption.AllDirectories))
        {
            var text = await File.ReadAllTextAsync(path, cancellationToken);
            for (var i = 0; i < forbidden.Length; i++)
                _ = await Assert.That(text.Contains(forbidden[i], StringComparison.Ordinal)).IsFalse().Because(path);
        }

        foreach (var path in Directory.GetFiles(transportRoot, "*.proto", SearchOption.AllDirectories))
        {
            var text = await File.ReadAllTextAsync(path, cancellationToken);
            _ = await Assert.That(text).DoesNotContain("SquirixReplicationService", StringComparison.Ordinal);
            _ = await Assert.That(text).DoesNotContain("squirix.replication", StringComparison.Ordinal);
        }
    }

    private static async Task AssertDisallowedEdge(XmlDocument policy, string from, string to)
    {
        var edge = FindEdge(policy, from, to);
        _ = await Assert.That(edge).IsNotNull();
        _ = await Assert.That(string.Equals(edge.LocalName, "Disallowed", StringComparison.Ordinal)).IsTrue()
                        .Because($"Edge from '{from}' to '{to}' must be declared under a <Disallowed> element, found <{edge.LocalName}>.");
    }

    /// <summary>Asserts <paramref name="registration" /> appears inside a branch guarded by <paramref name="guard" />.</summary>
    /// <param name="source">The hosting source text.</param>
    /// <param name="registration">The registration expression that must be present.</param>
    /// <param name="guard">The guard condition that must wrap the registration.</param>
    private static async Task AssertRegistrationGuarded(string source, string registration, string guard)
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

        _ = await Assert.That(registrationIndex >= 0).IsTrue().Because($"Expected the registration '{registration}' to be present.");

        // Walk back to the nearest enclosing if guard and require the FoundationOnly condition on it.
        for (var i = registrationIndex - 1; i >= 0; i--)
        {
            var trimmed = lines[i].TrimStart();
            if (!trimmed.StartsWith("if (", StringComparison.Ordinal))
                continue;

            _ = await Assert.That(trimmed.Contains(guard, StringComparison.Ordinal)).IsTrue().Because($"The registration '{registration}' must be guarded by '{guard}'.");
            return;
        }

        Assert.Fail($"The registration '{registration}' is not inside any guarded branch.");
    }

    /// <summary>Asserts that replication sources do not contain any of the forbidden markers.</summary>
    /// <param name="forbidden">The forbidden source markers.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    /// <param name="includePath">The optional path filter.</param>
    private static async Task AssertSourcesDoNotContainAsync(string[] forbidden, CancellationToken cancellationToken, Func<string, bool>? includePath = null)
    {
        var root = Path.Join(RepositoryPaths.FindRepositoryRoot(), "src", "squirix.server", "Cluster", "Replication");
        var paths = new List<string>(Directory.GetFiles(root, "*.cs", SearchOption.TopDirectoryOnly));
        paths.Sort(StringComparer.Ordinal);

        for (var i = 0; i < paths.Count; i++)
        {
            var path = paths[i];
            if (includePath != null && !includePath(path))
                continue;

            var text = await File.ReadAllTextAsync(path, cancellationToken);
            for (var markerIndex = 0; markerIndex < forbidden.Length; markerIndex++)
            {
                var marker = forbidden[markerIndex];
                _ = await Assert.That(text.Contains(marker, StringComparison.Ordinal)).IsFalse().Because($"{path} contains '{marker}'");
            }
        }
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
