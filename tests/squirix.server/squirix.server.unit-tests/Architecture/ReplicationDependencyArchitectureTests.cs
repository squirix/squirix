using System;
using System.Collections.Generic;
using System.IO;
using System.Reflection;
using System.Reflection.Metadata;
using System.Reflection.PortableExecutable;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using System.Xml;
using Squirix.Server.Attributes;
using Squirix.Server.UnitTests.SourceScanning;
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
    /// <summary>Checks the replication namespace dependency boundaries using the configured patterns.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task NsDepCopPolicyMatchesReplicationDag(CancellationToken cancellationToken)
    {
        var config = await File.ReadAllTextAsync(Path.Join(RepositoryPaths.FindRepositoryRoot(), "src", "squirix.server", "config.nsdepcop"), cancellationToken);
        var policy = new XmlDocument();
        policy.LoadXml(config);

        await AssertEdgeAsync(policy, "Squirix.Server.Cluster", "Squirix.Server.Storage.Journaling", true);
        await AssertEdgeAsync(policy, "Squirix.Server.Cluster.Transport", "Squirix.Server.Storage.Replication", true);
        await AssertEdgeAsync(policy, "Squirix.Server.Cluster.Replication", "Squirix.Server.Storage.Journaling", true);
        await AssertEdgeAsync(policy, "Squirix.Server.Cluster.Replication.Child", "Squirix.Server.Storage.Replication.Child", false);
        await AssertEdgeAsync(policy, "Squirix.Server.Cluster.Replication", "Squirix.Server.Storage.Replication", false);
        await AssertEdgeAsync(policy, "Squirix.Server.Storage.Replication", "Squirix.Server.Node.Services", true);
        await AssertEdgeAsync(policy, "Squirix.Server.Storage.Replication.Child", "Squirix.Server.Adapters.Grpc", true);
        await AssertEdgeAsync(policy, "Squirix.Server.Storage.Replication", "Squirix.Server.Cluster.Replication", true);
        await AssertEdgeAsync(policy, "Squirix.Server.Cluster.Replication", "Squirix.Server.Adapters.Grpc", true);
        await AssertEdgeAsync(policy, "Squirix.Server.Cluster.Replication", "Squirix.Server.Node.Hosting", true);
        await AssertEdgeAsync(policy, "Squirix.Server.Cluster.Replication", "Squirix.Server.Node.App", true);
        await AssertEdgeAsync(policy, "Squirix.Server.Cluster.Replication", "Squirix.Server.Cluster.Transport", true);
        await AssertEdgeAsync(policy, "Squirix.Server.Cluster.Replication", "Squirix.Server.LocalCache", true);
        await AssertEdgeAsync(policy, "Squirix.Server.Node.App", "Squirix.Server.Storage.Replication", true);
        await AssertEdgeAsync(policy, "Squirix.Server.Node.App", "Squirix.Server.Cluster.Replication", true);
        await AssertEdgeAsync(policy, "Squirix.Server.Node.App.Helpers", "Squirix.Server.Cluster.Replication.Child", true);
        await AssertEdgeAsync(policy, "Squirix.Server.Node.App.Decorators", "Squirix.Server.Storage.Replication.Child", true);
    }

    /// <summary>Replication sources must not contain debug output or dumping dependencies.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public Task ReplicationUsesNoDumpingNamespaces(CancellationToken cancellationToken) => AssertSourcesDoNotMatchAsync(
        ReplicationSourceRules.DebugOutput, cancellationToken);

    /// <summary>Replication sources must not declare writable static fields.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ReplicationHasNoWritableStaticFields(CancellationToken cancellationToken)
    {
        await using var stream = File.OpenRead(Path.Join(AppContext.BaseDirectory, "Squirix.Server.dll"));
        using var pe = new PEReader(stream);
        var metadata = pe.GetMetadataReader();
        var violations = new List<string>();
        foreach (var handle in metadata.TypeDefinitions)
        {
            cancellationToken.ThrowIfCancellationRequested();
            var type = metadata.GetTypeDefinition(handle);
            var typeName = metadata.GetString(type.Name);
            if (typeName.StartsWith('<'))
                continue;

            var owner = type;
            while (owner.Namespace.IsNil && !owner.GetDeclaringType().IsNil)
                owner = metadata.GetTypeDefinition(owner.GetDeclaringType());

            var ns = metadata.GetString(owner.Namespace);
            if (!string.Equals(ns, "Squirix.Server.Cluster.Replication", StringComparison.Ordinal) && !ns.StartsWith("Squirix.Server.Cluster.Replication.", StringComparison.Ordinal))
                continue;

            foreach (var fieldHandle in type.GetFields())
            {
                var field = metadata.GetFieldDefinition(fieldHandle);
                var fieldName = metadata.GetString(field.Name);
                if ((field.Attributes & (FieldAttributes.Static | FieldAttributes.InitOnly | FieldAttributes.Literal)) == FieldAttributes.Static)
                    violations.Add($"{ns}.{typeName}.{fieldName}");
            }
        }

        _ = await Assert.That(violations).IsEmpty().Because(string.Join(Environment.NewLine, violations));
    }

    /// <summary>Replication sources must not resolve services through a locator.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public Task ReplicationUsesNoServiceLocator(CancellationToken cancellationToken) => AssertSourcesDoNotMatchAsync(
        ReplicationSourceRules.ServiceLocator, cancellationToken);

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

    private static async Task AssertEdgeAsync(XmlDocument policy, string from, string to, bool expectedDisallowed)
    {
        var disallowed = false;
        foreach (XmlNode node in policy.GetElementsByTagName("Disallowed"))
        {
            if (node is XmlElement edge && NamespaceMatches(edge.GetAttribute("From"), from) && NamespaceMatches(edge.GetAttribute("To"), to))
            {
                disallowed = true;
                break;
            }
        }

        _ = await Assert.That(disallowed).IsEqualTo(expectedDisallowed).Because($"Dependency '{from}' to '{to}' has an unexpected policy.");
    }

    private static bool NamespaceMatches(string pattern, string name)
    {
        if (pattern.Length >= 2 && pattern[0] == '/' && pattern[^1] == '/')
            return Regex.IsMatch(name, pattern[1..^1], RegexOptions.CultureInvariant, TimeSpan.FromSeconds(1));
        if (string.Equals(pattern, "*", StringComparison.Ordinal))
            return true;
        if (pattern.EndsWith(".*", StringComparison.Ordinal))
        {
            var prefix = pattern[..^2];
            return string.Equals(name, prefix, StringComparison.Ordinal) || name.StartsWith(prefix + ".", StringComparison.Ordinal);
        }

        return string.Equals(pattern, name, StringComparison.Ordinal);
    }

    private static async Task AssertSourcesDoNotMatchAsync(Regex pattern, CancellationToken cancellationToken)
    {
        var paths = await ServerSourceFiles.EnumerateCsharpFilesAsync("Cluster", "Replication");
        foreach (var path in paths)
        {
            var source = await File.ReadAllTextAsync(path, cancellationToken);
            _ = await Assert.That(ReplicationSourceRules.HasViolation(pattern, source)).IsFalse().Because(path);
        }
    }
}
