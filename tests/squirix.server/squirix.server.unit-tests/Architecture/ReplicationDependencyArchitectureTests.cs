using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using System.Xml.Linq;
using Squirix.Server.UnitTests.Support;
using Xunit;

namespace Squirix.Server.UnitTests.Architecture;

/// <summary>Replication dependency barriers for replication namespaces and protocol-model isolation.</summary>
public sealed class ReplicationDependencyArchitectureTests : ServerUnitTestBase
{
    /// <summary>Ensures the client package never project-references the server package.</summary>
    [Fact]
    public void ClientDoesNotReferenceServer()
    {
        var root = RepositoryPaths.FindRepositoryRoot();
        var clientProj = Path.Join(root, "src", "squirix", "Squirix.csproj");
        var references = LoadProjectReferences(clientProj);
        Assert.DoesNotContain(
            references,
            static path => path.Contains("Squirix.Server.csproj", StringComparison.OrdinalIgnoreCase));
    }

    /// <summary>Ensures product packages do not reference the protocol-model project.</summary>
    [Fact]
    public void ProductDoesNotReferenceProtocolModel()
    {
        var root = RepositoryPaths.FindRepositoryRoot();
        foreach (var project in EnumerateProductProjects(root))
        {
            var references = LoadProjectReferences(project);
            Assert.False(
                references.Exists(static path => path.Contains("Squirix.ProtocolModel", StringComparison.OrdinalIgnoreCase)),
                $"Product project '{project}' must not reference Squirix.ProtocolModel.");
        }
    }

    /// <summary>Ensures the protocol-model project does not reference product packages.</summary>
    [Fact]
    public void ProtocolModelDoesNotReferenceProduct()
    {
        var root = RepositoryPaths.FindRepositoryRoot();
        var modelProj = Path.Join(root, "src", "squirix.protocol-model", "Squirix.ProtocolModel.csproj");
        Assert.True(File.Exists(modelProj), "Protocol model project is required by M8-01.");
        var references = LoadProjectReferences(modelProj);
        Assert.DoesNotContain(
            references,
            static path =>
                path.Contains("src/squirix/", StringComparison.OrdinalIgnoreCase)
                || path.Contains("Squirix.Server.csproj", StringComparison.OrdinalIgnoreCase)
                || path.Contains("Squirix.csproj", StringComparison.OrdinalIgnoreCase));
    }

    /// <summary>Ensures Runtime sources do not depend on Node.Hosting.</summary>
    [Fact]
    public Task RuntimeDoesNotDependOnHosting()
    {
        var repo = RepositoryPaths.FindRepositoryRoot();
        return AssertRootsDoNotReferenceHostingAsync([Path.Join(repo, "src", "squirix.server", "Runtime")]);
    }

    /// <summary>Ensures Runtime and Core domain sources do not depend on Node.Hosting.</summary>
    [Fact]
    public Task RuntimeAndDomainDoNotDependOnHosting()
    {
        var repo = RepositoryPaths.FindRepositoryRoot();
        var roots = new[]
        {
            Path.Join(repo, "src", "squirix.server", "Runtime"),
            Path.Join(repo, "src", "squirix.server", "Core"),
        };

        return AssertRootsDoNotReferenceHostingAsync(roots);
    }

    private static async Task AssertRootsDoNotReferenceHostingAsync(IReadOnlyList<string> roots)
    {
        foreach (var root in roots)
        {
            Assert.True(Directory.Exists(root), $"Expected source root '{root}'.");
            foreach (var path in Directory.GetFiles(root, "*.cs", SearchOption.AllDirectories))
            {
                if (path.Contains($"{Path.DirectorySeparatorChar}obj{Path.DirectorySeparatorChar}", StringComparison.Ordinal))
                    continue;

                var text = await File.ReadAllTextAsync(path, DefaultCancellationToken);
                Assert.False(
                    text.Contains("Squirix.Server.Node.Hosting", StringComparison.Ordinal),
                    $"Domain/runtime file '{path}' must not reference Node.Hosting.");
            }
        }
    }

    private static List<string> LoadProjectReferences(string projectPath)
    {
        var document = XDocument.Load(projectPath);
        var result = new List<string>();
        foreach (var element in document.Descendants("ProjectReference"))
        {
            var include = element.Attribute("Include")?.Value;
            if (!string.IsNullOrWhiteSpace(include))
                result.Add(include.Replace('\\', '/'));
        }

        return result;
    }

    private static IEnumerable<string> EnumerateProductProjects(string repositoryRoot)
    {
        yield return Path.Join(repositoryRoot, "src", "squirix", "Squirix.csproj");
        yield return Path.Join(repositoryRoot, "src", "squirix.server", "Squirix.Server.csproj");
        var host = Path.Join(repositoryRoot, "src", "squirix.server.host", "Squirix.Server.Host.csproj");
        if (File.Exists(host))
            yield return host;
    }
}
