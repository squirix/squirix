using System;
using System.Collections.Frozen;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using System.Xml.Linq;
using Squirix.TestKit.IO;
using Squirix.Transport.Grpc.Cache;
using Xunit;

namespace Squirix.UnitTests.Architecture;

/// <summary>Architecture rules for the client SDK assembly boundary.</summary>
public sealed class ArchitectureTests
{
    private const string ClientProjectRelativePath = "src/squirix/Squirix.csproj";

    private static readonly Lazy<string> RepositoryRoot = new(ResolveRepositoryRoot);

    private static readonly Lazy<XDocument> ClientProject = new(LoadClientProject);

    private static readonly Lazy<MsbuildProjectIndex> ClientProjectIndex = new(static () => MsbuildProjectIndex.Parse(ClientProject.Value));

    private static readonly string[] BlockedClientRuntimeNamespaces =
    [
        "Squirix.Server.Adapters",
        "Squirix.Server.LocalCache",
        "Squirix.Server.Node",
        "Squirix.Server.Storage",
        "Squirix.Server.Runtime",
    ];

    /// <summary>Ensures the client-generated gRPC CLR transport types remain internal and client-only.</summary>
    [Fact]
    public void ClientAssemblyGrpcTransportTypesShouldRemainInternalClientSurface()
    {
        Assert.False(typeof(CacheEntryWire).IsPublic);
        Assert.False(typeof(SquirixCacheService).IsPublic);
        _ = typeof(SquirixCacheService.SquirixCacheServiceClient);
    }

    /// <summary>Ensures the client assembly does not take dependencies on server-owned runtime namespaces.</summary>
    [Fact]
    public void ClientAssemblyShouldNotDependOnServerRuntimeNamespaces()
    {
        foreach (var blockedNamespace in BlockedClientRuntimeNamespaces)
        {
            var result = SdkArchitectureScope.Sdk.ShouldNot().HaveDependencyOn(blockedNamespace).GetResult();
            ArchitectureAssertions.AssertArchitecture(result);
        }
    }

    /// <summary>Ensures the client package does not grant the server assembly access to internal SDK types.</summary>
    [Fact]
    public void ClientAssemblyShouldNotExposeInternalsToSquirixServer()
    {
        var assemblyInfoPath = PathKit.Combine(PathKit.Combine(RepositoryRoot.Value, "src/squirix/Properties"), "AssemblyInfo.cs");
        var text = File.ReadAllText(assemblyInfoPath);
        Assert.DoesNotContain("InternalsVisibleTo(\"Squirix.Server\"", text, StringComparison.Ordinal);
    }

    /// <summary>Ensures the core package does not reference the server package.</summary>
    [Fact]
    public void ClientAssemblyShouldNotReferenceSquirixServer()
    {
        var references = ClientProjectIndex.Value.GetIncludes("ProjectReference");
        Assert.DoesNotContain(references, static reference => reference.Contains("Squirix.Server.csproj", StringComparison.OrdinalIgnoreCase));
    }

    /// <summary>Ensures the basic SDK path generates the narrow KV and expiration transport contract from shared source.</summary>
    [Fact]
    public void ClientProjectShouldGenerateNarrowCacheGrpcTransportContractFromSharedSource()
    {
        var protobuf = ClientProjectIndex.Value.RequireIncludedElement("Protobuf", @"..\shared\transport\grpc\Protos\SquirixCache.proto");

        Assert.Equal("Client", protobuf.Attribute("GrpcServices")?.Value);
        Assert.Equal(@"..\shared\transport\grpc\Protos", protobuf.Attribute("ProtoRoot")?.Value);
        Assert.Equal("Internal", protobuf.Attribute("Access")?.Value);
        _ = typeof(SquirixCacheService.SquirixCacheServiceClient);
    }

    /// <summary>Ensures the client project does not grow server-hosting dependency debt.</summary>
    [Fact]
    public void ClientProjectShouldNotReferenceServerHostingPackages()
    {
        var index = ClientProjectIndex.Value;

        Assert.DoesNotContain(
            index.GetIncludes("PackageReference"),
            static include => include.Equals("Grpc.AspNetCore", StringComparison.Ordinal)
                || include.StartsWith("Microsoft.AspNetCore.", StringComparison.Ordinal));

        Assert.DoesNotContain(
            index.GetIncludes("FrameworkReference"),
            static include => include.StartsWith("Microsoft.AspNetCore", StringComparison.Ordinal));
    }

    /// <summary>Ensures the core project does not depend on the server project.</summary>
    [Fact]
    public void ClientProjectShouldNotReferenceSquirixServerProject()
    {
        var references = ClientProjectIndex.Value.GetIncludes("ProjectReference");

        Assert.DoesNotContain(
            references,
            static reference => reference.Contains("squirix.server", StringComparison.OrdinalIgnoreCase)
                || reference.Contains("Squirix.Server.csproj", StringComparison.OrdinalIgnoreCase));
    }

    /// <summary>
    /// Ensures <see cref="ISquirixClient.GetCacheAsync{T}" /> exposes a non-owning cache projection.
    /// </summary>
    [Fact]
    public void GetCacheAsyncReturnsNonOwningCacheHandle()
    {
        Assert.NotNull((Func<ISquirixClient, string, CancellationToken, ValueTask<ICache<int>>>)ProbeAsync);
        return;

        static ValueTask<ICache<int>> ProbeAsync(ISquirixClient client, string name, CancellationToken cancellationToken)
        {
            return client.GetCacheAsync<int>(name, cancellationToken);
        }
    }

    private static string ResolveRepositoryRoot()
    {
        var dir = new DirectoryInfo(AppContext.BaseDirectory);
        while (dir is not null)
        {
            if (File.Exists(PathKit.Combine(dir.FullName, "squirix.slnx")))
                return dir.FullName;

            dir = dir.Parent;
        }

        throw new InvalidOperationException("Repository root not found.");
    }

    private static XDocument LoadClientProject()
    {
        var path = PathKit.Combine(RepositoryRoot.Value, ClientProjectRelativePath.Replace('/', Path.DirectorySeparatorChar));
        Assert.True(File.Exists(path));
        return XDocument.Load(path);
    }

    private sealed class MsbuildProjectIndex
    {
        private readonly FrozenDictionary<string, List<string>> _includes;
        private readonly FrozenDictionary<string, List<XElement>> _includedElements;

        private MsbuildProjectIndex(FrozenDictionary<string, List<string>> includes, FrozenDictionary<string, List<XElement>> includedElements)
        {
            _includes = includes;
            _includedElements = includedElements;
        }

        public static MsbuildProjectIndex Parse(XDocument project)
        {
            var includes = new Dictionary<string, List<string>>(StringComparer.OrdinalIgnoreCase);
            var includedElements = new Dictionary<string, List<XElement>>(StringComparer.OrdinalIgnoreCase);

            CollectIncludes(project.Root, includes, includedElements);

            return new MsbuildProjectIndex(includes.ToFrozenDictionary(StringComparer.OrdinalIgnoreCase), includedElements.ToFrozenDictionary(StringComparer.OrdinalIgnoreCase));
        }

        public List<string> GetIncludes(string itemName) => _includes.TryGetValue(itemName, out var list) ? list : [];

        public XElement RequireIncludedElement(string localName, string include)
        {
            Assert.True(_includedElements.TryGetValue(localName, out var elements), $"Expected MSBuild element '{localName}' with Include='{include}'.");

            XElement? match = null;
            for (var i = 0; i < elements.Count; i++)
            {
                var element = elements[i];
                if (!string.Equals(element.Attribute("Include")?.Value, include, StringComparison.Ordinal))
                    continue;
                match = element;
                break;
            }

            Assert.True(match is not null, $"Expected MSBuild element '{localName}' with Include='{include}'.");
            return match;
        }

        private static void AddInclude(
            Dictionary<string, List<string>> includes,
            Dictionary<string, List<XElement>> includedElements,
            string localName,
            string include,
            XElement element)
        {
            if (!includes.TryGetValue(localName, out var includeList))
            {
                includeList = [];
                includes[localName] = includeList;
            }

            includeList.Add(include);

            if (!includedElements.TryGetValue(localName, out var elementList))
            {
                elementList = [];
                includedElements[localName] = elementList;
            }

            elementList.Add(element);
        }

        private static void CollectIncludes(
            XElement? root,
            Dictionary<string, List<string>> includes,
            Dictionary<string, List<XElement>> includedElements)
        {
            if (root is null)
                return;

            var localName = root.Name.LocalName;
            var include = root.Attribute("Include")?.Value;
            if (!string.IsNullOrWhiteSpace(include))
                AddInclude(includes, includedElements, localName, include, root);

            for (var node = root.FirstNode; node is not null; node = node.NextNode)
            {
                if (node is XElement child)
                    CollectIncludes(child, includes, includedElements);
            }
        }
    }
}
