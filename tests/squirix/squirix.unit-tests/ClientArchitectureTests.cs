using System;
using System.Collections.Frozen;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using System.Xml;
using System.Xml.XPath;
using Squirix.Attributes;
using Squirix.Client;
using Squirix.TestKit;
using Squirix.Transport.Grpc.Cache;
using Xunit;

namespace Squirix.UnitTests;

/// <summary>Architecture rules for the client SDK assembly boundary.</summary>
[Immutable]
public sealed class ClientArchitectureTests
{
    private const string ClientProjectRelativePath = "src/squirix/Squirix.csproj";
    private static readonly Lazy<string> RepositoryRoot = new(ResolveRepositoryRoot);
    private static readonly Lazy<XPathNavigator> ClientProject = new(LoadClientProject);

    private static readonly Lazy<MsbuildProjectIndex> ClientProjectIndex = new(static () => ParseMsbuildProject(ClientProject.Value));

    /// <summary>Ensures the client-generated gRPC CLR transport types remain internal and client-only.</summary>
    [Fact]
    public void ClientAssemblyGrpcTransportTypesRemainInternal()
    {
        Assert.False(typeof(CacheEntryWire).IsPublic);
        Assert.False(typeof(SquirixCacheService).IsPublic);
        _ = typeof(SquirixCacheService.SquirixCacheServiceClient);
    }

    /// <summary>Ensures the client package does not grant the server assembly access to internal SDK types.</summary>
    [Fact]
    public void ClientAssemblyShouldNotExposeInternalsToServer()
    {
        var path = PathKit.Combine(PathKit.Combine(RepositoryRoot.Value, "src/squirix/Properties"), "AssemblyInfo.cs");
        var text = File.ReadAllText(path);
        Assert.DoesNotContain("InternalsVisibleTo(\"Squirix.Server\"", text, StringComparison.Ordinal);
    }

    /// <summary>Ensures the basic SDK path generates the narrow KV and expiration transport contract from shared source.</summary>
    [Fact]
    public void ClientProjectGeneratesNarrowCacheGrpcFromShared()
    {
        var protobuf = ClientProjectIndex.Value.RequireIncludedElement("Protobuf", @"..\shared\Squirix\Transport\Grpc\Protos\SquirixCache.proto");

        Assert.Equal("Client", protobuf.GetAttribute("GrpcServices", string.Empty));
        Assert.Equal(@"..\shared\Squirix\Transport\Grpc\Protos", protobuf.GetAttribute("ProtoRoot", string.Empty));
        Assert.Equal("Internal", protobuf.GetAttribute("Access", string.Empty));
        _ = typeof(SquirixCacheService.SquirixCacheServiceClient);
    }

    /// <summary>Ensures the client project does not grow server-hosting dependency debt.</summary>
    [Fact]
    public void ClientProjectShouldNotReferenceServerHosting()
    {
        var index = ClientProjectIndex.Value;
        var packageReferences = index.GetIncludes("PackageReference");
        if (packageReferences is not null)
        {
            Assert.DoesNotContain(
                packageReferences,
                static include => include.Equals("Grpc.AspNetCore", StringComparison.Ordinal) || include.StartsWith("Microsoft.AspNetCore.", StringComparison.Ordinal));
        }

        var frameworkReferences = index.GetIncludes("FrameworkReference");
        if (frameworkReferences is not null)
            Assert.DoesNotContain(frameworkReferences, static include => include.StartsWith("Microsoft.AspNetCore", StringComparison.Ordinal));
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

    private static void AddMsbuildInclude(
        Dictionary<string, List<string>> includes,
        Dictionary<string, List<XPathNavigator>> includedElements,
        string localName,
        string include,
        XPathNavigator element)
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

        elementList.Add(element.Clone());
    }

    private static void CollectMsbuildIncludes(XPathNavigator root, Dictionary<string, List<string>> includes, Dictionary<string, List<XPathNavigator>> includedElements)
    {
        var localName = root.LocalName;
        var include = root.GetAttribute("Include", string.Empty);
        if (!string.IsNullOrWhiteSpace(include))
            AddMsbuildInclude(includes, includedElements, localName, include, root);

        var children = root.SelectChildren(XPathNodeType.Element);
        while (children.MoveNext())
            CollectMsbuildIncludes(children.Current!, includes, includedElements);
    }

    private static XPathNavigator LoadClientProject()
    {
        var path = PathKit.Combine(RepositoryRoot.Value, ClientProjectRelativePath.Replace('/', Path.DirectorySeparatorChar));
        Assert.True(File.Exists(path));

        var document = new XmlDocument();
        document.Load(path);
        return document.CreateNavigator()!;
    }

    private static MsbuildProjectIndex ParseMsbuildProject(XPathNavigator project)
    {
        var includes = new Dictionary<string, List<string>>(StringComparer.OrdinalIgnoreCase);
        var includedElements = new Dictionary<string, List<XPathNavigator>>(StringComparer.OrdinalIgnoreCase);

        CollectMsbuildIncludes(project, includes, includedElements);

        return new MsbuildProjectIndex(includes.ToFrozenDictionary(StringComparer.OrdinalIgnoreCase), includedElements.ToFrozenDictionary(StringComparer.OrdinalIgnoreCase));
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

    [Immutable]
    private sealed class MsbuildProjectIndex
    {
        private readonly FrozenDictionary<string, List<XPathNavigator>> _includedElements;
        private readonly FrozenDictionary<string, List<string>> _includes;

        internal MsbuildProjectIndex(FrozenDictionary<string, List<string>> includes, FrozenDictionary<string, List<XPathNavigator>> includedElements)
        {
            _includes = includes;
            _includedElements = includedElements;
        }

        internal List<string>? GetIncludes(string itemName) => _includes.GetValueOrDefault(itemName);

        internal XPathNavigator RequireIncludedElement(string localName, string include)
        {
            Assert.True(_includedElements.TryGetValue(localName, out var elements));

            XPathNavigator? match = null;
            for (var i = 0; i < elements.Count; i++)
            {
                var element = elements[i];
                if (!string.Equals(element.GetAttribute("Include", string.Empty), include, StringComparison.Ordinal))
                    continue;
                match = element;
                break;
            }

            Assert.True(match is not null);
            return match;
        }
    }
}
