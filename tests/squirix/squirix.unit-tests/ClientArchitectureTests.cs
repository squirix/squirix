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
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.UnitTests;

/// <summary>Architecture rules for the client SDK assembly boundary.</summary>
[Immutable]
public sealed class ClientArchitectureTests
{
    private const string ClientProjectRelativePath = "src/squirix/Squirix.csproj";
    private static readonly Lazy<string> RepositoryRoot = new(ResolveRepositoryRoot);
    private static MsbuildProjectIndex? _clientProjectIndex;

    /// <summary>Loads the client project model once per test class.</summary>
    [Before(HookType.Class)]
    public static async Task LoadClientProjectAsync()
    {
        var path = PathKit.Combine(RepositoryRoot.Value, ClientProjectRelativePath.Replace('/', Path.DirectorySeparatorChar));
        _ = await Assert.That(File.Exists(path)).IsTrue();

        var document = new XmlDocument();
        document.Load(path);
        _clientProjectIndex = ParseMsbuildProject(document.CreateNavigator()!);
    }

    /// <summary>Ensures the basic SDK path generates the narrow KV and expiration transport contract from shared source.</summary>
    [Test]
    public async Task GeneratesNarrowCacheGrpcFromShared()
    {
        var protobuf = await RequireClientProjectIndex().RequireIncludedElementAsync("Protobuf", @"..\shared\Squirix\Transport\Grpc\Protos\SquirixCache.proto");

        _ = await Assert.That(protobuf.GetAttribute("GrpcServices", string.Empty)).IsEqualTo("Client");
        _ = await Assert.That(protobuf.GetAttribute("ProtoRoot", string.Empty)).IsEqualTo(@"..\shared\Squirix\Transport\Grpc\Protos");
        _ = await Assert.That(protobuf.GetAttribute("Access", string.Empty)).IsEqualTo("Internal");
        _ = typeof(SquirixCacheService.SquirixCacheServiceClient);
    }

    /// <summary>Ensures <see cref="ISquirixClient.GetCacheAsync{T}" /> exposes a non-owning cache projection.</summary>
    [Test]
    public async Task GetCacheAsyncReturnsNonOwningCacheHandle()
    {
        var probe = ProbeAsync;
        _ = await Assert.That(probe.Method.Name).Contains(nameof(ProbeAsync), StringComparison.Ordinal);
        return;

        static ValueTask<ICache<int>> ProbeAsync(ISquirixClient client, string name, CancellationToken cancellationToken)
        {
            return client.GetCacheAsync<int>(name, cancellationToken);
        }
    }

    /// <summary>Ensures the client-generated gRPC CLR transport types remain internal and client-only.</summary>
    [Test]
    public async Task GrpcTransportTypesRemainInternal()
    {
        _ = await Assert.That(typeof(CacheEntryWire).IsPublic).IsFalse();
        _ = await Assert.That(typeof(SquirixCacheService).IsPublic).IsFalse();
        _ = typeof(SquirixCacheService.SquirixCacheServiceClient);
    }

    /// <summary>Ensures the client package does not grant the server assembly access to internal SDK types.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ShouldNotExposeInternalsToServer(CancellationToken cancellationToken)
    {
        var path = PathKit.Combine(PathKit.Combine(RepositoryRoot.Value, "src/squirix/Properties"), "AssemblyInfo.cs");
        var text = await File.ReadAllTextAsync(path, cancellationToken);
        _ = await Assert.That(text).DoesNotContain("InternalsVisibleTo(\"Squirix.Server\"", StringComparison.Ordinal);
    }

    /// <summary>Ensures the client project does not grow server-hosting dependency debt.</summary>
    [Test]
    public async Task ShouldNotReferenceServerHosting()
    {
        var index = RequireClientProjectIndex();
        var packageReferences = index.GetIncludes("PackageReference");
        if (packageReferences != null)
            _ = await Assert.That(packageReferences).DoesNotContain(static include => include.Equals("Grpc.AspNetCore", StringComparison.Ordinal) || include.StartsWith("Microsoft.AspNetCore.", StringComparison.Ordinal));

        var frameworkReferences = index.GetIncludes("FrameworkReference");
        if (frameworkReferences != null)
            _ = await Assert.That(frameworkReferences).DoesNotContain(static include => include.StartsWith("Microsoft.AspNetCore", StringComparison.Ordinal));
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

    private static MsbuildProjectIndex ParseMsbuildProject(XPathNavigator project)
    {
        var includes = new Dictionary<string, List<string>>(StringComparer.OrdinalIgnoreCase);
        var includedElements = new Dictionary<string, List<XPathNavigator>>(StringComparer.OrdinalIgnoreCase);

        CollectMsbuildIncludes(project, includes, includedElements);

        return new MsbuildProjectIndex(includes.ToFrozenDictionary(StringComparer.OrdinalIgnoreCase), includedElements.ToFrozenDictionary(StringComparer.OrdinalIgnoreCase));
    }

    private static MsbuildProjectIndex RequireClientProjectIndex() => _clientProjectIndex ?? ThrowModelNotInitialized();

    private static string ResolveRepositoryRoot()
    {
        var dir = new DirectoryInfo(AppContext.BaseDirectory);
        while (dir != null)
        {
            if (File.Exists(PathKit.Combine(dir.FullName, "squirix.slnx")))
                return dir.FullName;

            dir = dir.Parent;
        }

        throw new InvalidOperationException("Repository root not found.");
    }

    private static MsbuildProjectIndex ThrowModelNotInitialized() => throw new InvalidOperationException("Client project model is not initialized.");

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

        internal async Task<XPathNavigator> RequireIncludedElementAsync(string localName, string include)
        {
            _ = await Assert.That(_includedElements.TryGetValue(localName, out var elements)).IsTrue();

            var elementList = elements!;
            XPathNavigator? match = null;
            for (var i = 0; i < elementList.Count; i++)
            {
                var element = elementList[i];
                if (!string.Equals(element.GetAttribute("Include", string.Empty), include, StringComparison.Ordinal))
                    continue;
                match = element;
                break;
            }

            return await Assert.That(match).IsNotNull();
        }
    }
}
