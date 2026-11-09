using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using System.Xml;
using System.Xml.XPath;
using Squirix.Attributes;
using Squirix.Testing;
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
    private static XPathDocument? _clientProject;

    /// <summary>Loads the client project model once per test class.</summary>
    [Before(HookType.Class)]
    public static async Task LoadClientProjectAsync()
    {
        var path = PathKit.Combine(RepositoryRoot.Value, ClientProjectRelativePath.Replace('/', Path.DirectorySeparatorChar));
        _ = await Assert.That(File.Exists(path)).IsTrue();

        using var reader = XmlReader.Create(path);
        _clientProject = new XPathDocument(reader);
    }

    /// <summary>Ensures the basic SDK path generates the narrow KV and expiration transport contract from shared source.</summary>
    [Test]
    public async Task GeneratesNarrowCacheGrpcFromShared()
    {
        var protobuf = await Assert.That(FindIncludedElement("Protobuf", @"..\shared\Squirix\Transport\Grpc\Protos\SquirixCache.proto")).IsNotNull();

        _ = await Assert.That(protobuf.GetAttribute("GrpcServices", string.Empty)).IsEqualTo("Client");
        _ = await Assert.That(protobuf.GetAttribute("ProtoRoot", string.Empty)).IsEqualTo(@"..\shared\Squirix\Transport\Grpc\Protos");
        _ = await Assert.That(protobuf.GetAttribute("Access", string.Empty)).IsEqualTo("Internal");
        _ = typeof(SquirixCacheService.SquirixCacheServiceClient);
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
        var packageReferences = GetIncludes("PackageReference");
        _ = await Assert.That(packageReferences).DoesNotContain(static include => include.Equals("Grpc.AspNetCore", StringComparison.Ordinal) || include.StartsWith("Microsoft.AspNetCore.", StringComparison.Ordinal));

        var frameworkReferences = GetIncludes("FrameworkReference");
        _ = await Assert.That(frameworkReferences).DoesNotContain(static include => include.StartsWith("Microsoft.AspNetCore", StringComparison.Ordinal));
    }

    private static List<string> GetIncludes(string itemName)
    {
        var includes = new List<string>();
        var elements = ProjectElements();
        while (elements.MoveNext())
        {
            var element = elements.Current!;
            if (!string.Equals(element.LocalName, itemName, StringComparison.OrdinalIgnoreCase))
                continue;

            var include = element.GetAttribute("Include", string.Empty);
            if (!string.IsNullOrWhiteSpace(include))
                includes.Add(include);
        }

        return includes;
    }

    private static XPathNavigator? FindIncludedElement(string itemName, string include)
    {
        var elements = ProjectElements();
        while (elements.MoveNext())
        {
            var element = elements.Current!;
            if (string.Equals(element.LocalName, itemName, StringComparison.OrdinalIgnoreCase) &&
                string.Equals(element.GetAttribute("Include", string.Empty), include, StringComparison.Ordinal))
                return element;
        }

        return null;
    }

    private static XPathNodeIterator ProjectElements() =>
        (_clientProject ?? ThrowProjectNotLoaded()).CreateNavigator().SelectDescendants(XPathNodeType.Element, false);

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

    private static XPathDocument ThrowProjectNotLoaded() => throw new InvalidOperationException("Client project model is not initialized.");
}
