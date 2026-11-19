using System;
using System.Collections.Generic;
using System.IO;
using System.Reflection.Metadata;
using System.Reflection.PortableExecutable;
using System.Threading.Tasks;
using System.Xml;
using System.Xml.XPath;
using Squirix.Attributes;
using Squirix.Testing;
using Squirix.Transport.Wire.Cache;
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

    /// <summary>Checks that neither the SDK project nor its compiled assembly references gRPC.</summary>
    [Test]
    public async Task CacheTransportDoesNotReferenceGrpc()
    {
        _ = await Assert.That(GetIncludes("PackageReference")).DoesNotContain(static include => include.StartsWith("Grpc.", StringComparison.Ordinal));
        _ = await Assert.That(GetIncludes("Protobuf")).IsEmpty();

        await using var stream = File.OpenRead(PathKit.Combine(AppContext.BaseDirectory, "Squirix.dll"));
        using var pe = new PEReader(stream);
        var reader = pe.GetMetadataReader();
        foreach (var handle in reader.AssemblyReferences)
        {
            var reference = reader.GetAssemblyReference(handle);
            _ = await Assert.That(reader.GetString(reference.Name).StartsWith("Grpc.", StringComparison.Ordinal)).IsFalse();
        }

        _ = await Assert.That(typeof(CacheEntryWire).IsPublic).IsFalse();
    }

    /// <summary>Ensures the client package does not grant the server assembly access to internal SDK types.</summary>
    [Test]
    public async Task ShouldNotExposeInternalsToServer()
    {
        await using var stream = File.OpenRead(PathKit.Combine(AppContext.BaseDirectory, "Squirix.dll"));
        using var pe = new PEReader(stream);
        var reader = pe.GetMetadataReader();
        var friends = new List<string>();
        foreach (var handle in reader.GetAssemblyDefinition().GetCustomAttributes())
        {
            var attribute = reader.GetCustomAttribute(handle);
            if (attribute.Constructor.Kind != HandleKind.MemberReference)
                continue;
            var parent = reader.GetMemberReference((MemberReferenceHandle)attribute.Constructor).Parent;
            if (parent.Kind != HandleKind.TypeReference)
                continue;
            var type = reader.GetTypeReference((TypeReferenceHandle)parent);
            if (!reader.StringComparer.Equals(type.Namespace, "System.Runtime.CompilerServices") ||
                !reader.StringComparer.Equals(type.Name, "InternalsVisibleToAttribute"))
                continue;

            var value = reader.GetBlobReader(attribute.Value);
            var prolog = Convert.ToInt32(value.ReadUInt16());
            var name = value.ReadSerializedString();
            _ = await Assert.That(prolog).IsEqualTo(1);
            var friend = await Assert.That(name).IsNotNull();
            friends.Add(AssemblyNameInfo.Parse(friend).Name);
        }

        _ = await Assert.That(friends).DoesNotContain("Squirix.Server", StringComparer.OrdinalIgnoreCase);
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
