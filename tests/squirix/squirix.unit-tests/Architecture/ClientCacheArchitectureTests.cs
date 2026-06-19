using System;
using System.Collections.Generic;
using System.IO;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using System.Xml.Linq;
using NetArchTest.Rules;
using Squirix.TestKit.IO;
using Xunit;

namespace Squirix.UnitTests.Architecture;

/// <summary>Architecture rules for the client SDK assembly boundary.</summary>
public sealed class ClientCacheArchitectureTests
{
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
        var entryType = ClientArchitecture.MainAssembly.GetType("Squirix.Transport.Grpc.Cache.CacheEntryWire", true)!;

        Assert.Same(ClientArchitecture.MainAssembly, entryType.Assembly);
        Assert.False(entryType.IsPublic);
        Assert.Null(ClientArchitecture.MainAssembly.GetType("Squirix.Transport.Grpc.Cache.SquirixCacheService+SquirixCacheServiceBase", false));
    }

    /// <summary>Ensures the client assembly does not take dependencies on server-owned runtime namespaces.</summary>
    [Fact]
    public void ClientAssemblyShouldNotDependOnServerRuntimeNamespaces()
    {
        foreach (var blockedNamespace in BlockedClientRuntimeNamespaces)
        {
            var result = Types.InAssembly(ClientArchitecture.MainAssembly).ShouldNot().HaveDependencyOn(blockedNamespace).GetResult();

            ArchitectureAssertions.AssertArchitecture(result);
        }
    }

    /// <summary>Ensures the client package does not grant the server assembly access to internal SDK types.</summary>
    [Fact]
    public void ClientAssemblyShouldNotExposeInternalsToSquirixServer()
    {
        var friendAssemblies = new List<string>();
        foreach (var attribute in ClientArchitecture.MainAssembly.GetCustomAttributes<InternalsVisibleToAttribute>())
            friendAssemblies.Add(GetSimpleAssemblyName(attribute.AssemblyName));

        Assert.DoesNotContain(friendAssemblies, static assemblyName => string.Equals(assemblyName, "Squirix.Server", StringComparison.Ordinal));
    }

    /// <summary>Ensures the core package does not reference the server package.</summary>
    [Fact]
    public void ClientAssemblyShouldNotReferenceSquirixServer()
    {
        var references = new List<string>();
        foreach (var assembly in ClientArchitecture.MainAssembly.GetReferencedAssemblies())
            references.Add(assembly.Name!);
        Assert.DoesNotContain("Squirix.Server", references, StringComparer.Ordinal);
    }

    /// <summary>Ensures the basic SDK path generates the narrow KV and expiration transport contract from shared source.</summary>
    [Fact]
    public void ClientProjectShouldGenerateNarrowCacheGrpcTransportContractFromSharedSource()
    {
        XElement? protobuf = null;
        foreach (var element in LoadProject("src/squirix/Squirix.csproj").Descendants())
        {
            if (!string.Equals(element.Name.LocalName, "Protobuf", StringComparison.OrdinalIgnoreCase))
                continue;

            if (!string.Equals(element.Attribute("Include")?.Value, @"..\shared\transport\grpc\Protos\SquirixCache.proto", StringComparison.Ordinal))
                continue;

            protobuf = element;
            break;
        }

        Assert.NotNull(protobuf);
        Assert.Equal("Client", protobuf.Attribute("GrpcServices")?.Value);
        Assert.Equal(@"..\shared\transport\grpc\Protos", protobuf.Attribute("ProtoRoot")?.Value);
        Assert.Equal("Internal", protobuf.Attribute("Access")?.Value);
        Assert.NotNull(ClientArchitecture.MainAssembly.GetType("Squirix.Transport.Grpc.Cache.SquirixCacheService+SquirixCacheServiceClient", false));
    }

    /// <summary>Ensures the client project does not grow server-hosting dependency debt.</summary>
    [Fact]
    public void ClientProjectShouldNotReferenceServerHostingPackages()
    {
        var project = LoadProject("src/squirix/Squirix.csproj");
        var serverPackageReferences = new List<string>();
        foreach (var include in ReadIncludes(project, "PackageReference"))
        {
            if (include.Equals("Grpc.AspNetCore", StringComparison.Ordinal) || include.StartsWith("Microsoft.AspNetCore.", StringComparison.Ordinal))
                serverPackageReferences.Add(include);
        }

        Assert.Empty(serverPackageReferences);

        var serverFrameworkReferences = new List<string>();
        foreach (var include in ReadIncludes(project, "FrameworkReference"))
        {
            if (include.StartsWith("Microsoft.AspNetCore", StringComparison.Ordinal))
                serverFrameworkReferences.Add(include);
        }

        Assert.Empty(serverFrameworkReferences);
    }

    /// <summary>Ensures the core project does not depend on the server project.</summary>
    [Fact]
    public void ClientProjectShouldNotReferenceSquirixServerProject()
    {
        var references = ReadProjectIncludes("src/squirix/Squirix.csproj", "ProjectReference");

        Assert.DoesNotContain(references, static reference => reference.Contains("squirix.server", StringComparison.OrdinalIgnoreCase));
        Assert.DoesNotContain(references, static reference => reference.Contains("Squirix.Server.csproj", StringComparison.OrdinalIgnoreCase));
    }

    /// <summary>Ensures the client API describes owner-local atomic batching without exposing topology terminology.</summary>
    [Fact]
    public void ClientPublicApiShouldNotExposeShardTerminology()
    {
        var offenders = new List<string>();
        foreach (var type in ClientArchitecture.MainAssembly.ExportedTypes)
        {
            if (type.FullName is null || !type.FullName.Contains("Shard", StringComparison.Ordinal))
                continue;

            offenders.Add(type.FullName);
        }

        offenders.Sort(StringComparer.Ordinal);

        Assert.Empty(offenders);
    }

    /// <summary>
    /// Ensures <see cref="ISquirixClient.GetCacheAsync{T}" /> exposes a non-owning cache projection.
    /// </summary>
    [Fact]
    public void GetCacheAsyncReturnsNonOwningCacheHandle()
    {
        var m = typeof(ISquirixClient).GetMethod(nameof(ISquirixClient.GetCacheAsync), [typeof(string), typeof(CancellationToken)]);
        Assert.NotNull(m);
        Assert.True(m.ReturnType.IsGenericType);
        Assert.Equal(typeof(ValueTask<>), m.ReturnType.GetGenericTypeDefinition());
        var arg = Assert.Single(m.ReturnType.GetGenericArguments());
        Assert.True(arg.IsGenericType);
        Assert.Equal(typeof(ICache<>), arg.GetGenericTypeDefinition());
    }

    private static string FindRepositoryRoot()
    {
        var dir = new DirectoryInfo(AppContext.BaseDirectory);
        while (dir is not null)
        {
            if (File.Exists(PathKit.Combine(dir.FullName, "squirix.slnx")))
            {
                return dir.FullName;
            }

            dir = dir.Parent;
        }

        throw new InvalidOperationException("Repository root not found.");
    }

    private static string GetSimpleAssemblyName(string assemblyName)
    {
        var commaIndex = assemblyName.IndexOf(',', StringComparison.Ordinal);
        return commaIndex < 0 ? assemblyName : assemblyName[..commaIndex];
    }

    private static XDocument LoadProject(string relativePath)
    {
        var path = PathKit.Combine(FindRepositoryRoot(), relativePath.Replace('/', Path.DirectorySeparatorChar));
        Assert.True(File.Exists(path), $"Expected project at {path}.");
        return XDocument.Load(path);
    }

    private static string[] ReadIncludes(XDocument project, string itemName)
    {
        var includes = new List<string>();
        foreach (var element in project.Descendants())
        {
            if (!string.Equals(element.Name.LocalName, itemName, StringComparison.OrdinalIgnoreCase))
                continue;

            var value = element.Attribute("Include")?.Value;
            if (string.IsNullOrWhiteSpace(value))
                continue;

            includes.Add(value);
        }

        return includes.ToArray();
    }

    private static string[] ReadProjectIncludes(string projectPath, string itemName) => ReadIncludes(LoadProject(projectPath), itemName);
}
