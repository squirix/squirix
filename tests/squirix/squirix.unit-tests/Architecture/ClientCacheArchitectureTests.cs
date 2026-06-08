using System;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using System.Xml.Linq;
using NetArchTest.Rules;
using Squirix.TestKit.IO;
using Xunit;

namespace Squirix.UnitTests.Architecture;

/// <summary>
/// Architecture rules for the client SDK assembly boundary.
/// </summary>
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

    /// <summary>
    /// Ensures the client-generated gRPC CLR transport types remain internal and client-only.
    /// </summary>
    [Fact]
    public void ClientAssemblyGrpcTransportTypesShouldRemainInternalClientSurface()
    {
        var entryType = ClientArchitecture.MainAssembly.GetType("Squirix.Transport.Grpc.Cache.Entry", true)!;

        Assert.Same(ClientArchitecture.MainAssembly, entryType.Assembly);
        Assert.False(entryType.IsPublic);
        Assert.Null(ClientArchitecture.MainAssembly.GetType("Squirix.Transport.Grpc.Cache.SquirixCacheService+SquirixCacheServiceBase", false));
    }

    /// <summary>
    /// Ensures the client assembly does not take dependencies on server-owned runtime namespaces.
    /// </summary>
    [Fact]
    public void ClientAssemblyShouldNotDependOnServerRuntimeNamespaces()
    {
        foreach (var blockedNamespace in BlockedClientRuntimeNamespaces)
        {
            var result = Types.InAssembly(ClientArchitecture.MainAssembly).ShouldNot().HaveDependencyOn(blockedNamespace).GetResult();

            ArchitectureAssertions.AssertArchitecture(result);
        }
    }

    /// <summary>
    /// Ensures the client project does not grow server-hosting dependency debt.
    /// </summary>
    [Fact]
    public void ClientProjectShouldNotReferenceServerHostingPackages()
    {
        var project = LoadProject("src/squirix/Squirix.csproj");
        var serverPackageReferences = ReadIncludes(project, "PackageReference").Where(static include =>
            include.Equals("Grpc.AspNetCore", StringComparison.Ordinal) || include.StartsWith("Microsoft.AspNetCore.", StringComparison.Ordinal)).ToArray();

        Assert.Empty(serverPackageReferences);

        var serverFrameworkReferences = ReadIncludes(project, "FrameworkReference").Where(static include => include.StartsWith("Microsoft.AspNetCore", StringComparison.Ordinal))
                                                                                   .ToArray();

        Assert.Empty(serverFrameworkReferences);
    }

    /// <summary>
    /// Ensures the client API describes owner-local atomic batching without exposing topology terminology.
    /// </summary>
    [Fact]
    public void ClientPublicApiShouldNotExposeShardTerminology()
    {
        var offenders = ClientArchitecture.MainAssembly.ExportedTypes.Where(static type => type.FullName is not null && type.FullName.Contains("Shard", StringComparison.Ordinal))
                                          .Select(static type => type.FullName).OrderBy(static name => name, StringComparer.Ordinal).ToArray();

        Assert.Empty(offenders);
    }

    /// <summary>
    /// Ensures the client package does not grant the server assembly access to internal SDK types.
    /// </summary>
    [Fact]
    public void ClientAssemblyShouldNotExposeInternalsToSquirixServer()
    {
        var friendAssemblies = ClientArchitecture.MainAssembly.GetCustomAttributes<InternalsVisibleToAttribute>()
                                                 .Select(static attribute => GetSimpleAssemblyName(attribute.AssemblyName)).ToArray();

        Assert.DoesNotContain(friendAssemblies, static assemblyName => string.Equals(assemblyName, "Squirix.Server", StringComparison.Ordinal));
    }

    /// <summary>
    /// Ensures the core package does not reference the server package.
    /// </summary>
    [Fact]
    public void ClientAssemblyShouldNotReferenceSquirixServer()
    {
        var references = ClientArchitecture.MainAssembly.GetReferencedAssemblies().Select(static a => a.Name).ToArray();
        Assert.DoesNotContain("Squirix.Server", references, StringComparer.Ordinal);
    }

    /// <summary>
    /// Ensures the basic SDK path generates the narrow KV and expiration transport contract from shared source.
    /// </summary>
    [Fact]
    public void ClientProjectShouldGenerateNarrowCacheGrpcTransportContractFromSharedSource()
    {
        var protobuf = LoadProject("src/squirix/Squirix.csproj").Descendants().Where(static element => element.Name.LocalName == "Protobuf").SingleOrDefault(static element =>
            string.Equals(element.Attribute("Include")?.Value, @"..\shared\transport\grpc\Protos\SquirixCache.proto", StringComparison.Ordinal));

        Assert.NotNull(protobuf);
        Assert.Equal("Client", protobuf.Attribute("GrpcServices")?.Value);
        Assert.Equal(@"..\shared\transport\grpc\Protos", protobuf.Attribute("ProtoRoot")?.Value);
        Assert.Equal("Internal", protobuf.Attribute("Access")?.Value);
        Assert.NotNull(ClientArchitecture.MainAssembly.GetType("Squirix.Transport.Grpc.Cache.SquirixCacheService+SquirixCacheServiceClient", false));
    }

    /// <summary>
    /// Ensures the core project does not depend on the server project.
    /// </summary>
    [Fact]
    public void ClientProjectShouldNotReferenceSquirixServerProject()
    {
        var references = ReadProjectIncludes("src/squirix/Squirix.csproj", "ProjectReference");

        Assert.DoesNotContain(references, static reference => reference.Contains("squirix.server", StringComparison.OrdinalIgnoreCase));
        Assert.DoesNotContain(references, static reference => reference.Contains("Squirix.Server.csproj", StringComparison.OrdinalIgnoreCase));
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

    private static string[] ReadIncludes(XDocument project, string itemName) =>
    [
        .. project.Descendants().Where(element => element.Name.LocalName == itemName).Select(static element => element.Attribute("Include")?.Value)
                  .Where(static value => !string.IsNullOrWhiteSpace(value)).Select(static value => value!),
    ];

    private static string[] ReadProjectIncludes(string projectPath, string itemName) => ReadIncludes(LoadProject(projectPath), itemName);
}
