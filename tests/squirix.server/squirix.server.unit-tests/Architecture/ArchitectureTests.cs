using System;
using System.Collections.Generic;
using System.IO;
using System.Reflection;
using System.Threading.Tasks;
using System.Xml.Linq;
using NetArchTest.Rules;
using Squirix.Server.TestKit.IO;
using Squirix.Server.UnitTests.Support;
using Xunit;

namespace Squirix.Server.UnitTests.Architecture;

/// <summary>Enforces high-value architectural dependency boundaries for the main Squirix assembly.</summary>
public sealed class ArchitectureTests : UnitTestBase
{
    private static readonly string[] ForbiddenSharedGrpcTransportMapperRuntimeMarkers =
    [
        "ICacheRuntime",
        "ILogicalNamespacedCache",
        "ICacheApi<",
        "LocalCache<",
        "ClusteredCache<",
        "JournalCoordinator",
        "SnapshotCoordinator",
        "Squirix.Storage.Journaling",
        "Squirix.Storage.Snapshot",
        "Squirix.Runtime",
    ];

    private static readonly string[] KnownServerFrameworkDependencyBaseline =
    [
        "Microsoft.AspNetCore.App",
    ];

    private static readonly string[] KnownServerPackageDependencyBaseline =
    [
        "Grpc.AspNetCore",
        "Microsoft.AspNetCore.Authentication.JwtBearer",
    ];

    /// <summary>Ensures transport adapters do not take dependencies on low-level journal JSON internals.</summary>
    [Fact]
    public void AdaptersShouldNotDependOnJournalJsonInternals()
    {
        var result = Types.InAssembly(SquirixArchitecture.ServerAssembly).That().ResideInNamespaceStartingWith(ServerArchitectureNamespaces.Adapters).ShouldNot()
                          .HaveDependencyOn($"{ServerArchitectureNamespaces.Storage}.Journaling.Json").GetResult();

        ArchitectureAssertions.AssertArchitecture(result);
    }

    /// <summary>Ensures client and server projects compile the same shared gRPC transport mapper sources.</summary>
    [Fact]
    public void ClientAndServerProjectsShouldCompileSharedGrpcTransportMappersFromSameSources()
    {
        string[] expectedIncludes =
        [
            @"..\shared\transport\grpc\Mappers\GrpcStaleOwnerMarkers.cs",
        ];

        var serverIncludes = ReadProjectCompileIncludes("src/squirix.server/Squirix.Server.csproj");

        foreach (var include in expectedIncludes)
            Assert.Contains(include, serverIncludes, StringComparer.Ordinal);
    }

    /// <summary>Ensures filter types stay at the REST adapter boundary.</summary>
    [Fact]
    public void FilterTypesShouldLiveInAdaptersRestNamespace()
    {
        var result = ArchitectureNetArchRules.EvaluateShouldResideInOneOfNamespaces(
            Types.InAssembly(SquirixArchitecture.ServerAssembly).That().HaveNameEndingWith("Filter", StringComparison.InvariantCulture),
            [$"{ServerArchitectureNamespaces.Adapters}.Rest", $"{ServerArchitectureNamespaces.Adapters}.Endpoint.Rest"]);

        ArchitectureAssertions.AssertArchitecture(result);
    }

    /// <summary>Ensures handler types stay in the hosting security boundary.</summary>
    [Fact]
    public void HandlerTypesShouldLiveInNodeHostingSecurityNamespace()
    {
        var result = Types.InAssembly(SquirixArchitecture.ServerAssembly).That().HaveNameEndingWith("Handler", StringComparison.InvariantCulture).Should()
                          .ResideInNamespace($"{ServerArchitectureNamespaces.Node}.Hosting.Security").GetResult();

        ArchitectureAssertions.AssertArchitecture(result);
    }

    /// <summary>Ensures the journal thread is joined during disposal instead of being fire-and-forget.</summary>
    [Fact]
    public async Task JournalThreadShouldBeJoinedOnDispose()
    {
        var root = ArchitectureRepositoryPaths.FindRepositoryRoot();
        var text = await File.ReadAllTextAsync(PathKit.Combine(root, "src", "squirix.server", "Storage", "Journaling", "JournalCoordinator.cs"), DefaultCancellationToken);

        Assert.Contains("_journalThread.Join(", text, StringComparison.Ordinal);
        Assert.Contains("AwaitJournalThreadDuringDisposeAsync", text, StringComparison.Ordinal);
    }

    /// <summary>Ensures metrics types stay centralized in the observability namespace.</summary>
    [Fact]
    public void MetricsTypesShouldLiveInObservabilityNamespace()
    {
        var result = Types.InAssembly(SquirixArchitecture.ServerAssembly).That().HaveNameEndingWith("Metrics", StringComparison.InvariantCulture).And().AreNotInterfaces().Should()
                          .ResideInNamespace($"{ServerArchitectureNamespaces.Node}.Observability").GetResult();

        ArchitectureAssertions.AssertArchitecture(result);
    }

    /// <summary>Ensures backpressure controls stay isolated from storage concerns.</summary>
    [Fact]
    public void NodeBackpressureShouldNotDependOnStorage()
    {
        var result = Types.InAssembly(SquirixArchitecture.ServerAssembly).That().ResideInNamespaceStartingWith($"{ServerArchitectureNamespaces.Node}.Backpressure").ShouldNot()
                          .HaveDependencyOn(ServerArchitectureNamespaces.Storage).GetResult();

        ArchitectureAssertions.AssertArchitecture(result);
    }

    /// <summary>Ensures node services remain application-layer components and do not depend on transport adapters.</summary>
    [Fact]
    public void NodeServicesShouldNotDependOnAdapters()
    {
        var result = Types.InAssembly(SquirixArchitecture.ServerAssembly).That().ResideInNamespaceStartingWith($"{ServerArchitectureNamespaces.Node}.Services").ShouldNot()
                          .HaveDependencyOn(ServerArchitectureNamespaces.Adapters).GetResult();

        ArchitectureAssertions.AssertArchitecture(result);
    }

    /// <summary>Ensures observability remains transport-agnostic and reusable across adapters.</summary>
    [Fact]
    public void ObservabilityShouldNotDependOnAdapters()
    {
        var result = Types.InAssembly(SquirixArchitecture.ServerAssembly).That().ResideInNamespaceStartingWith($"{ServerArchitectureNamespaces.Node}.Observability").ShouldNot()
                          .HaveDependencyOn(ServerArchitectureNamespaces.Adapters).GetResult();

        ArchitectureAssertions.AssertArchitecture(result);
    }

    /// <summary>Ensures configuration option types live only in approved configuration namespaces.</summary>
    [Fact]
    public void OptionsTypesShouldLiveInApprovedNamespaces()
    {
        var serverResult = ArchitectureNetArchRules.EvaluateShouldResideInOneOfNamespaces(
            Types.InAssembly(SquirixArchitecture.ServerAssembly).That().HaveNameEndingWith("Options", StringComparison.InvariantCulture),
            ArchitectureAllowlists.ServerOptionsTypeNamespaces);

        ArchitectureAssertions.AssertArchitecture(serverResult);
    }

    /// <summary>Ensures product code does not use access-check bypass attributes.</summary>
    [Fact]
    public async Task ProductionSourcesShouldNotUseIgnoresAccessChecksTo()
    {
        var root = PathKit.Combine(ArchitectureRepositoryPaths.FindRepositoryRoot(), "src");
        var objMarker = $"{Path.DirectorySeparatorChar}obj{Path.DirectorySeparatorChar}";
        var paths = new List<string>();
        foreach (var path in Directory.EnumerateFiles(root, "*.cs", SearchOption.AllDirectories))
            paths.Add(path);

        paths.Sort(StringComparer.Ordinal);
        var offenders = new List<string>();
        foreach (var path in paths)
        {
            if (path.Contains(objMarker, StringComparison.Ordinal))
                continue;

            var text = await File.ReadAllTextAsync(path, DefaultCancellationToken);
            if (text.Contains("IgnoresAccessChecksTo", StringComparison.Ordinal))
                offenders.Add(path);
        }

        Assert.Empty(offenders);
    }

    /// <summary>Ensures repository projects and sources do not hide dependencies with global or implicit usings.</summary>
    [Fact]
    public void RepositoryShouldNotUseGlobalOrImplicitUsings()
    {
        var root = ArchitectureRepositoryPaths.FindRepositoryRoot();
        var sourceOffenders = new List<string>();
        foreach (var path in Directory.EnumerateFiles(root, "*.cs", SearchOption.AllDirectories))
        {
            if (IsGeneratedOutputPath(path))
                continue;

            if (Path.GetFileName(path).Equals("GlobalUsings.cs", StringComparison.OrdinalIgnoreCase))
            {
                sourceOffenders.Add(Path.GetRelativePath(root, path).Replace(Path.DirectorySeparatorChar, '/'));
                continue;
            }

            var hasGlobalUsing = false;
            foreach (var line in File.ReadLines(path))
            {
                if (!line.TrimStart().StartsWith("global using ", StringComparison.Ordinal))
                    continue;

                hasGlobalUsing = true;
                break;
            }

            if (hasGlobalUsing)
                sourceOffenders.Add(Path.GetRelativePath(root, path).Replace(Path.DirectorySeparatorChar, '/'));
        }

        sourceOffenders.Sort(StringComparer.Ordinal);

        var projectOffenders = new List<string>();
        foreach (var path in Directory.EnumerateFiles(root, "*.csproj", SearchOption.AllDirectories))
        {
            if (IsGeneratedOutputPath(path))
                continue;

            var hasImplicitUsings = false;
            foreach (var element in LoadProjectByAbsolutePath(path).Descendants())
            {
                if (!string.Equals(element.Name.LocalName, "ImplicitUsings", StringComparison.OrdinalIgnoreCase))
                    continue;

                if (!element.Value.Trim().Equals("enable", StringComparison.OrdinalIgnoreCase))
                    continue;

                hasImplicitUsings = true;
                break;
            }

            if (hasImplicitUsings)
                projectOffenders.Add(Path.GetRelativePath(root, path).Replace(Path.DirectorySeparatorChar, '/'));
        }

        projectOffenders.Sort(StringComparer.Ordinal);

        Assert.Empty(sourceOffenders);
        Assert.Empty(projectOffenders);
    }

    /// <summary>Ensures the server assembly generates server-side gRPC service bases from the shared transport namespace.</summary>
    [Fact]
    public void ServerAssemblyShouldGenerateGrpcServiceBaseFromSharedTransportNamespace()
    {
        var entryType = SquirixArchitecture.ServerAssembly.GetType("Squirix.Transport.Grpc.Cache.CacheEntryWire", true)!;
        var serviceType = SquirixArchitecture.ServerAssembly.GetType("Squirix.Transport.Grpc.Cache.SquirixCacheService", true)!;
        var serviceBaseType = SquirixArchitecture.ServerAssembly.GetType("Squirix.Transport.Grpc.Cache.SquirixCacheService+SquirixCacheServiceBase", true)!;

        Assert.Same(SquirixArchitecture.ServerAssembly, entryType.Assembly);
        Assert.Same(SquirixArchitecture.ServerAssembly, serviceType.Assembly);
        Assert.Same(SquirixArchitecture.ServerAssembly, serviceBaseType.Assembly);
        Assert.False(entryType.IsPublic);
        Assert.False(serviceType.IsPublic);
        Assert.False(serviceBaseType.IsPublic);
    }

    /// <summary>Ensures the server package does not reference the client SDK assembly.</summary>
    [Fact]
    public void ServerAssemblyShouldNotReferenceSquirix()
    {
        var references = new List<string>();
        foreach (var assembly in SquirixArchitecture.ServerAssembly.GetReferencedAssemblies())
            references.Add(assembly.Name!);
        Assert.DoesNotContain("Squirix", references, StringComparer.Ordinal);
    }

    /// <summary>Ensures standalone server bootstrap starts through the public ASP.NET Core hosting extensions.</summary>
    [Fact]
    public async Task ServerBootstrapSourcesShouldUseServerPackageHostStartupApi()
    {
        var sources = await ReadServerBootstrapSourceTextsAsync();
        var combined = string.Join(Environment.NewLine, Array.ConvertAll(sources, static source => source.Text));

        Assert.Contains("AddSquirixServerAsync", combined, StringComparison.Ordinal);
        Assert.Contains("MapSquirixServer", combined, StringComparison.Ordinal);
    }

    /// <summary>Ensures the standalone process host stays separate from the packable server runtime.</summary>
    [Fact]
    public void ServerHostProjectShouldBePackableGlobalToolExecutable()
    {
        var project = LoadProject("src/squirix.server.host/Squirix.Server.Host.csproj");

        Assert.Equal("net10.0", ReadProperty(project, "TargetFramework"));
        Assert.Equal("Exe", ReadProperty(project, "OutputType"));
        Assert.Equal("Squirix.Server.Host", ReadProperty(project, "AssemblyName"));
        Assert.Equal("Squirix.Server.Host", ReadProperty(project, "RootNamespace"));
        Assert.Equal("true", ReadProperty(project, "IsPackable"));
        Assert.Equal("true", ReadProperty(project, "PackAsTool"));
        Assert.Equal("squirix-server", ReadProperty(project, "ToolCommandName"));
        Assert.Equal("$(SquirixPackageVersion)", ReadProperty(project, "Version"));
        Assert.Equal("$(SquirixPackageVersion)", ReadProperty(project, "PackageVersion"));
        Assert.Equal([@"..\squirix.server\Squirix.Server.csproj"], ReadIncludes(project, "ProjectReference"));
    }

    /// <summary>Ensures InternalsVisibleTo grants match the approved server allowlist.</summary>
    [Fact]
    public async Task ServerInternalsVisibleToShouldMatchApprovedAllowlist()
    {
        string[] approved =
        [
            "Squirix.Server.UnitTests",
            "Squirix.Server.PropertyTests",
            "Squirix.Server.IntegrationTests",
            "Squirix.Server.SmokeTests",
            "Squirix.Server.TestKit",
            "Squirix.Server.Benchmarks",
            "squirix-test-host",
            "sqr-ring-distribution",
            "DynamicProxyGenAssembly2",
        ];

        var root = ArchitectureRepositoryPaths.FindRepositoryRoot();
        var assemblyInfoPath = PathKit.Combine(root, "src", "squirix.server", "Properties", "AssemblyInfo.cs");
        var text = await File.ReadAllTextAsync(assemblyInfoPath, DefaultCancellationToken);
        var granted = new List<string>();
        var index = 0;
        while ((index = text.IndexOf("InternalsVisibleTo(\"", index, StringComparison.Ordinal)) >= 0)
        {
            index += "InternalsVisibleTo(\"".Length;
            var end = text.IndexOf('"', index);
            granted.Add(text[index..end]);
            index = end + 1;
        }

        granted.Sort(StringComparer.Ordinal);
        var expected = new List<string>(approved);
        expected.Sort(StringComparer.Ordinal);
        Assert.Equal(expected, granted);
    }

    /// <summary>Ensures server product code does not depend on client SDK namespaces.</summary>
    [Fact]
    public void ServerProductCodeShouldNotImportSquirixNamespaces()
    {
        var forbiddenNamespaces = new[]
        {
            "Squirix.Atomic",
            "Squirix.Batch",
            "Squirix.Watch",
            "Squirix.Scan",
            "Squirix.Batch",
            "Squirix.Mutations",
            "Squirix.Errors",
            "Squirix.Internal",
            "Squirix.Runtime",
        };

        foreach (var forbiddenNamespace in forbiddenNamespaces)
        {
            var result = Types.InAssembly(SquirixArchitecture.ServerAssembly).ShouldNot().HaveDependencyOn(forbiddenNamespace).GetResult();

            ArchitectureAssertions.AssertArchitecture(result);
        }
    }

    /// <summary>Ensures the server runtime project has the required library package metadata.</summary>
    [Fact]
    public void ServerProjectShouldBePackableLibrary()
    {
        var project = LoadProject("src/squirix.server/Squirix.Server.csproj");

        Assert.Equal("net10.0", ReadProperty(project, "TargetFramework"));
        Assert.DoesNotContain(project.Descendants(), static element => string.Equals(element.Name.LocalName, "OutputType", StringComparison.OrdinalIgnoreCase));
        Assert.Equal(ServerArchitectureNamespaces.Root, ReadProperty(project, "AssemblyName"));
        Assert.Equal(ServerArchitectureNamespaces.Root, ReadProperty(project, "RootNamespace"));
        Assert.Equal(ServerArchitectureNamespaces.PackageId, ReadProperty(project, "PackageId"));
        Assert.Equal("$(SquirixPackageVersion)", ReadProperty(project, "Version"));
        Assert.Equal("$(SquirixPackageVersion)", ReadProperty(project, "PackageVersion"));
        Assert.Equal("Apache-2.0", ReadProperty(project, "PackageLicenseExpression"));
        Assert.Equal("true", ReadProperty(project, "IsPackable"));
        Assert.Equal("true", ReadProperty(project, "TreatWarningsAsErrors"));
        Assert.Equal("enable", ReadProperty(project, "Nullable"));
    }

    /// <summary>Ensures the server project generates the basic KV and expiration transport contract from shared source.</summary>
    [Fact]
    public void ServerProjectShouldGenerateNarrowCacheGrpcTransportContractFromSharedSource()
    {
        XElement? serverProtobuf = null;
        foreach (var element in LoadProject("src/squirix.server/Squirix.Server.csproj").Descendants())
        {
            if (!string.Equals(element.Name.LocalName, "Protobuf", StringComparison.OrdinalIgnoreCase))
                continue;

            if (!string.Equals(element.Attribute("Include")?.Value, @"..\shared\transport\grpc\Protos\SquirixCache.proto", StringComparison.Ordinal))
                continue;

            serverProtobuf = element;
            break;
        }

        Assert.NotNull(serverProtobuf);
        Assert.Equal("Server;Client", serverProtobuf.Attribute("GrpcServices")?.Value);
        Assert.Equal(@"..\shared\transport\grpc\Protos", serverProtobuf.Attribute("ProtoRoot")?.Value);
        Assert.Equal("Internal", serverProtobuf.Attribute("Access")?.Value);
    }

    /// <summary>Ensures the server project keeps the approved ASP.NET Core hosting dependency baseline.</summary>
    [Fact]
    public void ServerProjectShouldKeepApprovedHostingDependencyBaseline()
    {
        var project = LoadProject("src/squirix.server/Squirix.Server.csproj");
        var serverPackageReferences = new List<string>();
        foreach (var include in ReadIncludes(project, "PackageReference"))
        {
            if (include.Equals("Grpc.AspNetCore", StringComparison.Ordinal) || include.StartsWith("Microsoft.AspNetCore.", StringComparison.Ordinal))
                serverPackageReferences.Add(include);
        }

        serverPackageReferences.Sort(StringComparer.Ordinal);
        var unexpectedPackageReferences = CollectExcept(serverPackageReferences, KnownServerPackageDependencyBaseline, StringComparer.Ordinal);

        Assert.Empty(unexpectedPackageReferences);

        var serverFrameworkReferences = new List<string>();
        foreach (var include in ReadIncludes(project, "FrameworkReference"))
        {
            if (include.StartsWith("Microsoft.AspNetCore", StringComparison.Ordinal))
                serverFrameworkReferences.Add(include);
        }

        serverFrameworkReferences.Sort(StringComparer.Ordinal);
        var unexpectedFrameworkReferences = CollectExcept(serverFrameworkReferences, KnownServerFrameworkDependencyBaseline, StringComparer.Ordinal);

        Assert.Empty(unexpectedFrameworkReferences);
        Assert.Contains(serverFrameworkReferences, static include => include.Equals("Microsoft.AspNetCore.App", StringComparison.Ordinal));
    }

    /// <summary>Ensures the server project does not reference the client SDK project.</summary>
    [Fact]
    public void ServerProjectShouldNotReferenceSquirixProject()
    {
        var references = ReadProjectIncludes("src/squirix.server/Squirix.Server.csproj", "ProjectReference");

        Assert.DoesNotContain(
            references,
            static reference => reference.Contains("squirix.csproj", StringComparison.OrdinalIgnoreCase) &&
                                !reference.Contains("squirix.server", StringComparison.OrdinalIgnoreCase));
        Assert.DoesNotContain(references, static reference => reference.Contains(@"..\squirix\Squirix.csproj", StringComparison.Ordinal));
    }

    /// <summary>Ensures Prometheus metrics endpoint mapping is owned by the server package.</summary>
    [Fact]
    public void ServerShouldOwnPrometheusMetricsEndpointMapping()
    {
        var mappingType = SquirixArchitecture.ServerAssembly.GetType("Squirix.Server.Node.Observability.Metrics.SquirixMetricsEndpointExtensions", false);
        Assert.NotNull(mappingType);
        Assert.False(mappingType.IsPublic);
    }

    /// <summary>Ensures service types stay in approved service namespaces.</summary>
    [Fact]
    public void ServiceTypesShouldLiveInApprovedNamespaces()
    {
        var serverResult = ArchitectureNetArchRules.EvaluateShouldResideInOneOfNamespaces(
            Types.InAssembly(SquirixArchitecture.ServerAssembly).That().HaveNameEndingWith("Service", StringComparison.InvariantCulture),
            ArchitectureAllowlists.ServiceTypeNamespaces);

        ArchitectureAssertions.AssertArchitecture(serverResult);
    }

    /// <summary>Ensures shared stale-owner marker constants are compiled into the server build from shared source.</summary>
    [Fact]
    public void SharedGrpcStaleOwnerMarkerConstantsShouldBePresentInServerBuild()
    {
        var markersType = SquirixArchitecture.ServerAssembly.GetType("Squirix.Transport.Grpc.Mappers.GrpcStaleOwnerMarkers", true)!;
        var errorCodeKey = markersType.GetField("ErrorCodeMetadataKey", BindingFlags.NonPublic | BindingFlags.Static);
        var staleOwnerValue = markersType.GetField("StaleOwnerErrorCodeValue", BindingFlags.NonPublic | BindingFlags.Static);

        Assert.NotNull(errorCodeKey);
        Assert.NotNull(staleOwnerValue);
        Assert.Equal("squirix-error-code", errorCodeKey.GetRawConstantValue());
        Assert.Equal("stale-owner", staleOwnerValue.GetRawConstantValue());
    }

    /// <summary>Ensures share-sourced gRPC transport mapper sources do not reference core internal runtime contracts.</summary>
    [Fact]
    public async Task SharedGrpcTransportMapperSourcesShouldNotDependOnCoreInternalRuntimeTypes()
    {
        var mapperDirectory = PathKit.Combine(ArchitectureRepositoryPaths.FindRepositoryRoot(), "src", "shared", "transport", "grpc", "Mappers");
        Assert.True(Directory.Exists(mapperDirectory), $"Expected mapper directory at {mapperDirectory}.");

        var mapperPaths = new List<string>();
        foreach (var path in Directory.EnumerateFiles(mapperDirectory, "*.cs", SearchOption.TopDirectoryOnly))
            mapperPaths.Add(path);

        mapperPaths.Sort(StringComparer.Ordinal);
        var offenders = new List<string>();
        foreach (var path in mapperPaths)
        {
            var text = await File.ReadAllTextAsync(path, DefaultCancellationToken);
            foreach (var marker in ForbiddenSharedGrpcTransportMapperRuntimeMarkers)
            {
                if (text.Contains(marker, StringComparison.Ordinal))
                    offenders.Add($"{Path.GetFileName(path)}:{marker}");
            }
        }

        Assert.Empty(offenders);
    }

    /// <summary>Ensures share-sourced gRPC transport mappers use the shared mapper namespace.</summary>
    [Fact]
    public async Task SharedGrpcTransportMappersShouldUseGrpcMappersNamespace()
    {
        var mapperDirectory = PathKit.Combine(ArchitectureRepositoryPaths.FindRepositoryRoot(), "src", "shared", "transport", "grpc", "Mappers");
        var mapperPaths = new List<string>();
        foreach (var path in Directory.EnumerateFiles(mapperDirectory, "*.cs", SearchOption.TopDirectoryOnly))
            mapperPaths.Add(path);

        mapperPaths.Sort(StringComparer.Ordinal);
        var offenders = new List<string>();
        foreach (var path in mapperPaths)
        {
            var text = await File.ReadAllTextAsync(path, DefaultCancellationToken);
            if (!text.Contains("namespace Squirix.Transport.Grpc.Mappers;", StringComparison.Ordinal))
                offenders.Add(Path.GetFileName(path));
        }

        Assert.Empty(offenders);
    }

    /// <summary>Ensures storage types stay isolated from transport adapter concerns.</summary>
    [Fact]
    public void StorageShouldNotDependOnAdapters()
    {
        var result = Types.InAssembly(SquirixArchitecture.ServerAssembly).That().ResideInNamespaceStartingWith(ServerArchitectureNamespaces.Storage).ShouldNot()
                          .HaveDependencyOn(ServerArchitectureNamespaces.Adapters).GetResult();

        ArchitectureAssertions.AssertArchitecture(result);
    }

    /// <summary>Ensures storage code does not take a dependency on hosting/DI composition details.</summary>
    [Fact]
    public void StorageShouldNotDependOnNodeHosting()
    {
        var result = Types.InAssembly(SquirixArchitecture.ServerAssembly).That().ResideInNamespaceStartingWith(ServerArchitectureNamespaces.Storage).ShouldNot()
                          .HaveDependencyOn($"{ServerArchitectureNamespaces.Node}.Hosting").GetResult();

        ArchitectureAssertions.AssertArchitecture(result);
    }

    /// <summary>Ensures validator types stay centralized in the hosting composition layer.</summary>
    [Fact]
    public void ValidatorTypesShouldLiveInApprovedNamespaces()
    {
        var validatorNamespaces = new List<string>();
        foreach (var ns in ArchitectureAllowlists.ValidatorTypeNamespaces)
        {
            if (string.Equals(ns, "Squirix", StringComparison.Ordinal) || string.Equals(ns, "Squirix.Core", StringComparison.Ordinal))
                continue;

            validatorNamespaces.Add(ns);
        }

        var serverResult = ArchitectureNetArchRules.EvaluateShouldResideInOneOfNamespaces(
            Types.InAssembly(SquirixArchitecture.ServerAssembly).That().HaveNameEndingWith("Validator", StringComparison.InvariantCulture).And()
                 .DoNotHaveNameEndingWith("Invalidator", StringComparison.InvariantCulture),
            validatorNamespaces);

        ArchitectureAssertions.AssertArchitecture(serverResult);
    }

    private static List<string> CollectExcept(IReadOnlyList<string> left, string[] baseline, StringComparer comparer)
    {
        var result = new List<string>();
        foreach (var item in left)
        {
            var found = false;
            foreach (var known in baseline)
            {
                if (!comparer.Equals(item, known))
                    continue;
                found = true;
                break;
            }

            if (!found)
                result.Add(item);
        }

        return result;
    }

    private static bool IsGeneratedOutputPath(string path)
    {
        var normalized = path.Replace(Path.AltDirectorySeparatorChar, Path.DirectorySeparatorChar);
        var objMarker = $"{Path.DirectorySeparatorChar}obj{Path.DirectorySeparatorChar}";
        var binMarker = $"{Path.DirectorySeparatorChar}bin{Path.DirectorySeparatorChar}";
        return normalized.Contains(objMarker, StringComparison.OrdinalIgnoreCase) || normalized.Contains(binMarker, StringComparison.OrdinalIgnoreCase);
    }

    private static XDocument LoadProject(string relativePath)
    {
        var path = PathKit.Combine(ArchitectureRepositoryPaths.FindRepositoryRoot(), relativePath.Replace('/', Path.DirectorySeparatorChar));
        Assert.True(File.Exists(path), $"Expected project at {path}.");
        return LoadProjectByAbsolutePath(path);
    }

    private static XDocument LoadProjectByAbsolutePath(string path) => XDocument.Load(path);

    private static List<string> ReadIncludes(XDocument project, string itemName)
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

        return includes;
    }

    private static List<string> ReadProjectCompileIncludes(string projectPath) => ReadProjectIncludes(projectPath, "Compile");

    private static List<string> ReadProjectIncludes(string projectPath, string itemName) => ReadIncludes(LoadProject(projectPath), itemName);

    private static string ReadProperty(XDocument project, string propertyName)
    {
        string? value = null;
        foreach (var element in project.Descendants())
        {
            if (!string.Equals(element.Name.LocalName, propertyName, StringComparison.OrdinalIgnoreCase))
                continue;

            value = element.Value.Trim();
            break;
        }

        Assert.False(string.IsNullOrWhiteSpace(value), $"Expected MSBuild property '{propertyName}'.");
        return value;
    }

    private static async Task<(string RelativePath, string Text)[]> ReadServerBootstrapSourceTextsAsync()
    {
        var root = ArchitectureRepositoryPaths.FindRepositoryRoot();
        var relativePaths = new[]
        {
            "src/squirix.server.host/Program.cs",
            "src/squirix.server.host/ShutdownSignal.cs",
            "src/squirix.server.host/SquirixServerProcess.cs",
        };

        var sources = new (string RelativePath, string Text)[relativePaths.Length];
        for (var i = 0; i < relativePaths.Length; i++)
        {
            var relativePath = relativePaths[i].Replace('/', Path.DirectorySeparatorChar);
            var absolutePath = PathKit.Combine(root, relativePath);
            Assert.True(File.Exists(absolutePath), $"Expected server bootstrap source at {absolutePath}.");
            sources[i] = (relativePath, await File.ReadAllTextAsync(absolutePath, DefaultCancellationToken));
        }

        return sources;
    }
}
