using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Xml.Linq;
using NetArchTest.Rules;
using Xunit;

namespace Squirix.Server.UnitTests.Architecture;

/// <summary>
/// Enforces high-value architectural dependency boundaries for the main Squirix assembly.
/// </summary>
public sealed class ArchitectureTests
{
    private static readonly string[] ForbiddenSharedGrpcTransportMapperRuntimeMarkers =
    [
        "ICacheRuntime",
        "ILogicalNamespacedCache",
        "ICacheApi<",
        "LocalCache<",
        "ClusteredCache<",
        "JournalWriter",
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

    /// <summary>
    /// Ensures transport adapters do not take dependencies on low-level journal JSON internals.
    /// </summary>
    [Fact]
    public void AdaptersShouldNotDependOnJournalJsonInternals()
    {
        var result = Types.InAssembly(SquirixArchitecture.ServerAssembly).That().ResideInNamespaceStartingWith(ServerArchitectureNamespaces.Adapters).ShouldNot()
                          .HaveDependencyOn($"{ServerArchitectureNamespaces.Storage}.Journaling.Json").GetResult();

        ArchitectureAssertions.AssertArchitecture(result);
    }

    /// <summary>
    /// Ensures client and server projects compile the same shared gRPC transport mapper sources.
    /// </summary>
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

    /// <summary>
    /// Ensures filter types stay at the REST adapter boundary.
    /// </summary>
    [Fact]
    public void FilterTypesShouldLiveInAdaptersRestNamespace()
    {
        var result = ArchitectureNetArchRules.EvaluateShouldResideInOneOfNamespaces(
            Types.InAssembly(SquirixArchitecture.ServerAssembly).That().HaveNameEndingWith("Filter"),
            [$"{ServerArchitectureNamespaces.Adapters}.Rest", $"{ServerArchitectureNamespaces.Adapters}.Endpoint.Rest"]);

        ArchitectureAssertions.AssertArchitecture(result);
    }

    /// <summary>
    /// Ensures handler types stay in the hosting security boundary.
    /// </summary>
    [Fact]
    public void HandlerTypesShouldLiveInNodeHostingSecurityNamespace()
    {
        var result = Types.InAssembly(SquirixArchitecture.ServerAssembly).That().HaveNameEndingWith("Handler").Should()
                          .ResideInNamespace($"{ServerArchitectureNamespaces.Node}.Hosting.Security").GetResult();

        ArchitectureAssertions.AssertArchitecture(result);
    }

    /// <summary>
    /// Ensures the journal periodic flush task is observed during disposal instead of being fire-and-forget.
    /// </summary>
    [Fact]
    public void JournalWriterFlushLoopShouldBeObservedByDispose()
    {
        var root = ArchitectureRepositoryPaths.FindRepositoryRoot();
        var text = File.ReadAllText(Path.Combine(root, "src", "squirix.server", "Storage", "Journaling", "JournalWriter.cs"));

        Assert.Contains("_flushLoopTask = FlushLoopAsync(_bgCts.Token);", text, StringComparison.Ordinal);
        Assert.Contains("await _flushLoopTask.ConfigureAwait(false);", text, StringComparison.Ordinal);
        Assert.DoesNotContain("_ = FlushLoopAsync(_bgCts.Token);", text, StringComparison.Ordinal);
    }

    /// <summary>
    /// Ensures metrics types stay centralized in the observability namespace.
    /// </summary>
    [Fact]
    public void MetricsTypesShouldLiveInObservabilityNamespace()
    {
        var result = Types.InAssembly(SquirixArchitecture.ServerAssembly).That().HaveNameEndingWith("Metrics").And().AreNotInterfaces().Should()
                          .ResideInNamespace($"{ServerArchitectureNamespaces.Node}.Observability").GetResult();

        ArchitectureAssertions.AssertArchitecture(result);
    }

    /// <summary>
    /// Ensures backpressure controls stay isolated from storage concerns.
    /// </summary>
    [Fact]
    public void NodeBackpressureShouldNotDependOnStorage()
    {
        var result = Types.InAssembly(SquirixArchitecture.ServerAssembly).That().ResideInNamespaceStartingWith($"{ServerArchitectureNamespaces.Node}.Backpressure").ShouldNot()
                          .HaveDependencyOn(ServerArchitectureNamespaces.Storage).GetResult();

        ArchitectureAssertions.AssertArchitecture(result);
    }

    /// <summary>
    /// Ensures node services remain application-layer components and do not depend on transport adapters.
    /// </summary>
    [Fact]
    public void NodeServicesShouldNotDependOnAdapters()
    {
        var result = Types.InAssembly(SquirixArchitecture.ServerAssembly).That().ResideInNamespaceStartingWith($"{ServerArchitectureNamespaces.Node}.Services").ShouldNot()
                          .HaveDependencyOn(ServerArchitectureNamespaces.Adapters).GetResult();

        ArchitectureAssertions.AssertArchitecture(result);
    }

    /// <summary>
    /// Ensures observability remains transport-agnostic and reusable across adapters.
    /// </summary>
    [Fact]
    public void ObservabilityShouldNotDependOnAdapters()
    {
        var result = Types.InAssembly(SquirixArchitecture.ServerAssembly).That().ResideInNamespaceStartingWith($"{ServerArchitectureNamespaces.Node}.Observability").ShouldNot()
                          .HaveDependencyOn(ServerArchitectureNamespaces.Adapters).GetResult();

        ArchitectureAssertions.AssertArchitecture(result);
    }

    /// <summary>
    /// Ensures configuration option types live only in approved configuration namespaces.
    /// </summary>
    [Fact]
    public void OptionsTypesShouldLiveInApprovedNamespaces()
    {
        var serverResult = ArchitectureNetArchRules.EvaluateShouldResideInOneOfNamespaces(
            Types.InAssembly(SquirixArchitecture.ServerAssembly).That().HaveNameEndingWith("Options"),
            ArchitectureAllowlists.ServerOptionsTypeNamespaces);

        ArchitectureAssertions.AssertArchitecture(serverResult);
    }

    /// <summary>
    /// Ensures product code does not use access-check bypass attributes.
    /// </summary>
    [Fact]
    public void ProductionSourcesShouldNotUseIgnoresAccessChecksTo()
    {
        var root = Path.Combine(ArchitectureRepositoryPaths.FindRepositoryRoot(), "src");
        var objMarker = $"{Path.DirectorySeparatorChar}obj{Path.DirectorySeparatorChar}";
        var offenders = Directory.EnumerateFiles(root, "*.cs", SearchOption.AllDirectories).Where(path => !path.Contains(objMarker, StringComparison.Ordinal))
                                 .Where(static path => File.ReadAllText(path).Contains("IgnoresAccessChecksTo", StringComparison.Ordinal))
                                 .OrderBy(static path => path, StringComparer.Ordinal).ToArray();

        Assert.Empty(offenders);
    }

    /// <summary>
    /// Ensures repository projects and sources do not hide dependencies with global or implicit usings.
    /// </summary>
    [Fact]
    public void RepositoryShouldNotUseGlobalOrImplicitUsings()
    {
        var root = ArchitectureRepositoryPaths.FindRepositoryRoot();
        var sourceOffenders = Directory.EnumerateFiles(root, "*.cs", SearchOption.AllDirectories).Where(static path => !IsGeneratedOutputPath(path))
                                       .Where(static path => Path.GetFileName(path).Equals("GlobalUsings.cs", StringComparison.OrdinalIgnoreCase) ||
                                                             File.ReadLines(path).Any(static line => line.TrimStart().StartsWith("global using ", StringComparison.Ordinal)))
                                       .Select(path => Path.GetRelativePath(root, path).Replace(Path.DirectorySeparatorChar, '/'))
                                       .OrderBy(static path => path, StringComparer.Ordinal).ToArray();

        var projectOffenders = Directory.EnumerateFiles(root, "*.csproj", SearchOption.AllDirectories).Where(static path => !IsGeneratedOutputPath(path))
                                        .Where(static path => LoadProjectByAbsolutePath(path).Descendants().Any(static element =>
                                             element.Name.LocalName == "ImplicitUsings" && element.Value.Trim().Equals("enable", StringComparison.OrdinalIgnoreCase)))
                                        .Select(path => Path.GetRelativePath(root, path).Replace(Path.DirectorySeparatorChar, '/'))
                                        .OrderBy(static path => path, StringComparer.Ordinal).ToArray();

        Assert.Empty(sourceOffenders);
        Assert.Empty(projectOffenders);
    }

    /// <summary>
    /// Ensures the server assembly generates server-side gRPC service bases from the shared transport namespace.
    /// </summary>
    [Fact]
    public void ServerAssemblyShouldGenerateGrpcServiceBaseFromSharedTransportNamespace()
    {
        var entryType = SquirixArchitecture.ServerAssembly.GetType("Squirix.Transport.Grpc.Cache.Entry", true)!;
        var serviceType = SquirixArchitecture.ServerAssembly.GetType("Squirix.Transport.Grpc.Cache.SquirixCacheService", true)!;
        var serviceBaseType = SquirixArchitecture.ServerAssembly.GetType("Squirix.Transport.Grpc.Cache.SquirixCacheService+SquirixCacheServiceBase", true)!;

        Assert.Same(SquirixArchitecture.ServerAssembly, entryType.Assembly);
        Assert.Same(SquirixArchitecture.ServerAssembly, serviceType.Assembly);
        Assert.Same(SquirixArchitecture.ServerAssembly, serviceBaseType.Assembly);
        Assert.False(entryType.IsPublic);
        Assert.False(serviceType.IsPublic);
        Assert.False(serviceBaseType.IsPublic);
    }

    /// <summary>
    /// Ensures the server package does not reference the client SDK assembly.
    /// </summary>
    [Fact]
    public void ServerAssemblyShouldNotReferenceSquirix()
    {
        var references = SquirixArchitecture.ServerAssembly.GetReferencedAssemblies().Select(static a => a.Name).ToArray();
        Assert.DoesNotContain("Squirix", references, StringComparer.Ordinal);
    }

    /// <summary>
    /// Ensures standalone server bootstrap starts through the public ASP.NET Core hosting extensions.
    /// </summary>
    [Fact]
    public void ServerBootstrapSourcesShouldUseServerPackageHostStartupApi()
    {
        var sources = ReadServerBootstrapSourceTexts();
        var combined = string.Join(Environment.NewLine, sources.Select(static source => source.Text));

        Assert.Contains("AddSquirixServer", combined, StringComparison.Ordinal);
        Assert.Contains("MapSquirixServer", combined, StringComparison.Ordinal);
    }

    /// <summary>
    /// Ensures the standalone process host stays separate from the packable server runtime.
    /// </summary>
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

    /// <summary>
    /// Ensures InternalsVisibleTo grants match the approved server allowlist.
    /// </summary>
    [Fact]
    public void ServerInternalsVisibleToShouldMatchApprovedAllowlist()
    {
        string[] approved =
        [
            "Squirix.Server.UnitTests",
            "Squirix.Server.PropertyTests",
            "Squirix.Server.IntegrationTests",
            "Squirix.Server.SmokeTests",
            "Squirix.Server.TestKit",
            "squirix-test-host",
            "sqr-ring-distribution",
            "DynamicProxyGenAssembly2",
        ];

        var root = ArchitectureRepositoryPaths.FindRepositoryRoot();
        var assemblyInfoPath = Path.Combine(root, "src", "squirix.server", "Properties", "AssemblyInfo.cs");
        var text = File.ReadAllText(assemblyInfoPath);
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
        var expected = approved.OrderBy(static name => name, StringComparer.Ordinal).ToArray();
        Assert.Equal(expected, granted);
    }

    /// <summary>
    /// Ensures server product code does not depend on client SDK namespaces.
    /// </summary>
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

    /// <summary>
    /// Ensures the server runtime project has the required library package metadata.
    /// </summary>
    [Fact]
    public void ServerProjectShouldBePackableLibrary()
    {
        var project = LoadProject("src/squirix.server/Squirix.Server.csproj");

        Assert.Equal("net10.0", ReadProperty(project, "TargetFramework"));
        Assert.DoesNotContain(project.Descendants(), static element => element.Name.LocalName == "OutputType");
        Assert.Equal(ServerArchitectureNamespaces.Root, ReadProperty(project, "AssemblyName"));
        Assert.Equal(ServerArchitectureNamespaces.Root, ReadProperty(project, "RootNamespace"));
        Assert.Equal(ServerArchitectureNamespaces.Root, ReadProperty(project, "PackageId"));
        Assert.Equal("$(SquirixPackageVersion)", ReadProperty(project, "Version"));
        Assert.Equal("$(SquirixPackageVersion)", ReadProperty(project, "PackageVersion"));
        Assert.Equal("Apache-2.0", ReadProperty(project, "PackageLicenseExpression"));
        Assert.Equal("true", ReadProperty(project, "IsPackable"));
        Assert.Equal("true", ReadProperty(project, "TreatWarningsAsErrors"));
        Assert.Equal("enable", ReadProperty(project, "Nullable"));
    }

    /// <summary>
    /// Ensures the server project generates only server-owned durability protos.
    /// </summary>
    [Fact]
    public void ServerProjectShouldGenerateJournalEnvelopeTransportContract()
    {
        var protobuf = LoadProject("src/squirix.server/Squirix.Server.csproj").Descendants().Where(static element => element.Name.LocalName == "Protobuf")
                                                                              .SingleOrDefault(static element => string.Equals(
                                                                                   element.Attribute("Include")?.Value,
                                                                                   @"Storage\Journaling\Protos\JournalEnvelope.proto",
                                                                                   StringComparison.Ordinal));

        Assert.NotNull(protobuf);
        Assert.Equal("Server", protobuf.Attribute("GrpcServices")?.Value);
        Assert.Equal(@"Storage\Journaling\Protos", protobuf.Attribute("ProtoRoot")?.Value);
        Assert.Equal("Internal", protobuf.Attribute("Access")?.Value);

        var root = ArchitectureRepositoryPaths.FindRepositoryRoot();
        var protoText = File.ReadAllText(Path.Combine(root, "src", "squirix.server", "Storage", "Journaling", "Protos", "JournalEnvelope.proto"));
        Assert.Contains("option csharp_namespace = \"Squirix.Server.Storage.JournalProto\";", protoText, StringComparison.Ordinal);
        Assert.Contains("message JournalEnvelope", protoText, StringComparison.Ordinal);
        Assert.Contains("message Put", protoText, StringComparison.Ordinal);
        Assert.Contains("message Remove", protoText, StringComparison.Ordinal);
        Assert.DoesNotContain("message JournalBatch", protoText, StringComparison.Ordinal);
        Assert.Contains("package squirix.journal;", protoText, StringComparison.Ordinal);
    }

    /// <summary>
    /// Ensures the server project keeps the approved ASP.NET Core hosting dependency baseline.
    /// </summary>
    [Fact]
    public void ServerProjectShouldKeepApprovedHostingDependencyBaseline()
    {
        var project = LoadProject("src/squirix.server/Squirix.Server.csproj");
        var serverPackageReferences = ReadIncludes(project, "PackageReference")
                                     .Where(static include =>
                                          include.Equals("Grpc.AspNetCore", StringComparison.Ordinal) || include.StartsWith("Microsoft.AspNetCore.", StringComparison.Ordinal))
                                     .OrderBy(static include => include, StringComparer.Ordinal).ToArray();
        var unexpectedPackageReferences = serverPackageReferences.Except(KnownServerPackageDependencyBaseline, StringComparer.Ordinal).ToArray();

        Assert.Empty(unexpectedPackageReferences);

        var serverFrameworkReferences = ReadIncludes(project, "FrameworkReference").Where(static include => include.StartsWith("Microsoft.AspNetCore", StringComparison.Ordinal))
                                                                                   .OrderBy(static include => include, StringComparer.Ordinal).ToArray();
        var unexpectedFrameworkReferences = serverFrameworkReferences.Except(KnownServerFrameworkDependencyBaseline, StringComparer.Ordinal).ToArray();

        Assert.Empty(unexpectedFrameworkReferences);
        Assert.Contains(serverFrameworkReferences, static include => include.Equals("Microsoft.AspNetCore.App", StringComparison.Ordinal));
    }

    /// <summary>
    /// Ensures the server project does not reference the client SDK project.
    /// </summary>
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

    /// <summary>
    /// Ensures Prometheus metrics endpoint mapping is owned by the server package.
    /// </summary>
    [Fact]
    public void ServerShouldOwnPrometheusMetricsEndpointMapping()
    {
        var mappingType = SquirixArchitecture.ServerAssembly.GetType("Squirix.Server.Node.Observability.Metrics.SquirixMetricsEndpointExtensions", false);
        Assert.NotNull(mappingType);
        Assert.False(mappingType.IsPublic);
    }

    /// <summary>
    /// Ensures service types stay in approved service namespaces.
    /// </summary>
    [Fact]
    public void ServiceTypesShouldLiveInApprovedNamespaces()
    {
        var serverResult = ArchitectureNetArchRules.EvaluateShouldResideInOneOfNamespaces(
            Types.InAssembly(SquirixArchitecture.ServerAssembly).That().HaveNameEndingWith("Service"),
            ArchitectureAllowlists.ServiceTypeNamespaces);

        ArchitectureAssertions.AssertArchitecture(serverResult);
    }

    /// <summary>
    /// Ensures shared stale-owner marker constants are compiled into the server build from shared source.
    /// </summary>
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

    /// <summary>
    /// Ensures share-sourced gRPC transport mapper sources do not reference core internal runtime contracts.
    /// </summary>
    [Fact]
    public void SharedGrpcTransportMapperSourcesShouldNotDependOnCoreInternalRuntimeTypes()
    {
        var mapperDirectory = Path.Combine(ArchitectureRepositoryPaths.FindRepositoryRoot(), "src", "shared", "transport", "grpc", "Mappers");
        Assert.True(Directory.Exists(mapperDirectory), $"Expected mapper directory at {mapperDirectory}.");

        var offenders = Directory.EnumerateFiles(mapperDirectory, "*.cs", SearchOption.TopDirectoryOnly).Select(static path => (Path: path, Text: File.ReadAllText(path)))
                                 .SelectMany(static pair => ForbiddenSharedGrpcTransportMapperRuntimeMarkers
                                                           .Where(marker => pair.Text.Contains(marker, StringComparison.Ordinal))
                                                           .Select(marker => $"{Path.GetFileName(pair.Path)}:{marker}")).OrderBy(
                                      static offender => offender,
                                      StringComparer.Ordinal).ToArray();

        Assert.Empty(offenders);
    }

    /// <summary>
    /// Ensures share-sourced gRPC transport mappers use the shared mapper namespace.
    /// </summary>
    [Fact]
    public void SharedGrpcTransportMappersShouldUseGrpcMappersNamespace()
    {
        var mapperDirectory = Path.Combine(ArchitectureRepositoryPaths.FindRepositoryRoot(), "src", "shared", "transport", "grpc", "Mappers");
        var offenders = Directory.EnumerateFiles(mapperDirectory, "*.cs", SearchOption.TopDirectoryOnly).Select(static path => (Path: path, Text: File.ReadAllText(path)))
                                 .Where(static pair => !pair.Text.Contains("namespace Squirix.Transport.Grpc.Mappers;", StringComparison.Ordinal))
                                 .Select(static pair => Path.GetFileName(pair.Path))
                                 .OrderBy(static path => path, StringComparer.Ordinal).ToArray();

        Assert.Empty(offenders);
    }

    /// <summary>
    /// Ensures the server project generates the basic KV and expiration transport contract from shared source.
    /// </summary>
    [Fact]
    public void ServerProjectShouldGenerateNarrowCacheGrpcTransportContractFromSharedSource()
    {
        var serverProtobuf = LoadProject("src/squirix.server/Squirix.Server.csproj").Descendants().Where(static element => element.Name.LocalName == "Protobuf")
                                                                                    .SingleOrDefault(static element => string.Equals(
                                                                                         element.Attribute("Include")?.Value,
                                                                                         @"..\shared\transport\grpc\Protos\SquirixCache.proto",
                                                                                         StringComparison.Ordinal));

        Assert.NotNull(serverProtobuf);
        Assert.Equal("Server;Client", serverProtobuf.Attribute("GrpcServices")?.Value);
        Assert.Equal(@"..\shared\transport\grpc\Protos", serverProtobuf.Attribute("ProtoRoot")?.Value);
        Assert.Equal("Internal", serverProtobuf.Attribute("Access")?.Value);
    }

    /// <summary>
    /// Ensures storage types stay isolated from transport adapter concerns.
    /// </summary>
    [Fact]
    public void StorageShouldNotDependOnAdapters()
    {
        var result = Types.InAssembly(SquirixArchitecture.ServerAssembly).That().ResideInNamespaceStartingWith(ServerArchitectureNamespaces.Storage).ShouldNot()
                          .HaveDependencyOn(ServerArchitectureNamespaces.Adapters).GetResult();

        ArchitectureAssertions.AssertArchitecture(result);
    }

    /// <summary>
    /// Ensures storage code does not take a dependency on hosting/DI composition details.
    /// </summary>
    [Fact]
    public void StorageShouldNotDependOnNodeHosting()
    {
        var result = Types.InAssembly(SquirixArchitecture.ServerAssembly).That().ResideInNamespaceStartingWith(ServerArchitectureNamespaces.Storage).ShouldNot()
                          .HaveDependencyOn($"{ServerArchitectureNamespaces.Node}.Hosting").GetResult();

        ArchitectureAssertions.AssertArchitecture(result);
    }

    /// <summary>
    /// Ensures validator types stay centralized in the hosting composition layer.
    /// </summary>
    [Fact]
    public void ValidatorTypesShouldLiveInApprovedNamespaces()
    {
        var serverResult = ArchitectureNetArchRules.EvaluateShouldResideInOneOfNamespaces(
            Types.InAssembly(SquirixArchitecture.ServerAssembly).That().HaveNameEndingWith("Validator").And().DoNotHaveNameEndingWith("Invalidator"),
            [.. ArchitectureAllowlists.ValidatorTypeNamespaces.Where(static ns => ns is not "Squirix" and not "Squirix.Core")]);

        ArchitectureAssertions.AssertArchitecture(serverResult);
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
        var path = Path.Combine(ArchitectureRepositoryPaths.FindRepositoryRoot(), relativePath.Replace('/', Path.DirectorySeparatorChar));
        Assert.True(File.Exists(path), $"Expected project at {path}.");
        return LoadProjectByAbsolutePath(path);
    }

    private static XDocument LoadProjectByAbsolutePath(string path) => XDocument.Load(path);

    private static string[] ReadIncludes(XDocument project, string itemName) =>
    [
        .. project.Descendants().Where(element => element.Name.LocalName == itemName).Select(static element => element.Attribute("Include")?.Value)
                  .Where(static value => !string.IsNullOrWhiteSpace(value)).Select(static value => value!),
    ];

    private static string[] ReadProjectCompileIncludes(string projectPath) => ReadProjectIncludes(projectPath, "Compile");

    private static string[] ReadProjectIncludes(string projectPath, string itemName) => ReadIncludes(LoadProject(projectPath), itemName);

    private static string ReadProperty(XDocument project, string propertyName)
    {
        var value = project.Descendants().FirstOrDefault(element => element.Name.LocalName == propertyName)?.Value.Trim();
        Assert.False(string.IsNullOrWhiteSpace(value), $"Expected MSBuild property '{propertyName}'.");
        return value;
    }

    private static (string RelativePath, string Text)[] ReadServerBootstrapSourceTexts()
    {
        var root = ArchitectureRepositoryPaths.FindRepositoryRoot();
        var relativePaths = new[]
        {
            "src/squirix.server.host/Program.cs",
            "src/squirix.server.host/ShutdownSignal.cs",
            "src/squirix.server.host/SquirixServerProcess.cs",
        };

        return
        [
            .. relativePaths.Select(static path => path.Replace('/', Path.DirectorySeparatorChar)).Select(path => (RelativePath: path, AbsolutePath: Path.Combine(root, path)))
                            .Select(static pair =>
                             {
                                 Assert.True(File.Exists(pair.AbsolutePath), $"Expected server bootstrap source at {pair.AbsolutePath}.");
                                 return (pair.RelativePath, Text: File.ReadAllText(pair.AbsolutePath));
                             }),
        ];
    }
}
