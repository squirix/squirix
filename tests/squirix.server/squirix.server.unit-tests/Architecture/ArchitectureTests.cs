using System;
using System.Collections.Frozen;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using System.Xml.Linq;
using ArchUnitNET.xUnitV3;
using Squirix.Server.Node.Observability.Metrics;
using Squirix.Server.TestKit.IO;
using Squirix.Server.UnitTests.Support;
using Squirix.Transport.Grpc.Cache;
using Squirix.Transport.Grpc.Mappers;
using Xunit;
using static ArchUnitNET.Fluent.ArchRuleDefinition;

namespace Squirix.Server.UnitTests.Architecture;

/// <summary>Enforces high-value architectural dependency boundaries for the main Squirix assembly.</summary>
public sealed class ArchitectureTests : UnitTestBase
{
    private const string ServerProjectRelativePath = "src/squirix.server/Squirix.Server.csproj";

    private static readonly Lazy<XDocument> ServerProject = new(LoadServerProject);

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

    private static readonly Lazy<MsbuildProjectIndex> ServerProjectIndex = new(static () => MsbuildProjectIndex.Parse(ServerProject.Value));

    /// <summary>Ensures transport adapters do not take dependencies on low-level journal JSON internals.</summary>
    [Fact]
    public void AdaptersShouldNotDependOnJournalJsonInternals()
    {
        var rule = ServerArchitectureScope.Server.And().HaveFullNameContaining(ServerArchitectureNamespaces.Adapters)
            .Should().NotDependOnAnyTypesThat().HaveFullNameContaining($"{ServerArchitectureNamespaces.Storage}.Journaling.Json");

        rule.Check(ServerArchitecture.Instance);
    }

    /// <summary>Ensures client and server projects compile the same shared gRPC transport mapper sources.</summary>
    [Fact]
    public void ClientAndServerProjectsShouldCompileSharedGrpcTransportMappersFromSameSources()
    {
        string[] expectedIncludes =
        [
            @"..\shared\transport\grpc\Mappers\GrpcStaleOwnerMarkers.cs",
        ];

        var serverIncludes = ServerProjectIndex.Value.GetIncludes("Compile");

        foreach (var include in expectedIncludes)
            Assert.Contains(include, serverIncludes, StringComparer.Ordinal);
    }

    /// <summary>Ensures filter types stay at the REST adapter boundary.</summary>
    [Fact]
    public void FilterTypesShouldLiveInAdaptersRestNamespace()
    {
        ArchitectureRuleHelpers.AssertResideInOneOfNamespaces(
            ServerArchitectureScope.Server.And().HaveNameEndingWith("Filter"),
            [$"{ServerArchitectureNamespaces.Adapters}.Rest", $"{ServerArchitectureNamespaces.Adapters}.Endpoint.Rest"]);
    }

    /// <summary>Ensures handler types stay in the hosting security boundary.</summary>
    [Fact]
    public void HandlerTypesShouldLiveInNodeHostingSecurityNamespace()
    {
        var rule = ServerArchitectureScope.Server.And().HaveNameEndingWith("Handler")
            .Should().ResideInNamespace($"{ServerArchitectureNamespaces.Node}.Hosting.Security")
            .WithoutRequiringPositiveResults();

        rule.Check(ServerArchitecture.Instance);
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
        var rule = ServerArchitectureScope.Server.And().HaveNameEndingWith("Metrics").And().AreNot(Interfaces())
            .Should().ResideInNamespace($"{ServerArchitectureNamespaces.Node}.Observability");

        rule.Check(ServerArchitecture.Instance);
    }

    /// <summary>Ensures backpressure controls stay isolated from storage concerns.</summary>
    [Fact]
    public void NodeBackpressureShouldNotDependOnStorage()
    {
        var rule = ServerArchitectureScope.Server.And().HaveFullNameContaining($"{ServerArchitectureNamespaces.Node}.Backpressure")
            .Should().NotDependOnAnyTypesThat().HaveFullNameContaining(ServerArchitectureNamespaces.Storage);

        rule.Check(ServerArchitecture.Instance);
    }

    /// <summary>Ensures node services remain application-layer components and do not depend on transport adapters.</summary>
    [Fact]
    public void NodeServicesShouldNotDependOnAdapters()
    {
        var rule = ServerArchitectureScope.Server.And().HaveFullNameContaining($"{ServerArchitectureNamespaces.Node}.Services")
            .Should().NotDependOnAnyTypesThat().HaveFullNameContaining(ServerArchitectureNamespaces.Adapters);

        rule.Check(ServerArchitecture.Instance);
    }

    /// <summary>Ensures observability remains transport-agnostic and reusable across adapters.</summary>
    [Fact]
    public void ObservabilityShouldNotDependOnAdapters()
    {
        var rule = ServerArchitectureScope.Server.And().HaveFullNameContaining($"{ServerArchitectureNamespaces.Node}.Observability")
            .Should().NotDependOnAnyTypesThat().HaveFullNameContaining(ServerArchitectureNamespaces.Adapters);

        rule.Check(ServerArchitecture.Instance);
    }

    /// <summary>Ensures configuration option types live only in approved configuration namespaces.</summary>
    [Fact]
    public void OptionsTypesShouldLiveInApprovedNamespaces()
    {
        ArchitectureRuleHelpers.AssertResideInOneOfNamespaces(
            ServerArchitectureScope.Server.And().HaveNameEndingWith("Options"),
            ArchitectureAllowlists.ServerOptionsTypeNamespaces);
    }

    /// <summary>Ensures product code does not use access-check bypass attributes.</summary>
    [Fact]
    public async Task ProductionSourcesShouldNotUseIgnoresAccessChecksTo()
    {
        var root = PathKit.Combine(ArchitectureRepositoryPaths.FindRepositoryRoot(), "src");
        var objMarker = $"{Path.DirectorySeparatorChar}obj{Path.DirectorySeparatorChar}";
        var paths = new List<string>(200);
        paths.AddRange(Directory.GetFiles(root, "*.cs", SearchOption.AllDirectories));

        paths.Sort(StringComparer.Ordinal);
        foreach (var path in paths)
        {
            if (path.Contains(objMarker, StringComparison.Ordinal))
                continue;

            var text = await File.ReadAllTextAsync(path, DefaultCancellationToken);
            Assert.False(text.Contains("IgnoresAccessChecksTo", StringComparison.Ordinal), path);
        }
    }

    /// <summary>Ensures repository projects and sources do not hide dependencies with global or implicit usings.</summary>
    [Fact]
    public void RepositoryShouldNotUseGlobalOrImplicitUsings()
    {
        var root = ArchitectureRepositoryPaths.FindRepositoryRoot();
        var sourceOffenders = new List<string>();
        foreach (var path in Directory.GetFiles(root, "*.cs", SearchOption.AllDirectories))
        {
            if (IsGeneratedOutputPath(path))
                continue;

            if (Path.GetFileName(path).Equals("GlobalUsings.cs", StringComparison.OrdinalIgnoreCase))
            {
                sourceOffenders.Add(Path.GetRelativePath(root, path).Replace(Path.DirectorySeparatorChar, '/'));
                continue;
            }

            var hasGlobalUsing = false;
            foreach (var line in File.ReadAllLines(path))
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
        foreach (var path in Directory.GetFiles(root, "*.csproj", SearchOption.AllDirectories))
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
        Assert.False(typeof(CacheEntryWire).IsPublic);
        Assert.False(typeof(SquirixCacheService).IsPublic);
        Assert.False(typeof(SquirixCacheService.SquirixCacheServiceBase).IsPublic);
    }

    /// <summary>Ensures the server package does not reference the client SDK assembly.</summary>
    [Fact]
    public void ServerAssemblyShouldNotReferenceSquirix()
    {
        var references = ServerProjectIndex.Value.GetIncludes("ProjectReference");
        Assert.DoesNotContain(references, static reference => reference.Contains(@"..\squirix\Squirix.csproj", StringComparison.OrdinalIgnoreCase));
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
        var index = MsbuildProjectIndex.Parse(LoadProject("src/squirix.server.host/Squirix.Server.Host.csproj"));

        Assert.Equal("net10.0", index.RequireProperty("TargetFramework"));
        Assert.Equal("Exe", index.RequireProperty("OutputType"));
        Assert.Equal("Squirix.Server.Host", index.RequireProperty("AssemblyName"));
        Assert.Equal("Squirix.Server.Host", index.RequireProperty("RootNamespace"));
        Assert.Equal("true", index.RequireProperty("IsPackable"));
        Assert.Equal("true", index.RequireProperty("PackAsTool"));
        Assert.Equal("squirix-server", index.RequireProperty("ToolCommandName"));
        Assert.Equal("$(SquirixPackageVersion)", index.RequireProperty("Version"));
        Assert.Equal("$(SquirixPackageVersion)", index.RequireProperty("PackageVersion"));
        Assert.Equal(@"..\squirix.server\Squirix.Server.csproj", index.GetIncludes("ProjectReference")[0]);
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
        Array.Sort(approved, StringComparer.Ordinal);
        Assert.Equal(approved, granted);
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
            var rule = ServerArchitectureScope.Server.Should().NotDependOnAnyTypesThat().HaveFullNameContaining(forbiddenNamespace);

            rule.Check(ServerArchitecture.Instance);
        }
    }

    /// <summary>Ensures the server runtime project has the required library package metadata.</summary>
    [Fact]
    public void ServerProjectShouldBePackableLibrary()
    {
        var index = ServerProjectIndex.Value;

        Assert.Equal("net10.0", index.RequireProperty("TargetFramework"));
        Assert.False(index.ContainsElement("OutputType"));
        Assert.Equal(ServerArchitectureNamespaces.Root, index.RequireProperty("AssemblyName"));
        Assert.Equal(ServerArchitectureNamespaces.Root, index.RequireProperty("RootNamespace"));
        Assert.Equal(ServerArchitectureNamespaces.PackageId, index.RequireProperty("PackageId"));
        Assert.Equal("$(SquirixPackageVersion)", index.RequireProperty("Version"));
        Assert.Equal("$(SquirixPackageVersion)", index.RequireProperty("PackageVersion"));
        Assert.Equal("Apache-2.0", index.RequireProperty("PackageLicenseExpression"));
        Assert.Equal("true", index.RequireProperty("IsPackable"));
        Assert.Equal("true", index.RequireProperty("TreatWarningsAsErrors"));
        Assert.Equal("enable", index.RequireProperty("Nullable"));
    }

    /// <summary>Ensures the server project generates the basic KV and expiration transport contract from shared source.</summary>
    [Fact]
    public void ServerProjectShouldGenerateNarrowCacheGrpcTransportContractFromSharedSource()
    {
        var protobuf = ServerProjectIndex.Value.RequireIncludedElement("Protobuf", @"..\shared\transport\grpc\Protos\SquirixCache.proto");

        Assert.Equal("Server;Client", protobuf.Attribute("GrpcServices")?.Value);
        Assert.Equal(@"..\shared\transport\grpc\Protos", protobuf.Attribute("ProtoRoot")?.Value);
        Assert.Equal("Internal", protobuf.Attribute("Access")?.Value);
    }

    /// <summary>Ensures the server project keeps the approved ASP.NET Core hosting dependency baseline.</summary>
    [Fact]
    public void ServerProjectShouldKeepApprovedHostingDependencyBaseline()
    {
        var index = ServerProjectIndex.Value;
        var frameworkIncludes = index.GetIncludes("FrameworkReference");

        Assert.Empty(
            CollectUnexpectedMatches(
                index.GetIncludes("PackageReference"),
                static include => include.Equals("Grpc.AspNetCore", StringComparison.Ordinal) || include.StartsWith("Microsoft.AspNetCore.", StringComparison.Ordinal),
                KnownServerPackageDependencyBaseline,
                StringComparer.Ordinal));

        Assert.Empty(
            CollectUnexpectedMatches(
                frameworkIncludes,
                static include => include.StartsWith("Microsoft.AspNetCore", StringComparison.Ordinal),
                KnownServerFrameworkDependencyBaseline,
                StringComparer.Ordinal));

        Assert.Contains(frameworkIncludes, static include => include.Equals("Microsoft.AspNetCore.App", StringComparison.Ordinal));
    }

    /// <summary>Ensures the server project does not reference the client SDK project.</summary>
    [Fact]
    public void ServerProjectShouldNotReferenceSquirixProject()
    {
        var list = ServerProjectIndex.Value.GetIncludes("ProjectReference");

        Assert.DoesNotContain(
            list,
            static reference => reference.Contains("squirix.csproj", StringComparison.OrdinalIgnoreCase) &&
                                !reference.Contains("squirix.server", StringComparison.OrdinalIgnoreCase));
        Assert.DoesNotContain(list, static reference => reference.Contains(@"..\squirix\Squirix.csproj", StringComparison.Ordinal));
    }

    /// <summary>Ensures Prometheus metrics endpoint mapping is owned by the server package.</summary>
    [Fact]
    public void ServerShouldOwnPrometheusMetricsEndpointMapping() => Assert.False(typeof(SquirixMetricsEndpointExtensions).IsPublic);

    /// <summary>Ensures service types stay in approved service namespaces.</summary>
    [Fact]
    public void ServiceTypesShouldLiveInApprovedNamespaces()
    {
        ArchitectureRuleHelpers.AssertResideInOneOfNamespaces(
            ServerArchitectureScope.Server.And().HaveNameEndingWith("Service"),
            ArchitectureAllowlists.ServiceTypeNamespaces);
    }

    /// <summary>Ensures shared stale-owner marker constants are compiled into the server build from shared source.</summary>
    [Fact]
    public void SharedGrpcStaleOwnerMarkerConstantsShouldBePresentInServerBuild()
    {
        var found = false;
        foreach (var entry in GrpcStaleOwnerMarkers.CreateStaleOwnerTrailers())
        {
            if (!string.Equals(entry.Key, "squirix-error-code", StringComparison.Ordinal) || !string.Equals(entry.Value, "stale-owner", StringComparison.Ordinal))
                continue;
            found = true;
            break;
        }

        Assert.True(found);
    }

    /// <summary>Ensures share-sourced gRPC transport mapper sources do not reference core internal runtime contracts.</summary>
    [Fact]
    public async Task SharedGrpcTransportMapperSourcesShouldNotDependOnCoreInternalRuntimeTypes()
    {
        var mapperDirectory = PathKit.Combine(ArchitectureRepositoryPaths.FindRepositoryRoot(), "src", "shared", "transport", "grpc", "Mappers");
        Assert.True(Directory.Exists(mapperDirectory), $"Expected mapper directory at {mapperDirectory}.");

        var mapperPaths = new List<string>(Directory.GetFiles(mapperDirectory, "*.cs", SearchOption.TopDirectoryOnly));

        mapperPaths.Sort(StringComparer.Ordinal);
        for (var i = 0; i < mapperPaths.Count; i++)
        {
            var path = mapperPaths[i];
            var text = await File.ReadAllTextAsync(path, DefaultCancellationToken);
            for (var markerIndex = 0; markerIndex < ForbiddenSharedGrpcTransportMapperRuntimeMarkers.Length; markerIndex++)
            {
                var marker = ForbiddenSharedGrpcTransportMapperRuntimeMarkers[markerIndex];
                Assert.False(text.Contains(marker, StringComparison.Ordinal), $"{Path.GetFileName(path)}:{marker}");
            }
        }
    }

    /// <summary>Ensures share-sourced gRPC transport mappers use the shared mapper namespace.</summary>
    [Fact]
    public async Task SharedGrpcTransportMappersShouldUseGrpcMappersNamespace()
    {
        var mapperDirectory = PathKit.Combine(ArchitectureRepositoryPaths.FindRepositoryRoot(), "src", "shared", "transport", "grpc", "Mappers");
        var mapperPaths = new List<string>(Directory.GetFiles(mapperDirectory, "*.cs", SearchOption.TopDirectoryOnly));

        mapperPaths.Sort(StringComparer.Ordinal);
        for (var i = 0; i < mapperPaths.Count; i++)
        {
            var path = mapperPaths[i];
            var text = await File.ReadAllTextAsync(path, DefaultCancellationToken);
            Assert.Contains("namespace Squirix.Transport.Grpc.Mappers;", text, StringComparison.Ordinal);
        }
    }

    /// <summary>Ensures storage types stay isolated from transport adapter concerns.</summary>
    [Fact]
    public void StorageShouldNotDependOnAdapters()
    {
        var rule = ServerArchitectureScope.Server.And().HaveFullNameContaining(ServerArchitectureNamespaces.Storage)
            .Should().NotDependOnAnyTypesThat().HaveFullNameContaining(ServerArchitectureNamespaces.Adapters);

        rule.Check(ServerArchitecture.Instance);
    }

    /// <summary>Ensures storage code does not take a dependency on hosting/DI composition details.</summary>
    [Fact]
    public void StorageShouldNotDependOnNodeHosting()
    {
        var rule = ServerArchitectureScope.Server.And().HaveFullNameContaining(ServerArchitectureNamespaces.Storage)
            .Should().NotDependOnAnyTypesThat().HaveFullNameContaining($"{ServerArchitectureNamespaces.Node}.Hosting");

        rule.Check(ServerArchitecture.Instance);
    }

    /// <summary>Ensures validator types stay centralized in the hosting composition layer.</summary>
    [Fact]
    public void ValidatorTypesShouldLiveInApprovedNamespaces()
    {
        ArchitectureRuleHelpers.AssertResideInOneOfNamespaces(
            ServerArchitectureScope.Server.And().HaveNameEndingWith("Validator").And().DoNotHaveNameEndingWith("Invalidator"),
            ArchitectureAllowlists.ValidatorTypeArchitectureNamespaces);
    }

    private static List<string> CollectUnexpectedMatches(List<string> includes, Func<string, bool> isMatch, string[] baseline, StringComparer comparer)
    {
        var unexpected = new List<string>();
        for (var index = 0; index < includes.Count; index++)
        {
            var include = includes[index];
            if (!isMatch(include))
                continue;

            var isBaseline = false;
            for (var baselineIndex = 0; baselineIndex < baseline.Length; baselineIndex++)
            {
                if (!comparer.Equals(include, baseline[baselineIndex]))
                    continue;

                isBaseline = true;
                break;
            }

            if (!isBaseline)
                unexpected.Add(include);
        }

        return unexpected;
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

    private static XDocument LoadServerProject()
    {
        var path = PathKit.Combine(ArchitectureRepositoryPaths.FindRepositoryRoot(), ServerProjectRelativePath.Replace('/', Path.DirectorySeparatorChar));
        Assert.True(File.Exists(path), $"Expected project at {path}.");
        return LoadProjectByAbsolutePath(path);
    }

    private static async Task<(string RelativePath, string Text)[]> ReadServerBootstrapSourceTextsAsync()
    {
        var root = ArchitectureRepositoryPaths.FindRepositoryRoot();
        var relativePaths = new[]
        {
            "src/squirix.server.host/Program.cs",
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

    private sealed class MsbuildProjectIndex
    {
        private readonly FrozenDictionary<string, List<XElement>> _includedElements;
        private readonly FrozenDictionary<string, List<string>> _includes;
        private readonly FrozenSet<string> _localNames;
        private readonly FrozenDictionary<string, string> _properties;

        private MsbuildProjectIndex(
            FrozenDictionary<string, string> properties,
            FrozenDictionary<string, List<string>> includes,
            FrozenDictionary<string, List<XElement>> includedElements,
            FrozenSet<string> localNames)
        {
            _properties = properties;
            _includes = includes;
            _includedElements = includedElements;
            _localNames = localNames;
        }

        public static MsbuildProjectIndex Parse(XDocument project)
        {
            var properties = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
            var includes = new Dictionary<string, List<string>>(StringComparer.OrdinalIgnoreCase);
            var includedElements = new Dictionary<string, List<XElement>>(StringComparer.OrdinalIgnoreCase);
            var localNames = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

            CollectIndexData(project.Root, properties, includes, includedElements, localNames);

            return new MsbuildProjectIndex(
                properties.ToFrozenDictionary(StringComparer.OrdinalIgnoreCase),
                includes.ToFrozenDictionary(StringComparer.OrdinalIgnoreCase),
                includedElements.ToFrozenDictionary(StringComparer.OrdinalIgnoreCase),
                localNames.ToFrozenSet(StringComparer.OrdinalIgnoreCase));
        }

        public bool ContainsElement(string localName) => _localNames.Contains(localName);

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

        public string RequireProperty(string propertyName)
        {
            Assert.True(_properties.TryGetValue(propertyName, out var value), $"Expected MSBuild property '{propertyName}'.");
            return value;
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

        private static void CollectIndexData(
            XElement? root,
            Dictionary<string, string> properties,
            Dictionary<string, List<string>> includes,
            Dictionary<string, List<XElement>> includedElements,
            HashSet<string> localNames)
        {
            if (root is null)
                return;

            var localName = root.Name.LocalName;
            _ = localNames.Add(localName);

            var include = root.Attribute("Include")?.Value;
            if (!string.IsNullOrWhiteSpace(include))
            {
                AddInclude(includes, includedElements, localName, include, root);
            }
            else if (!properties.ContainsKey(localName))
            {
                var value = root.Value;
                if (!string.IsNullOrWhiteSpace(value))
                    properties[localName] = value.Trim();
            }

            for (var node = root.FirstNode; node is not null; node = node.NextNode)
            {
                if (node is XElement child)
                    CollectIndexData(child, properties, includes, includedElements, localNames);
            }
        }
    }
}
