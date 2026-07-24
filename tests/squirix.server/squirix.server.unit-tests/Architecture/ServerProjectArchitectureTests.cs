using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using Squirix.Server.UnitTests.Support;
using Xunit;

namespace Squirix.Server.UnitTests.Architecture;

/// <summary>Architecture rules for server project packaging, IVT, bootstrap, and dependency baselines.</summary>
public sealed class ServerProjectArchitectureTests : ServerUnitTestBase
{
    /// <summary>Ensures the journal thread is joined during disposal instead of being fire-and-forget.</summary>
    [Fact]
    public async Task JournalThreadShouldBeJoinedOnDispose()
    {
        var root = RepositoryPaths.FindRepositoryRoot();
        var coordinatorText = await File.ReadAllTextAsync(Path.Join(root, "src", "squirix.server", "Storage", "Journaling", "JournalCoordinator.cs"), DefaultCancellationToken);
        var durabilityText = await File.ReadAllTextAsync(
            Path.Join(root, "src", "squirix.server", "Storage", "Journaling", "JournalCoordinatorDurabilityPipeline.cs"),
            DefaultCancellationToken);

        Assert.Contains("JournalThread.Join(", durabilityText, StringComparison.Ordinal);
        Assert.Contains("AwaitJournalThreadDuringDisposeAsync", coordinatorText, StringComparison.Ordinal);
    }

    /// <summary>Ensures product code does not use access-check bypass attributes.</summary>
    [Fact]
    public async Task ProductionSourcesShouldNotUseIgnoresAccessChecksTo()
    {
        var root = Path.Join(RepositoryPaths.FindRepositoryRoot(), "src");
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
    public async Task RepositoryShouldNotUseGlobalOrImplicitUsings()
    {
        var root = RepositoryPaths.FindRepositoryRoot();
        Assert.Empty(await ServerArchitectureFixtures.CollectGlobalUsingSourceOffendersAsync(root, DefaultCancellationToken));
        Assert.Empty(ServerArchitectureFixtures.CollectImplicitUsingsProjectOffenders(root));
    }

    /// <summary>Ensures the server package does not reference the client SDK assembly.</summary>
    [Fact]
    public void ServerAssemblyShouldNotReferenceSquirix()
    {
        var references = ServerArchitectureFixtures.GetServerProjectIndex().GetIncludes("ProjectReference");
        Assert.DoesNotContain(references, static reference => reference.Contains(@"..\squirix\Squirix.csproj", StringComparison.OrdinalIgnoreCase));
    }

    /// <summary>Ensures standalone server bootstrap starts through the public ASP.NET Core hosting extensions.</summary>
    [Fact]
    public async Task ServerBootstrapSourcesUsePackageHostStartupApi()
    {
        var sources = await ServerArchitectureFixtures.ReadServerBootstrapSourceTextsAsync(DefaultCancellationToken);
        var combined = string.Join(Environment.NewLine, Array.ConvertAll(sources, static source => source.Text));

        Assert.Contains("AddSquirixServerAsync", combined, StringComparison.Ordinal);
        Assert.Contains("MapSquirixServer", combined, StringComparison.Ordinal);
    }

    /// <summary>Ensures the standalone process host stays separate from the packable server runtime.</summary>
    [Fact]
    public void ServerHostProjectBePackableGlobalToolExecutable()
    {
        var index = ServerArchitectureFixtures.ParseMsbuildProject(ServerArchitectureFixtures.LoadProject("src/squirix.server.host/Squirix.Server.Host.csproj"));

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
    public async Task ServerInternalsVisibleToMatchApprovedAllowlist()
    {
        string[] approved =
        [
            "Squirix.Server.UnitTests",
            "Squirix.Server.IntegrationTests",
            "Squirix.Server.SmokeTests",
            "Squirix.Server.TestKit",
            "Squirix.Server.Benchmarks",
            "squirix-test-host",
            "sqr-ring-distribution",
            "DynamicProxyGenAssembly2",
        ];

        var root = RepositoryPaths.FindRepositoryRoot();
        var assemblyInfoPath = Path.Join(root, "src", "squirix.server", "Properties", "AssemblyInfo.cs");
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

    /// <summary>Ensures the server runtime project has the required library package metadata.</summary>
    [Fact]
    public void ServerProjectShouldBePackableLibrary()
    {
        var index = ServerArchitectureFixtures.GetServerProjectIndex();

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

    /// <summary>Ensures the server project keeps the approved ASP.NET Core hosting dependency baseline.</summary>
    [Fact]
    public void ServerProjectKeepApprovedHostingDependencyBaseline()
    {
        var index = ServerArchitectureFixtures.GetServerProjectIndex();
        var frameworkIncludes = index.GetIncludes("FrameworkReference");

        Assert.Empty(
            ServerArchitectureFixtures.CollectUnexpectedMatches(
                index.GetIncludes("PackageReference"),
                static include => include.Equals("Grpc.AspNetCore", StringComparison.Ordinal) || include.StartsWith("Microsoft.AspNetCore.", StringComparison.Ordinal),
                ServerArchitectureFixtures.KnownServerPackageDependencyBaseline,
                StringComparer.Ordinal));

        Assert.Empty(
            ServerArchitectureFixtures.CollectUnexpectedMatches(
                frameworkIncludes,
                static include => include.StartsWith("Microsoft.AspNetCore", StringComparison.Ordinal),
                ServerArchitectureFixtures.KnownServerFrameworkDependencyBaseline,
                StringComparer.Ordinal));

        Assert.Contains(frameworkIncludes, static include => include.Equals("Microsoft.AspNetCore.App", StringComparison.Ordinal));
    }

    /// <summary>Ensures the server project does not reference the client SDK project.</summary>
    [Fact]
    public void ServerProjectShouldNotReferenceSquirixProject()
    {
        var list = ServerArchitectureFixtures.GetServerProjectIndex().GetIncludes("ProjectReference");

        Assert.DoesNotContain(
            list,
            static reference => reference.Contains("squirix.csproj", StringComparison.OrdinalIgnoreCase) &&
                                !reference.Contains("squirix.server", StringComparison.OrdinalIgnoreCase));
        Assert.DoesNotContain(list, static reference => reference.Contains(@"..\squirix\Squirix.csproj", StringComparison.Ordinal));
    }
}
