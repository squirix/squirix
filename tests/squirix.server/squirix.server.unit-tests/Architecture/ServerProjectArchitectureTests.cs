using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.TestKit;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Architecture;

/// <summary>Architecture rules for server project packaging, IVT, bootstrap, and dependency baselines.</summary>
[Immutable]
public sealed class ServerProjectArchitectureTests : ServerUnitTestBase
{
    /// <summary>Ensures standalone server bootstrap starts through the public ASP.NET Core hosting extensions.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task BootstrapSourcesUsePackageHostStartup(CancellationToken cancellationToken)
    {
        var sources = await ServerArchitectureFixtures.ReadServerBootstrapSourceTextsAsync(cancellationToken);
        var combined = string.Join(Environment.NewLine, Array.ConvertAll(sources, static source => source.Text));

        _ = await Assert.That(combined).Contains("AddSquirixServerAsync", StringComparison.Ordinal);
        _ = await Assert.That(combined).Contains("MapSquirixServer", StringComparison.Ordinal);
    }

    /// <summary>Ensures the standalone process host stays separate from the packable server runtime.</summary>
    [Test]
    public async Task HostProjectPacksAsGlobalToolExecutable()
    {
        var index = ServerArchitectureFixtures.ParseMsbuildProject(await ServerArchitectureFixtures.LoadProjectAsync("src/squirix.server.host/Squirix.Server.Host.csproj"));

        _ = await Assert.That(await index.RequirePropertyAsync("TargetFramework")).IsEqualTo("net10.0");
        _ = await Assert.That(await index.RequirePropertyAsync("OutputType")).IsEqualTo("Exe");
        _ = await Assert.That(await index.RequirePropertyAsync("AssemblyName")).IsEqualTo("Squirix.Server.Host");
        _ = await Assert.That(await index.RequirePropertyAsync("RootNamespace")).IsEqualTo("Squirix.Server.Host");
        _ = await Assert.That(await index.RequirePropertyAsync("IsPackable")).IsEqualTo("true");
        _ = await Assert.That(await index.RequirePropertyAsync("PackAsTool")).IsEqualTo("true");
        _ = await Assert.That(await index.RequirePropertyAsync("ToolCommandName")).IsEqualTo("squirix-server");
        _ = await Assert.That(await index.RequirePropertyAsync("Version")).IsEqualTo("$(SquirixPackageVersion)");
        _ = await Assert.That(await index.RequirePropertyAsync("PackageVersion")).IsEqualTo("$(SquirixPackageVersion)");
        var projectReferences = index.GetIncludes("ProjectReference");
        _ = await Assert.That(projectReferences).IsNotNull();
        _ = await Assert.That(projectReferences[0]).IsEqualTo(@"..\squirix.server\Squirix.Server.csproj");
    }

    /// <summary>Ensures the server project keeps the approved ASP.NET Core hosting dependency baseline.</summary>
    [Test]
    public async Task HostingDependenciesMatchApprovedBaseline()
    {
        var index = ServerArchitectureFixtures.GetServerProjectIndex();
        var frameworkIncludes = index.GetIncludes("FrameworkReference");
        _ = await Assert.That(frameworkIncludes).IsNotNull();

        _ = await Assert.That(
            ServerArchitectureFixtures.CollectUnexpectedMatches(
                index.GetIncludes("PackageReference"),
                static include => include.Equals("Grpc.AspNetCore", StringComparison.Ordinal) || include.StartsWith("Microsoft.AspNetCore.", StringComparison.Ordinal),
                ServerArchitectureFixtures.KnownServerPackageDependencyBaseline,
                StringComparer.Ordinal)).IsEmpty();

        _ = await Assert.That(
            ServerArchitectureFixtures.CollectUnexpectedMatches(
                frameworkIncludes,
                static include => include.StartsWith("Microsoft.AspNetCore", StringComparison.Ordinal),
                ServerArchitectureFixtures.KnownServerFrameworkDependencyBaseline,
                StringComparer.Ordinal)).IsEmpty();

        _ = await Assert.That(frameworkIncludes).Contains(static include => include.Equals("Microsoft.AspNetCore.App", StringComparison.Ordinal));
    }

    /// <summary>Ensures InternalsVisibleTo grants match the approved server allowlist.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task InternalsVisibleToMatchesAllowlist(CancellationToken cancellationToken)
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
        var text = await File.ReadAllTextAsync(assemblyInfoPath, cancellationToken);
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
        await SequenceAssert.EqualAsync(approved, granted, StringComparer.OrdinalIgnoreCase);
    }

    /// <summary>Ensures the journal thread is joined during disposal instead of being fire-and-forget.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task JournalThreadShouldBeJoinedOnDispose(CancellationToken cancellationToken)
    {
        var root = RepositoryPaths.FindRepositoryRoot();
        var coordinatorText = await File.ReadAllTextAsync(Path.Join(root, "src", "squirix.server", "Storage", "Journaling", "JournalCoordinator.cs"), cancellationToken);
        var durabilityText = await File.ReadAllTextAsync(Path.Join(root, "src", "squirix.server", "Storage", "Journaling", "JournalDurabilityCoordinator.cs"), cancellationToken);

        _ = await Assert.That(durabilityText).Contains("JournalThread.Join(", StringComparison.Ordinal);
        _ = await Assert.That(coordinatorText).Contains("AwaitJournalThreadDuringDisposeAsync", StringComparison.Ordinal);
    }

    /// <summary>Ensures repository projects and sources do not hide dependencies with global or implicit usings.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task NoGlobalOrImplicitUsingsInRepo(CancellationToken cancellationToken)
    {
        var root = RepositoryPaths.FindRepositoryRoot();
        _ = await Assert.That(await ServerArchitectureFixtures.CollectGlobalUsingSourceOffendersAsync(root, cancellationToken)).IsEmpty();
        _ = await Assert.That(await ServerArchitectureFixtures.CollectImplicitUsingsOffendersAsync(root)).IsEmpty();
    }

    /// <summary>Ensures the server runtime project has the required library package metadata.</summary>
    [Test]
    public async Task ServerProjectShouldBePackableLibrary()
    {
        var index = ServerArchitectureFixtures.GetServerProjectIndex();

        _ = await Assert.That(await index.RequirePropertyAsync("TargetFramework")).IsEqualTo("net10.0");
        _ = await Assert.That(index.ContainsElement("OutputType")).IsFalse();
        _ = await Assert.That(await index.RequirePropertyAsync("AssemblyName")).IsEqualTo(ServerArchitectureNamespaces.Root);
        _ = await Assert.That(await index.RequirePropertyAsync("RootNamespace")).IsEqualTo(ServerArchitectureNamespaces.Root);
        _ = await Assert.That(await index.RequirePropertyAsync("PackageId")).IsEqualTo(ServerArchitectureNamespaces.PackageId);
        _ = await Assert.That(await index.RequirePropertyAsync("Version")).IsEqualTo("$(SquirixPackageVersion)");
        _ = await Assert.That(await index.RequirePropertyAsync("PackageVersion")).IsEqualTo("$(SquirixPackageVersion)");
        _ = await Assert.That(await index.RequirePropertyAsync("PackageLicenseExpression")).IsEqualTo("Apache-2.0");
        _ = await Assert.That(await index.RequirePropertyAsync("IsPackable")).IsEqualTo("true");
        _ = await Assert.That(await index.RequirePropertyAsync("TreatWarningsAsErrors")).IsEqualTo("true");
        _ = await Assert.That(await index.RequirePropertyAsync("Nullable")).IsEqualTo("enable");
    }

    /// <summary>Ensures product code does not use access-check bypass attributes.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task SourcesMustNotUseIgnoresAccessChecksTo(CancellationToken cancellationToken)
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

            var text = await File.ReadAllTextAsync(path, cancellationToken);
            _ = await Assert.That(text.Contains("IgnoresAccessChecksTo", StringComparison.Ordinal)).IsFalse();
        }
    }
}
