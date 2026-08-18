using System;
using System.Collections.Frozen;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using System.Xml;
using System.Xml.XPath;
using Xunit;

namespace Squirix.Server.UnitTests.Architecture;

/// <summary>Shared MSBuild project loaders and dependency baselines for server architecture tests.</summary>
internal static class ServerArchitectureFixtures
{
    internal static readonly string[] ForbiddenSharedGrpcTransportMapperRuntimeMarkers =
    [
        "ICacheRuntime",
        "ILogicalNamespacedCache",
        "ICacheApi<",
        "LocalCache<",
        "ClusteredCache<",
        "JournalCoordinator",
        "Coordinator",
        "Squirix.Storage.Journaling",
        "Squirix.Storage.Snapshot",
        "Squirix.Runtime",
    ];

    internal static readonly string[] KnownServerFrameworkDependencyBaseline =
    [
        "Microsoft.AspNetCore.App",
    ];

    internal static readonly string[] KnownServerPackageDependencyBaseline =
    [
        "Grpc.AspNetCore",
        "Microsoft.AspNetCore.Authentication.JwtBearer",
    ];

    private const string ServerProjectRelativePath = "src/squirix.server/Squirix.Server.csproj";

    private static readonly string[] ServerBootstrapSourceRelativePaths =
    [
        "src/squirix.server.host/Program.cs",
    ];

    private static readonly Lazy<XPathNavigator> ServerProject = new(LoadServerProject);

    private static readonly Lazy<MsbuildProjectIndex> ServerProjectIndex = new(static () => ParseMsbuildProject(ServerProject.Value));

    /// <summary>
    /// Scans repository <c>.cs</c> sources for <c>global using</c> directives or a <c>GlobalUsings.cs</c> file.
    /// </summary>
    /// <param name="repositoryRoot">Absolute path to the repository root.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>Sorted repo-relative paths of offending sources.</returns>
    internal static async Task<List<string>> CollectGlobalUsingSourceOffendersAsync(string repositoryRoot, CancellationToken cancellationToken)
    {
        var sourceOffenders = new List<string>();
        foreach (var path in Directory.GetFiles(repositoryRoot, "*.cs", SearchOption.AllDirectories))
        {
            // Skip build outputs so generated GlobalUsings under obj/ do not fail architecture tests.
            if (IsGeneratedOutputPath(path))
                continue;

            if (Path.GetFileName(path).Equals("GlobalUsings.cs", StringComparison.OrdinalIgnoreCase))
            {
                sourceOffenders.Add(Path.GetRelativePath(repositoryRoot, path).Replace(Path.DirectorySeparatorChar, '/'));
                continue;
            }

            var hasGlobalUsing = false;
            foreach (var line in await File.ReadAllLinesAsync(path, cancellationToken))
            {
                if (!line.TrimStart().StartsWith("global using ", StringComparison.Ordinal))
                    continue;

                hasGlobalUsing = true;
                break;
            }

            if (hasGlobalUsing)
                sourceOffenders.Add(Path.GetRelativePath(repositoryRoot, path).Replace(Path.DirectorySeparatorChar, '/'));
        }

        sourceOffenders.Sort(StringComparer.Ordinal);
        return sourceOffenders;
    }

    /// <summary>
    /// Scans repository <c>.csproj</c> files for <c>ImplicitUsings</c> set to <c>enable</c>.
    /// </summary>
    /// <param name="repositoryRoot">Absolute path to the repository root.</param>
    /// <returns>Sorted repo-relative paths of offending projects.</returns>
    internal static List<string> CollectImplicitUsingsProjectOffenders(string repositoryRoot)
    {
        var projectOffenders = new List<string>();
        foreach (var path in Directory.GetFiles(repositoryRoot, "*.csproj", SearchOption.AllDirectories))
        {
            if (IsGeneratedOutputPath(path))
                continue;

            var hasImplicitUsings = false;
            var navigator = LoadProject(path);
            var elements = navigator.Select("//*");
            while (elements.MoveNext())
            {
                var element = elements.Current!;
                if (!string.Equals(element.LocalName, "ImplicitUsings", StringComparison.OrdinalIgnoreCase))
                    continue;

                if (!element.Value.Trim().Equals("enable", StringComparison.OrdinalIgnoreCase))
                    continue;

                hasImplicitUsings = true;
                break;
            }

            if (hasImplicitUsings)
                projectOffenders.Add(Path.GetRelativePath(repositoryRoot, path).Replace(Path.DirectorySeparatorChar, '/'));
        }

        projectOffenders.Sort(StringComparer.Ordinal);
        return projectOffenders;
    }

    internal static List<string> CollectUnexpectedMatches(List<string>? includes, Func<string, bool> isMatch, string[] baseline, StringComparer comparer)
    {
        var unexpected = new List<string>();
        if (includes == null)
            return unexpected;

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

    internal static MsbuildProjectIndex GetServerProjectIndex() => ServerProjectIndex.Value;

    internal static XPathNavigator LoadProject(string relativeOrAbsolutePath)
    {
        var path = Path.IsPathRooted(relativeOrAbsolutePath) ? relativeOrAbsolutePath : Path.Join(
            RepositoryPaths.FindRepositoryRoot(),
            relativeOrAbsolutePath.Replace('/', Path.DirectorySeparatorChar));
        Assert.True(File.Exists(path));

        var document = new XmlDocument();
        document.Load(path);
        return document.CreateNavigator()!;
    }

    internal static MsbuildProjectIndex ParseMsbuildProject(XPathNavigator project)
    {
        var properties = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        var includes = new Dictionary<string, List<string>>(StringComparer.OrdinalIgnoreCase);
        var includedElements = new Dictionary<string, List<XPathNavigator>>(StringComparer.OrdinalIgnoreCase);
        var localNames = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

        CollectIndexData(project, properties, includes, includedElements, localNames);

        return new MsbuildProjectIndex(
            properties.ToFrozenDictionary(StringComparer.OrdinalIgnoreCase),
            includes.ToFrozenDictionary(StringComparer.OrdinalIgnoreCase),
            includedElements.ToFrozenDictionary(StringComparer.OrdinalIgnoreCase),
            localNames.ToFrozenSet(StringComparer.OrdinalIgnoreCase));
    }

    internal static async Task<(string RelativePath, string Text)[]> ReadServerBootstrapSourceTextsAsync(CancellationToken cancellationToken)
    {
        var root = RepositoryPaths.FindRepositoryRoot();
        var paths = ServerBootstrapSourceRelativePaths;

        var sources = new (string RelativePath, string Text)[paths.Length];
        for (var i = 0; i < paths.Length; i++)
        {
            var relativePath = paths[i].Replace('/', Path.DirectorySeparatorChar);
            var absolutePath = Path.Join(root, relativePath);
            Assert.True(File.Exists(absolutePath));
            sources[i] = (relativePath, await File.ReadAllTextAsync(absolutePath, cancellationToken));
        }

        return sources;
    }

    private static void AddInclude(
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

    private static void CollectIndexData(
        XPathNavigator root,
        Dictionary<string, string> properties,
        Dictionary<string, List<string>> includes,
        Dictionary<string, List<XPathNavigator>> includedElements,
        HashSet<string> localNames)
    {
        var localName = root.LocalName;
        _ = localNames.Add(localName);

        var include = root.GetAttribute("Include", string.Empty);
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

        var children = root.SelectChildren(XPathNodeType.Element);
        while (children.MoveNext())
            CollectIndexData(children.Current!, properties, includes, includedElements, localNames);
    }

    private static bool IsGeneratedOutputPath(string path)
    {
        var normalized = path.Replace(Path.AltDirectorySeparatorChar, Path.DirectorySeparatorChar);
        var objMarker = $"{Path.DirectorySeparatorChar}obj{Path.DirectorySeparatorChar}";
        var binMarker = $"{Path.DirectorySeparatorChar}bin{Path.DirectorySeparatorChar}";
        var artifactsMarker = $"{Path.DirectorySeparatorChar}artifacts{Path.DirectorySeparatorChar}";
        var ndependOutMarker = $"{Path.DirectorySeparatorChar}NDependOut{Path.DirectorySeparatorChar}";
        return normalized.Contains(objMarker, StringComparison.OrdinalIgnoreCase) || normalized.Contains(binMarker, StringComparison.OrdinalIgnoreCase) ||
               normalized.Contains(artifactsMarker, StringComparison.OrdinalIgnoreCase) || normalized.Contains(ndependOutMarker, StringComparison.OrdinalIgnoreCase);
    }

    private static XPathNavigator LoadServerProject()
    {
        var path = Path.Join(RepositoryPaths.FindRepositoryRoot(), ServerProjectRelativePath.Replace('/', Path.DirectorySeparatorChar));
        Assert.True(File.Exists(path));

        var document = new XmlDocument();
        document.Load(path);
        return document.CreateNavigator()!;
    }
}
