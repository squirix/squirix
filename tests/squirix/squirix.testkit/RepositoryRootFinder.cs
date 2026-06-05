using System;
using System.IO;
using System.Reflection;

namespace Squirix.TestKit;

/// <summary>
/// Locates the Squirix repository root by walking up the directory tree looking for <c>squirix.slnx</c>.
/// </summary>
public static class RepositoryRootFinder
{
    private const string RepositoryRootMetadataKey = "Squirix.RepositoryRoot";
    private const string SolutionFileName = "squirix.slnx";
    private const string SourceLayoutProbeFile = "src/squirix/SquirixClient.cs";

    /// <summary>
    /// Walks upward from <paramref name="startDirectory" /> (or <see cref="AppContext.BaseDirectory" />) looking for <c>squirix.slnx</c>.
    /// </summary>
    /// <param name="startDirectory">Directory to begin the walk; defaults to <see cref="AppContext.BaseDirectory" />.</param>
    /// <returns>The normalized absolute path to the repository root.</returns>
    /// <exception cref="InvalidOperationException">When no repository root can be resolved.</exception>
    public static string Find(string? startDirectory = null)
    {
        var dir = new DirectoryInfo(startDirectory ?? AppContext.BaseDirectory);
        while (dir is not null)
        {
            if (File.Exists(Path.Combine(dir.FullName, SolutionFileName)))
                return Path.GetFullPath(dir.FullName);

            dir = dir.Parent;
        }

        throw new InvalidOperationException($"Repository root not found. Expected '{SolutionFileName}' when walking upward from '{startDirectory ?? AppContext.BaseDirectory}'.");
    }

    /// <summary>
    /// Resolves the repository root for tests that read on-disk sources (for example public API shape checks).
    /// Checks MSBuild-embedded <c>Squirix.RepositoryRoot</c> metadata first, then walks upward from
    /// <see cref="AppContext.BaseDirectory" /> and <paramref name="secondaryProbeStartDirectory" />.
    /// Requires both <c>squirix.slnx</c> and <c>src/squirix/SquirixClient.cs</c> to exist.
    /// </summary>
    /// <param name="repositoryRootMetadataAssembly">Assembly that may carry <c>Squirix.RepositoryRoot</c> metadata.</param>
    /// <param name="secondaryProbeStartDirectory">Optional extra walk root when it differs from <see cref="AppContext.BaseDirectory" />.</param>
    /// <returns>The normalized absolute path to the repository root.</returns>
    /// <exception cref="InvalidOperationException">When the repository root cannot be resolved.</exception>
    public static string FindForSourceLayout(Assembly repositoryRootMetadataAssembly, string? secondaryProbeStartDirectory = null)
    {
        ArgumentNullException.ThrowIfNull(repositoryRootMetadataAssembly);

        foreach (var attr in repositoryRootMetadataAssembly.GetCustomAttributes<AssemblyMetadataAttribute>())
        {
            if (!string.Equals(attr.Key, RepositoryRootMetadataKey, StringComparison.Ordinal) || string.IsNullOrWhiteSpace(attr.Value))
            {
                continue;
            }

            var path = Path.GetFullPath(attr.Value.Trim());
            if (IsSourceLayoutRoot(path))
                return path;
        }

        return WalkUpForSourceLayout(AppContext.BaseDirectory) ?? WalkUpForSourceLayout(secondaryProbeStartDirectory) ??
            throw new InvalidOperationException($"Repository root not found. Expected '{SolutionFileName}' and '{SourceLayoutProbeFile}' when walking upward.");
    }

    private static bool IsSourceLayoutRoot(string fullPath) => File.Exists(Path.Combine(fullPath, SolutionFileName)) && File.Exists(Path.Combine(fullPath, SourceLayoutProbeFile));

    private static string? WalkUpForSourceLayout(string? startPath)
    {
        if (string.IsNullOrEmpty(startPath))
            return null;

        var dir = new DirectoryInfo(Path.GetFullPath(startPath));
        while (dir is not null)
        {
            if (IsSourceLayoutRoot(dir.FullName))
                return Path.GetFullPath(dir.FullName);

            dir = dir.Parent;
        }

        return null;
    }
}
