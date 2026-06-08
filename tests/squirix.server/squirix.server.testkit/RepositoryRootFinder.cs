using System;
using System.IO;
using Squirix.Server.TestKit.IO;

namespace Squirix.Server.TestKit;

/// <summary>
/// Locates the Squirix repository root by walking up the directory tree looking for <c>squirix.slnx</c>.
/// </summary>
public static class RepositoryRootFinder
{
    private const string SolutionFileName = "squirix.slnx";

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
            if (File.Exists(PathKit.Combine(dir.FullName, SolutionFileName)))
                return Path.GetFullPath(dir.FullName);

            dir = dir.Parent;
        }

        throw new InvalidOperationException($"Repository root not found. Expected '{SolutionFileName}' when walking upward from '{startDirectory ?? AppContext.BaseDirectory}'.");
    }
}
