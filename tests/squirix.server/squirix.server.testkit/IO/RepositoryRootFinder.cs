using System;
using System.IO;

namespace Squirix.Server.TestKit.IO;

/// <summary>
/// Locates the Squirix repository root by walking up the directory tree looking for <c>squirix.slnx</c>.
/// </summary>
public static class RepositoryRootFinder
{
    private const string SolutionFileName = "squirix.slnx";

    /// <summary>
    /// Walks upward from <see cref="AppContext.BaseDirectory" /> looking for <c>squirix.slnx</c>.
    /// </summary>
    /// <returns>The normalized absolute path to the repository root.</returns>
    /// <exception cref="InvalidOperationException">When no repository root can be resolved.</exception>
    public static string Find()
    {
        var dir = new DirectoryInfo(AppContext.BaseDirectory);
        while (dir != null)
        {
            if (File.Exists(NodePathKit.Combine(dir.FullName, SolutionFileName)))
                return Path.GetFullPath(dir.FullName);

            dir = dir.Parent;
        }

        throw new InvalidOperationException($"Repository root not found. Expected '{SolutionFileName}' when walking upward from '{AppContext.BaseDirectory}'.");
    }
}
