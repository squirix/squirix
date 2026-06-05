using System.IO;
using Squirix.Server.TestKit;
using Squirix.Server.TestKit.IO;

namespace Squirix.Server.UnitTests.Architecture;

/// <summary>
/// Resolves repository layout paths for source-based architecture tests.
/// </summary>
public static class ArchitectureRepositoryPaths
{
    /// <summary>
    /// Finds the repository root using embedded MSBuild metadata when available, otherwise walks upward from the test base directory.
    /// </summary>
    /// <returns>The absolute path to the repository root.</returns>
    public static string FindRepositoryRoot() => RepositoryRootFinder.Find();

    /// <summary>
    /// Reads a UTF-8 text file from <c>src/squirix.server</c> under the repository root.
    /// </summary>
    /// <param name="relativePathFromServerProject">Path relative to the server project directory (for example <c>Node\Hosting\Foo.cs</c>).</param>
    /// <returns>File contents.</returns>
    public static string ReadSquirixLibrarySource(string relativePathFromServerProject)
    {
        var root = FindRepositoryRoot();
        return File.ReadAllText(Path.GetFullPath(PathKit.Combine(root, "src", "squirix.server", relativePathFromServerProject)));
    }
}
