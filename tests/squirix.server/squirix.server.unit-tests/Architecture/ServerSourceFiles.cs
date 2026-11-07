using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Enumeration;
using System.Threading.Tasks;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;

namespace Squirix.Server.UnitTests.Architecture;

/// <summary>Enumerates <c language="csharp">src/squirix.server</c> C# sources for architecture scans.</summary>
internal static class ServerSourceFiles
{
    /// <summary>
    /// Resolves <c language="csharp">src/squirix.server</c> (optionally a subdirectory), validates it exists,
    /// and returns recursive <c language="csharp">*.cs</c> paths excluding <c language="csharp">obj</c> trees.
    /// </summary>
    /// <param name="relativePathSegments">Optional path segments under the server project root.</param>
    /// <returns>Matching source file paths.</returns>
    internal static async Task<IReadOnlyList<string>> EnumerateCsharpFilesAsync(params string[] relativePathSegments)
    {
        ArgumentNullException.ThrowIfNull(relativePathSegments);

        var serverRoot = Path.Join(RepositoryPaths.FindRepositoryRoot(), "src", "squirix.server");
        _ = await Assert.That(Directory.Exists(serverRoot)).IsTrue().Because($"Expected source root '{serverRoot}'.");

        var searchRoot = serverRoot;
        for (var index = 0; index < relativePathSegments.Length; index++)
            searchRoot = Path.Join(searchRoot, relativePathSegments[index]);

        _ = await Assert.That(Directory.Exists(searchRoot)).IsTrue().Because($"Expected source root '{searchRoot}'.");

        return new List<string>(EnumerateUnder(searchRoot));
    }

    /// <summary>Enumerates C# sources without descending into excluded build directories.</summary>
    /// <param name="searchRoot">The directory to scan.</param>
    /// <param name="excludeBin">Whether bin trees are excluded as well as obj trees.</param>
    /// <returns>Matching paths in the underlying directory enumeration order.</returns>
    internal static IEnumerable<string> EnumerateUnder(string searchRoot, bool excludeBin = false)
    {
        var separator = Path.DirectorySeparatorChar;
        var root = Path.GetFullPath(searchRoot) + separator;
        if (root.Contains($"{separator}obj{separator}", StringComparison.Ordinal) ||
            (excludeBin && root.Contains($"{separator}bin{separator}", StringComparison.Ordinal)))
            return [];

        return new FileSystemEnumerable<string>(
            searchRoot,
            static (ref FileSystemEntry entry) => entry.ToFullPath(),
            new EnumerationOptions { RecurseSubdirectories = true, IgnoreInaccessible = false, AttributesToSkip = FileAttributes.None })
        {
            ShouldIncludePredicate = static (ref FileSystemEntry entry) =>
                !entry.IsDirectory && FileSystemName.MatchesWin32Expression("*.cs", entry.FileName, OperatingSystem.IsWindows()),
            ShouldRecursePredicate = (ref FileSystemEntry entry) =>
                entry.FileName is not "obj" && (!excludeBin || entry.FileName is not "bin"),
        };
    }
}
