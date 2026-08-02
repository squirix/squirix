using System;
using System.Collections.Generic;
using System.IO;
using Xunit;

namespace Squirix.Server.UnitTests.Architecture;

/// <summary>Enumerates <c>src/squirix.server</c> C# sources for architecture scans.</summary>
internal static class ServerSourceFiles
{
    /// <summary>
    /// Resolves <c>src/squirix.server</c> (optionally a subdirectory), validates it exists,
    /// and returns recursive <c>*.cs</c> paths excluding <c>obj</c> trees.
    /// </summary>
    /// <param name="relativePathSegments">Optional path segments under the server project root.</param>
    /// <returns>Matching source file paths.</returns>
    internal static IReadOnlyList<string> EnumerateCsharpFiles(params string[] relativePathSegments)
    {
        ArgumentNullException.ThrowIfNull(relativePathSegments);

        var serverRoot = Path.Join(RepositoryPaths.FindRepositoryRoot(), "src", "squirix.server");
        Assert.True(Directory.Exists(serverRoot), $"Expected source root '{serverRoot}'.");

        var searchRoot = serverRoot;
        for (var index = 0; index < relativePathSegments.Length; index++)
            searchRoot = Path.Join(searchRoot, relativePathSegments[index]);

        Assert.True(Directory.Exists(searchRoot), $"Expected source root '{searchRoot}'.");

        var objMarker = $"{Path.DirectorySeparatorChar}obj{Path.DirectorySeparatorChar}";
        var files = Directory.GetFiles(searchRoot, "*.cs", SearchOption.AllDirectories);
        var results = new List<string>(files.Length);
        for (var index = 0; index < files.Length; index++)
        {
            var path = files[index];
            if (path.Contains(objMarker, StringComparison.Ordinal))
                continue;

            results.Add(path);
        }

        return results;
    }
}
