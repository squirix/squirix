using System;
using Squirix.Server.TestKit.IO;

namespace Squirix.Server.UnitTests.Architecture;

/// <summary>Resolves repository layout paths for source-based architecture tests.</summary>
internal static class ArchitectureRepositoryPaths
{
    private static readonly Lazy<string> RepositoryRoot = new(static () => RepositoryRootFinder.Find());

    /// <summary>Finds the repository root using embedded MSBuild metadata when available, otherwise walks upward from the test base directory.</summary>
    /// <returns>The absolute path to the repository root.</returns>
    internal static string FindRepositoryRoot() => RepositoryRoot.Value;
}
