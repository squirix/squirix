using System;
using System.IO;
using Squirix.TestKit;
using Squirix.TestKit.IO;
using Xunit;

namespace Squirix.UnitTests.TestKit;

/// <summary>
/// Focused coverage for <see cref="RepositoryRootFinder" />.
/// </summary>
public sealed class RepositoryRootFinderTests
{
    /// <summary>
    /// Verifies <see cref="RepositoryRootFinder.FindForSourceLayout" /> resolves the real repository using the same probes as public API source tests.
    /// </summary>
    [Fact]
    public void FindForSourceLayoutReturnsRootWithSolutionAndSourceProbe()
    {
        var root = RepositoryRootFinder.FindForSourceLayout(typeof(RepositoryRootFinderTests).Assembly, Path.GetDirectoryName(typeof(ICache<>).Assembly.Location));

        AssertRootHasRepositorySolutionFile(root);
        AssertRootHasClientSource(root);
    }

    /// <summary>
    /// Verifies the finder resolves the repository when the walk starts from a nested directory under the output tree.
    /// </summary>
    [Fact]
    public void FindReturnsRootFromNestedStartDirectory()
    {
        var parent = Path.Combine(AppContext.BaseDirectory, "nested");
        var nested = Path.Combine(parent, "deep");
        try
        {
            DirectoryKit.CreateDirectory(nested, AppContext.BaseDirectory);

            var root = RepositoryRootFinder.Find(nested);
            AssertRootHasRepositorySolutionFile(root);
        }
        finally
        {
            DirectoryKit.TryDeleteDirectory(parent);
        }
    }

    /// <summary>
    /// Verifies the finder resolves the repository when the walk starts from the test output directory.
    /// </summary>
    [Fact]
    public void FindReturnsRootFromTestOutputBaseDirectory()
    {
        var root = RepositoryRootFinder.Find();
        AssertRootHasRepositorySolutionFile(root);
    }

    /// <summary>
    /// Verifies the finder fails with a clear error when no repository markers exist along the parent chain.
    /// </summary>
    [Fact]
    public void FindThrowsWhenMarkersAreMissing()
    {
        var temp = DirectoryKit.CreateTempDirectory("squirix-repo-root-missing");
        try
        {
            var leaf = Path.Combine(temp, "a", "b");
            DirectoryKit.CreateDirectory(leaf, temp);

            _ = Assert.Throws<InvalidOperationException>(() => RepositoryRootFinder.Find(leaf));
        }
        finally
        {
            DirectoryKit.TryDeleteDirectory(temp);
        }
    }

    /// <summary>
    /// Verifies resolution uses the explicit start directory rather than assuming only the current working directory.
    /// </summary>
    [Fact]
    public void FindUsesExplicitStartDirectoryNotOnlyCurrentWorkingDirectory()
    {
        var temp = DirectoryKit.CreateTempDirectory("squirix-repo-root-explicit");
        try
        {
            var fakeRoot = Path.Combine(temp, "repo");
            DirectoryKit.CreateDirectory(fakeRoot, temp);
            FileKit.WriteAllText(Path.Combine(fakeRoot, "squirix.slnx"), string.Empty);
            var nested = Path.Combine(fakeRoot, "out", "bin");
            DirectoryKit.CreateDirectory(nested, fakeRoot);

            var resolved = RepositoryRootFinder.Find(nested);
            Assert.Equal(Path.GetFullPath(fakeRoot), Path.GetFullPath(resolved));
        }
        finally
        {
            DirectoryKit.TryDeleteDirectory(temp);
        }
    }

    private static void AssertRootHasClientSource(string root) => Assert.True(File.Exists(PathKit.Combine(root, "src", "squirix", "SquirixClient.cs")));

    private static void AssertRootHasRepositorySolutionFile(string root) => Assert.True(
        File.Exists(PathKit.Combine(root, "squirix.slnx")),
        "Expected squirix.slnx at repository root.");
}
