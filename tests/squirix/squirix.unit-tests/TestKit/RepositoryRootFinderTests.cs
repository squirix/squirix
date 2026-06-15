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
        var name = Path.GetDirectoryName(typeof(ICache<>).Assembly.Location) ?? string.Empty;
        var root = RepositoryRootFinder.FindForSourceLayout(typeof(RepositoryRootFinderTests).Assembly, name);

        AssertRootHasRepositorySolutionFile(root);
        AssertRootHasClientSource(root);
    }

    /// <summary>
    /// Verifies the finder resolves the repository when the walk starts from a nested directory under the output tree.
    /// </summary>
    [Fact]
    public void FindReturnsRootFromNestedStartDirectory()
    {
        using var parent = TempDirectory.CreateUnder(AppContext.BaseDirectory, "nested");
        var nested = PathKit.Combine(parent, "deep");
        DirectoryKit.CreateDirectory(nested, AppContext.BaseDirectory);

        var root = RepositoryRootFinder.Find(nested);
        AssertRootHasRepositorySolutionFile(root);
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
        using var temp = new TempDirectory("squirix-repo-root-missing");
        var leaf = PathKit.Combine(temp, "a", "b");
        DirectoryKit.CreateDirectory(leaf, temp);

        _ = Assert.Throws<InvalidOperationException>(() => RepositoryRootFinder.Find(leaf));
    }

    /// <summary>
    /// Verifies resolution uses the explicit start directory rather than assuming only the current working directory.
    /// </summary>
    [Fact]
    public void FindUsesExplicitStartDirectoryNotOnlyCurrentWorkingDirectory()
    {
        using var temp = new TempDirectory("squirix-repo-root-explicit");
        var fakeRoot = PathKit.Combine(temp, "repo");
        DirectoryKit.CreateDirectory(fakeRoot, temp);
        FileKit.WriteAllText(PathKit.Combine(fakeRoot, "squirix.slnx"), string.Empty);
        var nested = PathKit.Combine(fakeRoot, "out", "bin");
        DirectoryKit.CreateDirectory(nested, fakeRoot);

        var resolved = RepositoryRootFinder.Find(nested);
        Assert.Equal(Path.GetFullPath(fakeRoot), Path.GetFullPath(resolved));
    }

    private static void AssertRootHasClientSource(string root) => Assert.True(File.Exists(PathKit.Combine(root, "src", "squirix", "SquirixClient.cs")));

    private static void AssertRootHasRepositorySolutionFile(string root) => Assert.True(
        File.Exists(PathKit.Combine(root, "squirix.slnx")),
        "Expected squirix.slnx at repository root.");
}
