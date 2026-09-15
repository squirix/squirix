using System;
using System.IO;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.TestKit;
using Squirix.Server.TestKit.IO;
using Squirix.Server.UnitTests.Support;
using Squirix.Server.Utils;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Utils;

/// <summary>Covers directory path resolution and segment parsing.</summary>
[Immutable]
public sealed class DirectoryPathValidatorTests : ServerUnitTestBase
{
    /// <summary>IsDirectorySeparator recognizes both separators.</summary>
    [Test]
    public async Task IsDirectorySeparatorRecognizesBoth()
    {
        _ = await Assert.That(DirectoryPathHelpers.IsDirectorySeparator(Path.DirectorySeparatorChar)).IsTrue();
        _ = await Assert.That(DirectoryPathHelpers.IsDirectorySeparator(Path.AltDirectorySeparatorChar)).IsTrue();
        _ = await Assert.That(DirectoryPathHelpers.IsDirectorySeparator('x')).IsFalse();
    }

    /// <summary>TryReadNextSegment skips separators and returns segments.</summary>
    [Test]
    public async Task ReadNextSegmentReadsSegments()
    {
        var (firstOk, first, secondOk, second, thirdOk) = ReadSegments();
        _ = await Assert.That(firstOk).IsTrue();
        _ = await Assert.That(first).IsEqualTo("a");
        _ = await Assert.That(secondOk).IsTrue();
        _ = await Assert.That(second).IsEqualTo("b");
        _ = await Assert.That(thirdOk).IsFalse();
        return;

        static (bool FirstOk, string First, bool SecondOk, string Second, bool ThirdOk) ReadSegments()
        {
            var path = "/a//b/".AsSpan();
            var firstOk = PathEx.TryReadNextSegment(ref path, out var first);
            var secondOk = PathEx.TryReadNextSegment(ref path, out var second);
            var thirdOk = PathEx.TryReadNextSegment(ref path, out _);
            return (firstOk, first.ToString(), secondOk, second.ToString(), thirdOk);
        }
    }

    /// <summary>Resolves a relative path under a base directory.</summary>
    [Test]
    public async Task ResolveDirAcceptsRelativeBase()
    {
        using var root = new TempDirectory("squirix-dirpath-rel");
        var full = DirectoryPathValidator.ResolveValidatedDirectoryPath("child", root.Path, true);
        _ = await Assert.That(Path.IsPathRooted(full)).IsTrue();
        _ = await Assert.That(full).StartsWith(root.Path, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>Creates a missing base directory when provided.</summary>
    [Test]
    public async Task ResolveDirCreatesMissingBase()
    {
        using var root = new TempDirectory("squirix-dirpath-base-create");
        var baseDir = Path.Join(root.Path, "missing-base");
        var full = DirectoryPathValidator.ResolveValidatedDirectoryPath("child", baseDir, false);
        _ = await Assert.That(Directory.Exists(baseDir)).IsTrue();
        _ = await Assert.That(full).StartsWith(baseDir, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>Rejects targets that escape the base directory.</summary>
    [Test]
    public async Task ResolveDirRejectsBaseEscape()
    {
        using var root = new TempDirectory("squirix-dirpath-escape");
        var parent = Directory.GetParent(root.Path);
        _ = await Assert.That(parent).IsNotNull();
        var outside = Path.Join(parent.FullName, NodeInvariantIndexStrings.FormatPrefixedGuidN("squirix-dirpath-outside-"));
        _ = NodeExceptionAssert.For<UnauthorizedAccessException>().Throws(
            outside,
            root.Path,
            static (path, basePath) => DirectoryPathValidator.ResolveValidatedDirectoryPath(path, basePath, true));
    }

    /// <summary>Rejects empty paths.</summary>
    [Test]
    public void ResolveDirRejectsEmpty() => _ = NodeExceptionAssert.For<ArgumentException>().Throws(
        "  ",
        static value => DirectoryPathValidator.ResolveValidatedDirectoryPath(value, null, false));

    /// <summary>Rejects when a regular file already exists at the target.</summary>
    [Test]
    public void ResolveDirRejectsExistingFile()
    {
        using var root = new TempDirectory("squirix-dirpath-file");
        var target = Path.Join(root.Path, "blocked");
        File.WriteAllText(target, "x");
        _ = NodeExceptionAssert.For<IOException>().Throws(root.Path, static basePath => DirectoryPathValidator.ResolveValidatedDirectoryPath("blocked", basePath, true));
    }
}
