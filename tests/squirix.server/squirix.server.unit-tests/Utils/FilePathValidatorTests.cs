using System;
using System.IO;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.TestKit;
using Squirix.Server.UnitTests.Support;
using Squirix.Server.Utils;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Utils;

/// <summary>Covers operator path validation used before file I/O.</summary>
[Immutable]
public sealed class FilePathValidatorTests : IsolatedStorageTestBase
{
    /// <summary>PathEx multi-segment combine keeps results under the Dir.</summary>
    [Test]
    public async Task CombineAcceptsMultipleSegments()
    {
        var combined = PathEx.Combine(Dir.Path, "a", "b");
        _ = await Assert.That(combined).IsEqualTo(Path.GetFullPath(Path.Join(Dir.Path, "a", "b")));

        var triple = PathEx.Combine(Dir.Path, "a", "b", "c");
        _ = await Assert.That(triple).IsEqualTo(Path.GetFullPath(Path.Join(Dir.Path, "a", "b", "c")));
    }

    /// <summary>FileEx.TryDeleteFile treats traversal paths as skipped successes.</summary>
    [Test]
    public async Task FileExTryDeleteFileSkipsTraversalPaths() => _ = await Assert.That(FileEx.TryDeleteFile("../nope.txt")).IsTrue();

    /// <summary>PathEx relative joins reject parent-directory segments.</summary>
    [Test]
    public async Task PathExCombineRejectsParentSegments()
    {
        var root = Path.GetTempPath();
        var ex = NodeExceptionAssert.For<ArgumentException>().Throws(root, static value => PathEx.Combine(value, "foo/../bar"));
        _ = await Assert.That(ex.Message).Contains("'.' or '..'", StringComparison.Ordinal);
    }

    /// <summary>Accepts an absolute directory path without traversal segments.</summary>
    [Test]
    public async Task ResolveDirAcceptsAbsolutePath()
    {
        var input = Path.Join(Path.GetTempPath(), "squirix-path-validator");
        var full = FilePathValidator.ResolveValidatedDirectoryPath(input);
        _ = await Assert.That(full).IsEqualTo(Path.GetFullPath(input));
    }

    /// <summary>Accepts a simple relative file path and returns an absolute path.</summary>
    [Test]
    public async Task ResolveFileAcceptsRelativeName()
    {
        var full = FilePathValidator.ResolveValidatedFilePath("Squirix.settings.json");
        _ = await Assert.That(Path.IsPathRooted(full)).IsTrue();
        _ = await Assert.That(full).EndsWith("Squirix.settings.json", StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>Rejects parent-directory segments in operator paths.</summary>
    /// <param name="path">Path containing <c language="csharp">.</c> or <c language="csharp">..</c> segments.</param>
    [Test]
    [Arguments("..")]
    [Arguments("../Squirix.settings.json")]
    [Arguments("foo/../bar.json")]
    [Arguments("foo/./bar.json")]
    public async Task ResolveFileRejectsDotSegments(string path)
    {
        var ex = NodeExceptionAssert.For<ArgumentException>().Throws(path, static value => FilePathValidator.ResolveValidatedFilePath(value));
        _ = await Assert.That(ex.Message).Contains("'.' or '..'", StringComparison.Ordinal);
    }

    /// <summary>Rejects empty and whitespace paths.</summary>
    /// <param name="path">Empty or whitespace path.</param>
    [Test]
    [Arguments("")]
    [Arguments("   ")]
    public void ResolveValidatedFilePathRejectsEmpty(string? path) =>
        _ = NodeExceptionAssert.For<ArgumentException>().Throws(path, static value => FilePathValidator.ResolveValidatedFilePath(value!));

    /// <summary>Rejects null paths with an ArgumentNullException.</summary>
    [Test]
    public void ResolveValidatedFilePathRejectsNull()
    {
        const string? path = null;
        _ = NodeExceptionAssert.For<ArgumentNullException>().Throws(path, static value => FilePathValidator.ResolveValidatedFilePath(value!));
    }

    /// <summary>Rejects wildcards in operator paths.</summary>
    /// <param name="path">Path containing wildcards.</param>
    [Test]
    [Arguments("*.json")]
    [Arguments("settings?.json")]
    public async Task ResolveValidatedFilePathRejectsWildcards(string path)
    {
        var ex = NodeExceptionAssert.For<ArgumentException>().Throws(path, static value => FilePathValidator.ResolveValidatedFilePath(value));
        _ = await Assert.That(ex.Message).Contains("wildcard", StringComparison.OrdinalIgnoreCase);
    }
}
