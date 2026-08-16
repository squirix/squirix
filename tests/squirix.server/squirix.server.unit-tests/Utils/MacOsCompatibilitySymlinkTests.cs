using System;
using System.IO;
using JetBrains.Annotations;
using Squirix.Attributes;
using Squirix.Server.TestKit.IO;
using Squirix.Server.UnitTests.Support;
using Squirix.Server.Utils;
using Xunit;

namespace Squirix.Server.UnitTests.Utils;

/// <summary>Covers Darwin compatibility symlink helpers without requiring a macOS host.</summary>
[UsedImplicitly]
[Immutable]
public sealed class MacOsCompatibilitySymlinkTests : ServerUnitTestBase
{
    /// <summary>Allowlisted root link names are recognized.</summary>
    /// <param name="name">Candidate name.</param>
    /// <param name="expected">Expected allowlist result.</param>
    [Theory]
    [InlineData("var", true)]
    [InlineData("tmp", true)]
    [InlineData("etc", true)]
    [InlineData("usr", false)]
    [InlineData("VAR", false)]
    public static void IsAllowlistedRootLinkNameMatches(string name, bool expected) => Assert.Equal(expected, MacOsCompatibilitySymlink.IsAllowlistedRootLinkName(name));

    /// <summary>Private-target comparison is case-insensitive.</summary>
    [Fact]
    public static void IsExpectedPrivateTargetIgnoresCase()
    {
        Assert.True(MacOsCompatibilitySymlink.IsExpectedPrivateTarget("/private/tmp", "/PRIVATE/TMP"));
        Assert.False(MacOsCompatibilitySymlink.IsExpectedPrivateTarget("/private/tmp", "/private/var"));
    }

    /// <summary>Expected private paths are built under the volume root.</summary>
    [Fact]
    public static void BuildExpectedPrivatePathBuildsCanonicalPath()
    {
        var root = Path.GetPathRoot(Path.GetTempPath())!;
        Assert.True(MacOsCompatibilitySymlink.TryBuildExpectedPrivatePath(root, "tmp", out var expected));
        Assert.Contains("private", expected, StringComparison.OrdinalIgnoreCase);
        Assert.EndsWith("tmp", expected, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>Apple-host follow of a volume-root candidate fails when the entry is not a resolvable link.</summary>
    [Fact]
    public static void FollowOnAppleHostFailsRootCandidateIsNotALink()
    {
        var root = Path.GetPathRoot(Path.GetTempPath())!;
        var candidate = Path.Join(root, "tmp");
        var info = new DirectoryInfo(candidate);

        // Darwin ships /tmp -> /private/tmp; that path cannot exercise the non-link failure branch.
        if (info.LinkTarget is not null)
        {
            Assert.True(MacOsCompatibilitySymlink.TryFollow(info, true, out var followed));
            Assert.False(string.IsNullOrEmpty(followed));
            return;
        }

        Assert.False(MacOsCompatibilitySymlink.TryFollow(info, true, out var resolved));
        Assert.Equal(string.Empty, resolved);
    }

    /// <summary>Apple-host flag still rejects ordinary nested directories.</summary>
    [Fact]
    public static void FollowReturnsFalseForNonRootChildOnAppleHost()
    {
        using var root = new TempDirectory("squirix-macos-follow-nested");
        Assert.False(MacOsCompatibilitySymlink.TryFollow(new DirectoryInfo(root.Path), true, out var resolved));
        Assert.Equal(string.Empty, resolved);
    }

    /// <summary>Non-Apple hosts always fail follow.</summary>
    [Fact]
    public static void FollowReturnsFalseWhenNotAppleHost()
    {
        using var root = new TempDirectory("squirix-macos-follow-off");
        Assert.False(MacOsCompatibilitySymlink.TryFollow(new DirectoryInfo(root.Path), false, out var resolved));
        Assert.Equal(string.Empty, resolved);
    }

    /// <summary>Root-link identity accepts volume-root children named var/tmp/etc.</summary>
    /// <param name="name">Allowlisted root child name.</param>
    [Theory]
    [InlineData("var")]
    [InlineData("tmp")]
    [InlineData("etc")]
    public static void GetRootLinkIdentityAcceptsVolumeRootChildren(string name)
    {
        var root = Path.GetPathRoot(Path.GetTempPath())!;
        var candidate = Path.Join(root, name);
        Assert.True(MacOsCompatibilitySymlink.TryGetRootLinkIdentity(new DirectoryInfo(candidate), out var pathRoot, out var resolvedName));
        Assert.Equal(root, pathRoot);
        Assert.Equal(name, resolvedName);
    }

    /// <summary>Root-link identity rejects nested allowlisted names.</summary>
    [Fact]
    public static void GetRootLinkIdentityRejectsNestedAllowlistedName()
    {
        using var root = new TempDirectory("squirix-macos-identity-nested");
        var nested = Path.Join(root.Path, "var");
        Assert.False(MacOsCompatibilitySymlink.TryGetRootLinkIdentity(new DirectoryInfo(nested), out _, out _));
    }

    /// <summary>Resolving a non-link directory returns false.</summary>
    [Fact]
    public static void ResolveFinalTargetReturnsFalseOrdinaryDirectory()
    {
        using var root = new TempDirectory("squirix-macos-resolve");
        Assert.False(MacOsCompatibilitySymlink.TryResolveFinalTargetPath(new DirectoryInfo(root.Path), out var target));
        Assert.Equal(string.Empty, target);
    }
}
