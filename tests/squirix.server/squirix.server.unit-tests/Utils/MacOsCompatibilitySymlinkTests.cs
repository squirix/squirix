using System;
using System.IO;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Squirix.Server.Attributes;
using Squirix.Server.TestKit.IO;
using Squirix.Server.UnitTests.Support;
using Squirix.Server.Utils;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Utils;

/// <summary>Covers Darwin compatibility symlink helpers without requiring a macOS host.</summary>
[UsedImplicitly]
[Immutable]
public sealed class MacOsCompatibilitySymlinkTests : ServerUnitTestBase
{
    /// <summary>Resolving a non-link directory returns false.</summary>
    [Test]
    public async Task FinalTargetFalseForOrdinaryDir()
    {
        using var root = new TempDirectory("squirix-macos-resolve");
        _ = await Assert.That(MacOsCompatibilitySymlink.TryResolveFinalTargetPath(new DirectoryInfo(root.Path), out var target)).IsFalse();
        _ = await Assert.That(target).IsEqualTo(string.Empty);
    }

    /// <summary>Apple-host follow of a volume-root candidate fails when the entry is not a resolvable link.</summary>
    [Test]
    public async Task FollowFailsWhenRootNotALink()
    {
        var root = Path.GetPathRoot(Path.GetTempPath());
        var candidate = Path.Join(root, "tmp");
        var info = new DirectoryInfo(candidate);

        // Darwin ships /tmp -> /private/tmp; that path cannot exercise the non-link failure branch.
        if (info.LinkTarget != null)
        {
            _ = await Assert.That(MacOsCompatibilitySymlink.TryFollow(info, true, out var followed)).IsTrue();
            _ = await Assert.That(string.IsNullOrEmpty(followed)).IsFalse();
            return;
        }

        _ = await Assert.That(MacOsCompatibilitySymlink.TryFollow(info, true, out var resolved)).IsFalse();
        _ = await Assert.That(resolved).IsEqualTo(string.Empty);
    }

    /// <summary>Apple-host flag still rejects ordinary nested directories.</summary>
    [Test]
    public async Task FollowFalseForNonRootChild()
    {
        using var root = new TempDirectory("squirix-macos-follow-nested");
        _ = await Assert.That(MacOsCompatibilitySymlink.TryFollow(new DirectoryInfo(root.Path), true, out var resolved)).IsFalse();
        _ = await Assert.That(resolved).IsEqualTo(string.Empty);
    }

    /// <summary>Non-Apple hosts always fail to follow.</summary>
    [Test]
    public async Task FollowReturnsFalseWhenNotAppleHost()
    {
        using var root = new TempDirectory("squirix-macos-follow-off");
        _ = await Assert.That(MacOsCompatibilitySymlink.TryFollow(new DirectoryInfo(root.Path), false, out var resolved)).IsFalse();
        _ = await Assert.That(resolved).IsEqualTo(string.Empty);
    }

    /// <summary>Allowlisted root link names are recognized.</summary>
    /// <param name="name">Candidate name.</param>
    /// <param name="expected">Expected the allowlist result.</param>
    [Test]
    [Arguments("var", true)]
    [Arguments("tmp", true)]
    [Arguments("etc", true)]
    [Arguments("usr", false)]
    [Arguments("VAR", false)]
    public async Task IsAllowlistedRootLinkNameMatches(string name, bool expected) =>
        _ = await Assert.That(MacOsCompatibilitySymlink.IsAllowlistedRootLinkName(name)).IsEqualTo(expected);

    /// <summary>Private-target comparison is case-insensitive.</summary>
    [Test]
    public async Task IsExpectedPrivateTargetIgnoresCase()
    {
        _ = await Assert.That(MacOsCompatibilitySymlink.IsExpectedPrivateTarget("/private/tmp", "/PRIVATE/TMP")).IsTrue();
        _ = await Assert.That(MacOsCompatibilitySymlink.IsExpectedPrivateTarget("/private/tmp", "/private/var")).IsFalse();
    }

    /// <summary>Expected private paths are built under the volume root.</summary>
    [Test]
    public async Task PrivatePathIsCanonical()
    {
        var root = Path.GetPathRoot(Path.GetTempPath())!;
        _ = await Assert.That(MacOsCompatibilitySymlink.TryBuildExpectedPrivatePath(root, "tmp", out var expected)).IsTrue();
        _ = await Assert.That(expected).Contains("private", StringComparison.OrdinalIgnoreCase);
        _ = await Assert.That(expected).EndsWith("tmp", StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>Root-link identity accepts volume-root children named var/tmp/etc.</summary>
    /// <param name="name">Allowlisted root child name.</param>
    [Test]
    [Arguments("var")]
    [Arguments("tmp")]
    [Arguments("etc")]
    public async Task RootLinkIdentityAcceptsChildren(string name)
    {
        var root = Path.GetPathRoot(Path.GetTempPath());
        var candidate = Path.Join(root, name);
        _ = await Assert.That(MacOsCompatibilitySymlink.TryGetRootLinkIdentity(new DirectoryInfo(candidate), out var pathRoot, out var resolvedName)).IsTrue();
        _ = await Assert.That(pathRoot).IsEqualTo(root);
        _ = await Assert.That(resolvedName).IsEqualTo(name);
    }

    /// <summary>Root-link identity rejects nested allowlisted names.</summary>
    [Test]
    public async Task RootLinkIdentityRejectsNestedName()
    {
        using var root = new TempDirectory("squirix-macos-identity-nested");
        var nested = Path.Join(root.Path, "var");
        _ = await Assert.That(MacOsCompatibilitySymlink.TryGetRootLinkIdentity(new DirectoryInfo(nested), out _, out _)).IsFalse();
    }
}
