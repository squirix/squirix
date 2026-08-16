using System;
using System.IO;

namespace Squirix.Server.Utils;

/// <summary>
/// Allows Darwin root-level compatibility symlinks (<c>/var</c>, <c>/tmp</c>, <c>/etc</c> → <c>/private/...</c>)
/// while every other symlink in a DataDir path chain remains forbidden.
/// </summary>
internal static class MacOsCompatibilitySymlink
{
    /// <summary>Returns whether <paramref name="name" /> is a Darwin root compatibility link name.</summary>
    /// <param name="name">Directory name.</param>
    /// <returns><see langword="true" /> for var, tmp, or etc.</returns>
    internal static bool IsAllowlistedRootLinkName(string name) => string.Equals(name, "var", StringComparison.Ordinal) || string.Equals(name, "tmp", StringComparison.Ordinal) ||
                                                                   string.Equals(name, "etc", StringComparison.Ordinal);

    /// <summary>Returns whether <paramref name="targetFull" /> equals the expected private path.</summary>
    /// <param name="targetFull">Resolved symlink target.</param>
    /// <param name="expectedFull">Expected <c>{root}private/{name}</c> path.</param>
    /// <returns><see langword="true" /> when the paths match under ordinal-ignore-case comparison.</returns>
    internal static bool IsExpectedPrivateTarget(string targetFull, string expectedFull) => targetFull.Equals(expectedFull, StringComparison.OrdinalIgnoreCase);

    /// <summary>Builds the expected absolute path <c>{root}private/{name}</c> with trailing separators removed.</summary>
    /// <param name="root">Volume root path.</param>
    /// <param name="name">Allowlisted link name (<c>var</c>, <c>tmp</c>, or <c>etc</c>).</param>
    /// <param name="expectedFull">Trimmed expected path when this method returns <see langword="true" />.</param>
    /// <returns><see langword="true" /> when a non-empty expected path was produced.</returns>
    internal static bool TryBuildExpectedPrivatePath(string root, string name, out string expectedFull)
    {
        expectedFull = DirectoryPathHelpers.TrimTrailingSeparators(Path.Join(root, "private", name));
        return expectedFull.Length > 0;
    }

    /// <summary>
    /// Attempts to follow a Darwin root compatibility symlink to its canonical <c>/private/...</c> path.
    /// </summary>
    /// <param name="directory">Directory entry already known to be a symlink.</param>
    /// <param name="resolvedFullPath">
    /// Canonical absolute path under <c>/private</c> when this method returns <see langword="true" />;
    /// otherwise <see cref="string.Empty" />.
    /// </param>
    /// <returns>
    /// <see langword="true" /> only on macOS/Mac Catalyst when <paramref name="directory" /> is an allowlisted
    /// root link that resolves to <c>{root}private/{name}</c>.
    /// </returns>
    internal static bool TryFollow(DirectoryInfo directory, out string resolvedFullPath) => TryFollow(
        directory,
        OperatingSystem.IsMacOS() || OperatingSystem.IsMacCatalyst(),
        out resolvedFullPath);

    /// <summary>Attempts to follow a Darwin root compatibility symlink, with an explicit host-platform flag for tests.</summary>
    /// <param name="directory">Directory entry already known to be a symlink.</param>
    /// <param name="isAppleHost">When <see langword="false" />, always returns <see langword="false" />.</param>
    /// <param name="resolvedFullPath">Canonical absolute path under <c>/private</c> on success.</param>
    /// <returns><see langword="true" /> when the link is an allowlisted Darwin root compatibility symlink.</returns>
    internal static bool TryFollow(DirectoryInfo directory, bool isAppleHost, out string resolvedFullPath)
    {
        resolvedFullPath = string.Empty;

        // Non-Apple hosts never expose these OS compatibility links.
        if (!isAppleHost)
            return false;

        if (!TryGetRootLinkIdentity(directory, out var root, out var name))
            return false;

        if (!TryResolveFinalTargetPath(directory, out var targetFull))
            return false;

        if (!TryBuildExpectedPrivatePath(root, name, out var expectedFull))
            return false;

        // Reject anything that does not land on the well-known /private/{name} mapping.
        if (!IsExpectedPrivateTarget(targetFull, expectedFull))
            return false;

        resolvedFullPath = expectedFull;
        return true;
    }

    /// <summary>Returns whether <paramref name="directory" /> is a root child named <c>var</c>, <c>tmp</c>, or <c>etc</c>.</summary>
    /// <param name="directory">Directory to inspect.</param>
    /// <param name="root">Volume root when the check succeeds; otherwise <see cref="string.Empty" />.</param>
    /// <param name="name">Directory name when the check succeeds; otherwise <see cref="string.Empty" />.</param>
    /// <returns><see langword="true" /> when the entry is a Darwin root compatibility link candidate.</returns>
    internal static bool TryGetRootLinkIdentity(DirectoryInfo directory, out string root, out string name)
    {
        root = string.Empty;
        name = string.Empty;

        var pathRoot = Path.GetPathRoot(directory.FullName);
        var parent = directory.Parent;
        if (pathRoot is null || parent is null)
            return false;

        // Compare trimmed and raw root forms: Path.GetPathRoot may keep a trailing separator.
        var rootTrimmed = DirectoryPathHelpers.TrimTrailingSeparators(pathRoot);
        var parentTrimmed = DirectoryPathHelpers.TrimTrailingSeparators(parent.FullName);
        if (!parentTrimmed.Equals(rootTrimmed, StringComparison.Ordinal) && !parent.FullName.Equals(pathRoot, StringComparison.Ordinal))
            return false;

        // Only the three historical Darwin compatibility names are allowlisted.
        if (!IsAllowlistedRootLinkName(directory.Name))
            return false;

        root = pathRoot;
        name = directory.Name;
        return true;
    }

    /// <summary>Resolves the final symlink target path (full chain) with trailing separators removed.</summary>
    /// <param name="directory">Symlink directory entry.</param>
    /// <param name="targetFull">Trimmed absolute target path when resolution succeeds; otherwise <see cref="string.Empty" />.</param>
    /// <returns><see langword="true" /> when the link target was resolved.</returns>
    internal static bool TryResolveFinalTargetPath(DirectoryInfo directory, out string targetFull)
    {
        targetFull = string.Empty;
        FileSystemInfo? target;
        try
        {
            // returnFinalTarget: true — multi-hop links cannot bypass the /private/{name} check.
            target = directory.ResolveLinkTarget(true);
        }
        catch (IOException)
        {
            return false;
        }
        catch (UnauthorizedAccessException)
        {
            return false;
        }
        catch (NotSupportedException)
        {
            return false;
        }

        if (target is null)
            return false;

        targetFull = DirectoryPathHelpers.TrimTrailingSeparators(target.FullName);
        return true;
    }
}
