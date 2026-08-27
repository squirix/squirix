using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;

namespace Squirix.Server.Utils;

/// <summary>Safe directory creation with strict path validation and optional symlink rejection.</summary>
internal static class DirectoryEx
{
    private static ILogger Logger => LogManager.GetLogger("Squirix.Server.Utils.DirectoryEx");

    /// <summary>Safely creates a directory with strict validation and returns its normalized absolute path.</summary>
    /// <param name="path">
    /// The target directory path. May be relative or absolute. Must not be <see langword="null" />, empty, or whitespace,
    /// and must not contain invalid characters or wildcards.
    /// </param>
    /// <param name="baseDir">
    /// Optional base directory used to resolve a relative <paramref name="path" />. When provided,
    /// the resulting target must reside within this base directory (the method throws otherwise).
    /// If <paramref name="baseDir" /> does not exist, it is created.
    /// </param>
    /// <param name="forbidSymlinks">
    /// When <see langword="true" /> (default), forbids symbolic links/junctions both in the parent chain and at the
    /// target directory; the method throws if a link is detected. When <see langword="false" />, link checks are skipped.
    /// </param>
    /// <returns>The normalized absolute path of the created (or already existing) directory.</returns>
    /// <exception cref="ArgumentException">
    /// Thrown if <paramref name="path" /> (or <paramref name="baseDir" /> when provided) is empty or contains invalid characters.
    /// </exception>
    /// <exception cref="UnauthorizedAccessException">
    /// Thrown when the resolved target escapes <paramref name="baseDir" /> or the process lacks permissions to create/clean the directory.
    /// </exception>
    /// <exception cref="IOException">
    /// Thrown if a file exists at the target path, a forbidden symlink/junction is detected (when
    /// <paramref name="forbidSymlinks" /> is <see langword="true" />), or the target resolves to a link after creation.
    /// </exception>
    /// <remarks>
    ///     <para>
    ///     The method performs the following steps:
    ///     (1) validates inputs; (2) resolves an absolute path (combining with <paramref name="baseDir" /> or current working directory
    ///     for relative inputs); (3) ensures the target is within <paramref name="baseDir" /> if provided;
    ///     (4) validates path segments (e.g., on Windows: reserved names like <c language="csharp">CON</c>, <c language="csharp">PRN</c>, trailing dot/space);
    ///     (5) optionally checks for symlinks/junctions; (6) creates the directory when it does not exist.
    ///     </para>
    ///     <para>
    ///     This routine minimizes directory traversal and link attacks by rejecting targets that escape the base directory
    ///     and, by default, forbidding symlinks. Use the returned path immediately for subsequent operations.
    ///     </para>
    /// </remarks>
    internal static string CreateDirectory(string path, string? baseDir = null, bool forbidSymlinks = true)
    {
        var full = DirectoryPathValidator.ResolveValidatedDirectoryPath(path, baseDir, forbidSymlinks);
        return EnsureDirectoryReady(full, forbidSymlinks);
    }

    /// <summary>Safely creates a directory with strict validation and returns its normalized absolute path.</summary>
    /// <param name="path">The target directory path.</param>
    /// <param name="baseDir">Optional base directory used to resolve a relative <paramref name="path" />.</param>
    /// <param name="ensureEmpty">When <see langword="true" />, deletes existing contents of an already-present directory.</param>
    /// <param name="forbidSymlinks">When <see langword="true" />, forbids symbolic links/junctions in the path chain.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The normalized absolute path of the created (or already existing) directory.</returns>
    internal static Task<string> CreateDirectoryAsync(
        string path,
        string? baseDir = null,
        bool ensureEmpty = false,
        bool forbidSymlinks = true,
        CancellationToken cancellationToken = default) => EnsureDirectoryReadyAsync(
        DirectoryPathValidator.ResolveValidatedDirectoryPath(path, baseDir, forbidSymlinks),
        ensureEmpty,
        forbidSymlinks,
        cancellationToken);

    private static async Task CleanDirectoryContentsAsync(string dir, bool forbidSymlinks, CancellationToken cancellationToken)
    {
        // Delete contents (not the root). Retry a few times for Windows file locks.
        const int retries = 3;
        const int delayMs = 80;

        for (var attempt = 0; attempt < retries; attempt++)
        {
            try
            {
                var files = Directory.GetFiles(dir, "*", SearchOption.TopDirectoryOnly);
                for (var i = 0; i < files.Length; i++)
                {
                    var f = files[i];
                    ClearReadOnlyAttributes(f);
                    File.Delete(f);
                }

                var directories = Directory.GetDirectories(dir, "*", SearchOption.TopDirectoryOnly);
                for (var i = 0; i < directories.Length; i++)
                {
                    var d = directories[i];
                    var di = new DirectoryInfo(d);
                    if (forbidSymlinks && DirectorySymlinkGuard.IsSymlink(di))
                        throw new IOException("Refusing to descend into symlink/junction.");

                    var validated = FilePathValidator.ResolveValidatedDirectoryPath(d);
                    Directory.Delete(validated, true);
                }

                return;
            }
            catch (IOException) when (attempt < retries - 1)
            {
                await Task.Delay(TimeSpan.FromMilliseconds(delayMs), TimeProvider.System, cancellationToken).ConfigureAwait(false);
            }
            catch (UnauthorizedAccessException) when (attempt < retries - 1)
            {
                await Task.Delay(TimeSpan.FromMilliseconds(delayMs), TimeProvider.System, cancellationToken).ConfigureAwait(false);
            }
        }
    }

    private static void ClearReadOnlyAttributes(string file)
    {
        try
        {
            var attrs = File.GetAttributes(file);
            if ((attrs & FileAttributes.ReadOnly) != FileAttributes.None)
                File.SetAttributes(file, attrs & ~FileAttributes.ReadOnly);
        }
        catch (IOException ex)
        {
            // Best-effort cleanup: inability to clear read-only attributes must not block deletion attempts.
            LogManager.ReadOnlyAttributeClearFailed(Logger, ex, file);
        }
        catch (UnauthorizedAccessException ex)
        {
            // Best-effort cleanup: inability to clear read-only attributes must not block deletion attempts.
            LogManager.ReadOnlyAttributeClearFailed(Logger, ex, file);
        }
    }

    private static void EnsureDirectoryExistsAndIsRegular(string full, bool forbidSymlinks)
    {
        if (!Directory.Exists(full))
        {
            _ = Directory.CreateDirectory(full);
            DirectorySymlinkGuard.EnsureRegularDirectory(full, true, forbidSymlinks);
            return;
        }

        DirectorySymlinkGuard.EnsureRegularDirectory(full, false, forbidSymlinks);
    }

    private static string EnsureDirectoryReady(string full, bool forbidSymlinks)
    {
        EnsureDirectoryExistsAndIsRegular(full, forbidSymlinks);
        return full;
    }

    private static async Task<string> EnsureDirectoryReadyAsync(string full, bool ensureEmpty, bool forbidSymlinks, CancellationToken cancellationToken)
    {
        EnsureDirectoryExistsAndIsRegular(full, forbidSymlinks);

        if (!ensureEmpty)
            return full;

        var root = Path.GetPathRoot(full) ?? string.Empty;
        if (string.Equals(full.TrimEnd(Path.DirectorySeparatorChar), root.TrimEnd(Path.DirectorySeparatorChar), StringComparison.OrdinalIgnoreCase))
            throw new IOException("Refusing to clean a filesystem root.");

        await CleanDirectoryContentsAsync(full, forbidSymlinks, cancellationToken).ConfigureAwait(false);
        return full;
    }
}
