using System;
using System.Runtime.CompilerServices;

namespace Squirix.TestKit.IO;

/// <summary>
/// RAII wrapper that creates a guarded temp directory and deletes it on dispose.
/// </summary>
public sealed class TempDirectory : IDisposable
{
    /// <summary>
    /// Initializes a new instance of the <see cref="TempDirectory" /> class.
    /// </summary>
    /// <param name="innerDirectory">Subfolder under the system temp path.</param>
    /// <param name="hint">Optional trace segment, usually the calling test name.</param>
    public TempDirectory(string innerDirectory, [CallerMemberName] string? hint = null)
    {
        Path = DirectoryKit.CreateTempDirectory(innerDirectory, hint);
    }

    /// <summary>
    /// Gets the absolute path to the created directory.
    /// </summary>
    private string Path { get; }

    /// <summary>
    /// Gets the directory path for <paramref name="directory" />.
    /// </summary>
    /// <param name="directory">The temp directory handle.</param>
    public static implicit operator string(TempDirectory directory)
    {
        ArgumentNullException.ThrowIfNull(directory);
        return directory.Path;
    }

    /// <summary>
    /// Creates a unique directory under <paramref name="baseDirectory" /> and deletes it on dispose.
    /// </summary>
    /// <param name="baseDirectory">Directory that constrains the created path.</param>
    /// <param name="innerDirectory">Subfolder under <paramref name="baseDirectory" />.</param>
    /// <param name="hint">Optional trace segment, usually the calling test name.</param>
    /// <returns>A disposable handle to the created directory.</returns>
    public static TempDirectory CreateUnder(string baseDirectory, string innerDirectory, [CallerMemberName] string? hint = null)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(baseDirectory);
        ArgumentException.ThrowIfNullOrWhiteSpace(innerDirectory);

        var path = PathKit.Combine(baseDirectory, innerDirectory, Guid.NewGuid().ToString("N"));
        if (!string.IsNullOrEmpty(hint))
            path = PathKit.Combine(path, hint);

        DirectoryKit.CreateDirectory(path, baseDirectory);
        return new TempDirectory(path);
    }

    /// <inheritdoc />
    public override string ToString() => Path;

    /// <inheritdoc />
    public void Dispose() => DirectoryKit.TryDeleteDirectory(Path);
}
