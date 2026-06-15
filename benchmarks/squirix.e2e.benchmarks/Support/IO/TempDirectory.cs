using System;
using System.Runtime.CompilerServices;

namespace Squirix.E2EBenchmarks.Support.IO;

/// <summary>
/// RAII wrapper that creates a guarded temp directory and deletes it on dispose.
/// </summary>
internal sealed class TempDirectory : IDisposable
{
    /// <summary>
    /// Initializes a new instance of the <see cref="TempDirectory" /> class.
    /// </summary>
    /// <param name="innerDirectory">Subfolder under the system temp path.</param>
    /// <param name="hint">Optional trace segment, usually the calling benchmark name.</param>
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

    /// <inheritdoc />
    public override string ToString() => Path;

    /// <inheritdoc />
    public void Dispose() => DirectoryKit.TryDeleteDirectory(Path);
}
