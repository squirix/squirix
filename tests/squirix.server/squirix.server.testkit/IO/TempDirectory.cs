using System;
using System.IO;
using System.Runtime.CompilerServices;
using Squirix.Server.Attributes;
using Squirix.Server.TestKit.Diagnostics;

namespace Squirix.Server.TestKit.IO;

/// <summary>RAII wrapper that creates a guarded temp directory and deletes it on dispose.</summary>
[Immutable]
public sealed class TempDirectory : IDisposable
{
    /// <summary>
    /// Initializes a new instance of the <see cref="TempDirectory" /> class.
    /// </summary>
    /// <param name="innerDirectory">Subfolder under the system temp path.</param>
    /// <param name="hint">Optional trace segment, usually the calling test name.</param>
    public TempDirectory(string innerDirectory, [CallerMemberName] string? hint = null)
    {
        Path = CreateTempDirectory(innerDirectory, hint);
    }

    /// <summary>Gets the absolute path to the created directory.</summary>
    public string Path { get; }

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
    public void Dispose()
    {
        var attemptsLeft = 5;
        while (true)
        {
            try
            {
                if (Directory.Exists(Path))
                    Directory.Delete(Path, true);

                return;
            }
            catch (IOException ex) when (--attemptsLeft > 0)
            {
                TestLog.Suppressed($"Transient delete failure on '{Path}'; retrying.", ex);
            }
            catch (UnauthorizedAccessException ex) when (--attemptsLeft > 0)
            {
                TestLog.Suppressed($"Transient access failure on '{Path}'; retrying.", ex);
            }
        }
    }

    private static string CreateTempDirectory(string innerDirectory, [CallerMemberName] string? hint = null)
    {
        var d = string.IsNullOrEmpty(hint) ? System.IO.Path.Join(System.IO.Path.GetTempPath(), innerDirectory, Guid.NewGuid().ToString("N"))
            : System.IO.Path.Join(System.IO.Path.GetTempPath(), innerDirectory, Guid.NewGuid().ToString("N"), hint);
        Directory.CreateDirectory(d);
        return d;
    }
}
