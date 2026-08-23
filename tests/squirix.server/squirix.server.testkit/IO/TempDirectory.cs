using System;
using System.IO;
using System.Runtime.CompilerServices;
using System.Threading;
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
        const int maxAttempts = 5;
        var attempt = 1;
        while (true)
        {
            try
            {
                if (Directory.Exists(Path))
                    Directory.Delete(Path, true);

                return;
            }
            catch (Exception ex) when (attempt < maxAttempts && ex is IOException or UnauthorizedAccessException)
            {
                TestLog.Suppressed($"Transient delete failure on '{Path}' (attempt {attempt}); retrying.", ex);
                Thread.Sleep(20 * attempt);
                attempt++;
            }
        }
    }

    private static string CreateTempDirectory(string innerDirectory, [CallerMemberName] string? hint = null)
    {
        PathValidationKit.ValidateSegmentName(innerDirectory, nameof(innerDirectory));
        if (!string.IsNullOrEmpty(hint))
            PathValidationKit.ValidateSegmentName(hint, nameof(hint));

        var d = string.IsNullOrEmpty(hint) ? System.IO.Path.Join(System.IO.Path.GetTempPath(), innerDirectory, Guid.NewGuid().ToString("N"))
            : System.IO.Path.Join(System.IO.Path.GetTempPath(), innerDirectory, Guid.NewGuid().ToString("N"), hint);
        Directory.CreateDirectory(d);
        return d;
    }
}
