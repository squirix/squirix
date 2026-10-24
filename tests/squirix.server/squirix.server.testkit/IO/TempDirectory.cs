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
    /// <summary>Initializes a new instance of the <see cref="TempDirectory" /> class.</summary>
    /// <param name="innerDirectory">Subfolder under the test temp root.</param>
    /// <param name="hint">Optional trace segment, usually the calling test name.</param>
    public TempDirectory(string innerDirectory, [CallerMemberName] string? hint = null)
    {
        (Root, Path) = CreateTempDirectory(innerDirectory, hint);
    }

    /// <summary>Gets the absolute path to the created directory.</summary>
    private string Path { get; }

    /// <summary>Gets the unique directory that holds <see cref="Path" />; it is the one deleted on dispose.</summary>
    private string Root { get; }

    /// <summary>Gets the directory path for <paramref name="dir" />.</summary>
    /// <param name="dir">The temp directory handle.</param>
    public static implicit operator string(TempDirectory dir)
    {
        ArgumentNullException.ThrowIfNull(dir);
        return dir.Path;
    }

    /// <inheritdoc />
    public override string ToString() => Path;

    /// <inheritdoc />
    public void Dispose()
    {
        // Antivirus scanners and lingering handles on Windows can hold a just-closed file for seconds; ~5 s of backoff in total.
        const int maxAttempts = 11;
        var attempt = 1;
        using var backoff = new ManualResetEventSlim(false, 0);
        while (true)
        {
            try
            {
                if (Directory.Exists(Root))
                    Directory.Delete(Root, true);

                return;
            }
            catch (Exception ex) when (attempt < maxAttempts && ex is IOException or UnauthorizedAccessException)
            {
                TestLog.Suppressed($"Transient delete failure on '{Root}' (attempt {attempt}); retrying.", ex);
                _ = backoff.Wait(TimeSpan.FromMilliseconds(100 * attempt));
                attempt++;
            }
        }
    }

    private static (string Root, string Path) CreateTempDirectory(string innerDirectory, [CallerMemberName] string? hint = null)
    {
        PathValidationKit.ValidateSegmentName(innerDirectory, nameof(innerDirectory));
        if (!string.IsNullOrEmpty(hint))
            PathValidationKit.ValidateSegmentName(hint, nameof(hint));

        var root = System.IO.Path.Join(System.IO.Path.GetTempPath(), NodePathKit.TestTempRootName, innerDirectory, Guid.NewGuid().ToString("N"));
        var d = string.IsNullOrEmpty(hint) ? root : System.IO.Path.Join(root, hint);
        _ = Directory.CreateDirectory(d);
        return (root, d);
    }
}
