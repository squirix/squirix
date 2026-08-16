using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Attributes;

namespace Squirix.Server.TestKit.IO;

/// <summary>RAII wrapper that writes a temp settings JSON file and deletes it on dispose.</summary>
[Immutable]
public sealed class TempSettingsFile : IDisposable
{
    private TempSettingsFile(string path)
    {
        Path = path;
    }

    /// <summary>Gets the absolute path to the temp settings file.</summary>
    public string Path { get; }

    /// <summary>Gets the file path for <paramref name="file" />.</summary>
    /// <param name="file">The temp settings file handle.</param>
    public static implicit operator string(TempSettingsFile file)
    {
        ArgumentNullException.ThrowIfNull(file);
        return file.Path;
    }

    /// <summary>Writes JSON to a unique temp settings file under the system temp directory.</summary>
    /// <param name="prefix">Filename prefix (for example <c>squirix-mp-</c>).</param>
    /// <param name="json">Settings JSON payload.</param>
    /// <param name="cancellationToken">Cancellation token for the write.</param>
    /// <returns>A disposable handle to the temp file path.</returns>
    public static Task<TempSettingsFile> WriteAsync(string prefix, string json, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(prefix);
        ArgumentNullException.ThrowIfNull(json);
        return WriteCoreAsync(prefix, json, cancellationToken);
    }

    /// <inheritdoc />
    public override string ToString() => Path;

    /// <inheritdoc />
    public void Dispose()
    {
        try
        {
            File.Delete(Path);
        }
        catch (IOException)
        {
            // Best-effort cleanup for temp test files.
        }
    }

    private static async Task<TempSettingsFile> WriteCoreAsync(string prefix, string json, CancellationToken cancellationToken)
    {
        var path = System.IO.Path.Join(System.IO.Path.GetTempPath(), NodeInvariantIndexStrings.FormatPrefixedMiddleSuffix(prefix, System.IO.Path.GetRandomFileName(), ".json"));
        await File.WriteAllTextAsync(path, json, cancellationToken).ConfigureAwait(false);
        return new TempSettingsFile(path);
    }
}
