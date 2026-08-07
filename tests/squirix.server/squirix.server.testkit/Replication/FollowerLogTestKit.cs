using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Storage.Replication;

namespace Squirix.Server.TestKit.Replication;

/// <summary>Shared helpers for replica-group follower-log tests.</summary>
public static class FollowerLogTestKit
{
    /// <summary>Returns the length in bytes of the replica-group log file at <paramref name="logPath" />.</summary>
    /// <param name="logPath">The group log file path.</param>
    /// <returns>The log length in bytes.</returns>
    public static long GetLogLength(string logPath) => new FileInfo(logPath).Length;

    /// <summary>Flips every bit of the byte at <paramref name="offset" />, or of the last byte when out of range.</summary>
    /// <param name="path">The file to corrupt.</param>
    /// <param name="offset">The byte offset to flip; clamped to the last byte when the file is shorter.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>A task that completes when the corruption is written back.</returns>
    /// <exception cref="InvalidDataException">Thrown when the file is empty so no byte can be corrupted.</exception>
    public static async Task CorruptByteAsync(string path, int offset, CancellationToken cancellationToken)
    {
        var bytes = await File.ReadAllBytesAsync(path, cancellationToken).ConfigureAwait(false);
        if (bytes.Length is 0)
            throw new InvalidDataException($"Cannot corrupt an empty file at '{path}'.");

        var index = offset < bytes.Length ? offset : bytes.Length - 1;
        bytes[index] ^= 0xFF;
        await File.WriteAllBytesAsync(path, bytes, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>Flips every bit of the last byte of the file.</summary>
    /// <param name="path">The file to corrupt.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>A task that completes when the corruption is written back.</returns>
    /// <exception cref="InvalidDataException">Thrown when the file is empty so no byte can be corrupted.</exception>
    public static async Task CorruptTailAsync(string path, CancellationToken cancellationToken)
    {
        var bytes = await File.ReadAllBytesAsync(path, cancellationToken).ConfigureAwait(false);
        if (bytes.Length is 0)
            throw new InvalidDataException($"Cannot corrupt an empty file at '{path}'.");

        bytes[^1] ^= 0xFF;
        await File.WriteAllBytesAsync(path, bytes, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>Concatenates the UTF-8 payload of every entry in order.</summary>
    /// <param name="entries">The entries whose payloads are concatenated.</param>
    /// <returns>The concatenated payload string.</returns>
    internal static string Payload(IReadOnlyList<FollowerLogEntry> entries)
    {
        var result = new StringBuilder();
        for (var i = 0; i < entries.Count; i++)
            result.Append(Encoding.UTF8.GetString(entries[i].Payload.Span));

        return result.ToString();
    }
}
