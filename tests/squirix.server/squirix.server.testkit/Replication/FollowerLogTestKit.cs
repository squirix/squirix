using System;
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
    /// <summary>Flips every bit of the byte at <paramref name="offset" />, or of the last byte when past the end.</summary>
    /// <param name="path">The file to corrupt.</param>
    /// <param name="offset">The byte offset to flip; clamped to the last byte when the file is shorter.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>A task that completes when the corruption is written back.</returns>
    /// <exception cref="InvalidDataException">Thrown when the file is empty so no byte can be corrupted.</exception>
    /// <exception cref="ArgumentOutOfRangeException">Thrown when the offset is negative for a nonempty file.</exception>
    public static Task CorruptByteAsync(string path, int offset, CancellationToken cancellationToken) =>
        CorruptAtAsync(path, offset, cancellationToken);

    /// <summary>Flips every bit of the last byte of the file.</summary>
    /// <param name="path">The file to corrupt.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>A task that completes when the corruption is written back.</returns>
    /// <exception cref="InvalidDataException">Thrown when the file is empty so no byte can be corrupted.</exception>
    public static Task CorruptTailAsync(string path, CancellationToken cancellationToken) =>
        CorruptAtAsync(path, long.MaxValue, cancellationToken);

    /// <summary>Returns the length in bytes of the replica-group log file at <paramref name="logPath" />.</summary>
    /// <param name="logPath">The group log file path.</param>
    /// <returns>The log length in bytes.</returns>
    public static long GetLogLength(string logPath) => new FileInfo(logPath).Length;

    /// <summary>Makes the commit index the leader announced durable, as the apply loop of a follower does before it applies.</summary>
    /// <param name="log">The follower log.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>A task that completes once the durable commit index covers the announced one.</returns>
    internal static async Task MakeCommitDurableAsync(IFollowerLog log, CancellationToken cancellationToken)
    {
        var status = await log.GetStatusAsync(cancellationToken).ConfigureAwait(false);
        if (status.CommitHint > status.CommitIndex)
            _ = await log.AdvanceCommitAsync(status.CommitHint, cancellationToken).ConfigureAwait(false);
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

    private static async Task CorruptAtAsync(string path, long offset, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        using var handle = File.OpenHandle(path, FileMode.Open, FileAccess.ReadWrite, FileShare.Read, FileOptions.Asynchronous);
        var length = RandomAccess.GetLength(handle);
        if (length == 0)
            throw new InvalidDataException($"Cannot corrupt an empty file at '{path}'.");

        ArgumentOutOfRangeException.ThrowIfNegative(offset);
        var index = Math.Min(offset, length - 1);
        byte[] bytes = [0];
        if (await RandomAccess.ReadAsync(handle, bytes, index, cancellationToken).ConfigureAwait(false) != 1)
            throw new EndOfStreamException();

        bytes[0] ^= 0xFF;
        await RandomAccess.WriteAsync(handle, bytes, index, cancellationToken).ConfigureAwait(false);
    }
}
