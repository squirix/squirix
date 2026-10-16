using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Storage.Replication;

namespace Squirix.Server.TestKit.Replication;

/// <summary>Locates and hand-writes the generation files of a replica group log, so tests do not depend on their byte layout.</summary>
internal static class GroupLogFile
{
    /// <summary>Resolves the generation file the log selects as active.</summary>
    /// <param name="persistenceRoot">The persistence data directory.</param>
    /// <param name="groupId">The replica group identifier.</param>
    /// <returns>The path of the active generation file; slot 0 when no file holds a valid header.</returns>
    internal static string ActivePath(string persistenceRoot, string groupId)
    {
        Span<byte> first = stackalloc byte[GroupLogSlots.HeaderSize];
        Span<byte> second = stackalloc byte[GroupLogSlots.HeaderSize];
        var firstPath = GroupStoragePaths.GetLogSlotPath(persistenceRoot, groupId, 0);
        var secondPath = GroupStoragePaths.GetLogSlotPath(persistenceRoot, groupId, 1);
        var firstLength = ReadHead(firstPath, first, out var firstRead);
        var secondLength = ReadHead(secondPath, second, out var secondRead);
        var (slot, _) = GroupLogSlots.Select(first[..firstRead], firstLength, second[..secondRead], secondLength, firstPath);
        return slot == 0 ? firstPath : secondPath;
    }

    /// <summary>Reads the raw bytes of one generation file, for comparing a file before and after an operation.</summary>
    /// <param name="persistenceRoot">The persistence data directory.</param>
    /// <param name="groupId">The replica group identifier.</param>
    /// <param name="slot">The slot index, 0 or 1.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The bytes of the file, or <see langword="null" /> when it does not exist.</returns>
    internal static async Task<byte[]?> ReadSlotBytesAsync(string persistenceRoot, string groupId, int slot, CancellationToken cancellationToken)
    {
        var path = GroupStoragePaths.GetLogSlotPath(persistenceRoot, groupId, slot);
        return File.Exists(path) ? await File.ReadAllBytesAsync(path, cancellationToken).ConfigureAwait(false) : null;
    }

    /// <summary>Writes the sealed header of a generation at the start of a file, leaving the rest of the file as it is.</summary>
    /// <param name="path">The generation file path.</param>
    /// <param name="generation">The generation to seal.</param>
    internal static void WriteHeader(string path, ulong generation)
    {
        Span<byte> header = stackalloc byte[GroupLogSlots.HeaderSize];
        GroupLogSlots.WriteHeader(header, generation);
        using var handle = File.OpenHandle(path, FileMode.OpenOrCreate, FileAccess.Write, FileShare.ReadWrite | FileShare.Delete);
        RandomAccess.Write(handle, header, 0);
    }

    private static long ReadHead(string path, Span<byte> head, out int read)
    {
        read = 0;
        if (!File.Exists(path))
            return -1;

        using var handle = File.OpenHandle(path, FileMode.Open, FileAccess.Read, FileShare.ReadWrite | FileShare.Delete);
        read = RandomAccess.Read(handle, head, 0);
        return RandomAccess.GetLength(handle);
    }
}
