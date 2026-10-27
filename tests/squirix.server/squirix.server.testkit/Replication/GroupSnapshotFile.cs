using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Storage.Replication;

namespace Squirix.Server.TestKit.Replication;

/// <summary>Reads and hand-writes the slot files of a replica group snapshot, so tests do not depend on their byte layout.</summary>
internal static class GroupSnapshotFile
{
    /// <summary>Tells whether a snapshot slot file exists for a group.</summary>
    /// <param name="persistenceRoot">The persistence data directory.</param>
    /// <param name="groupId">The replica group identifier.</param>
    /// <returns><see langword="true" /> when either slot file exists.</returns>
    internal static bool Exists(string persistenceRoot, string groupId) =>
        File.Exists(GroupStoragePaths.GetSnapshotSlotPath(persistenceRoot, groupId, 0)) || File.Exists(GroupStoragePaths.GetSnapshotSlotPath(persistenceRoot, groupId, 1));

    /// <summary>Reads the published snapshot image of the slot the store selects, without the slot frame.</summary>
    /// <param name="persistenceRoot">The persistence data directory.</param>
    /// <param name="groupId">The replica group identifier.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The image, as the state transfer carries it.</returns>
    /// <exception cref="InvalidDataException">No slot holds a valid snapshot.</exception>
    internal static async Task<byte[]> ReadImageAsync(string persistenceRoot, string groupId, CancellationToken cancellationToken)
    {
        var first = await ReadBytesAsync(GroupStoragePaths.GetSnapshotSlotPath(persistenceRoot, groupId, 0), cancellationToken).ConfigureAwait(false);
        var second = await ReadBytesAsync(GroupStoragePaths.GetSnapshotSlotPath(persistenceRoot, groupId, 1), cancellationToken).ConfigureAwait(false);
        var selection = GroupSnapshotSlots.Select(
            first ?? [],
            first != null,
            second ?? [],
            second != null,
            new GroupSnapshotSlotContext(
                groupId,
                GroupSnapshotStore.DefaultMaxSnapshotBytes,
                GroupStoragePaths.GetSnapshotSlotPath(persistenceRoot, groupId, 0),
                GroupStoragePaths.GetSnapshotSlotPath(persistenceRoot, groupId, 1)));
        if (selection.Snapshot == null)
            throw new InvalidDataException($"The snapshot slots of group '{groupId}' hold no valid snapshot.");

        var slot = (selection.Slot == 0 ? first : second)!;
        var image = new byte[slot.Length - GroupSnapshotSlots.Overhead];
        slot.AsSpan(GroupSnapshotSlots.HeaderSize, image.Length).CopyTo(image);
        return image;
    }

    /// <summary>Reads the raw bytes of one slot file, for comparing a slot before and after an operation.</summary>
    /// <param name="persistenceRoot">The persistence data directory.</param>
    /// <param name="groupId">The replica group identifier.</param>
    /// <param name="slot">The slot index, 0 or 1.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The bytes of the slot file, or <see langword="null" /> when it does not exist.</returns>
    internal static Task<byte[]?> ReadSlotBytesAsync(string persistenceRoot, string groupId, int slot, CancellationToken cancellationToken) =>
        ReadBytesAsync(GroupStoragePaths.GetSnapshotSlotPath(persistenceRoot, groupId, slot), cancellationToken);

    /// <summary>Writes a snapshot image into freshly sealed slot files, replacing whatever the slots held.</summary>
    /// <param name="persistenceRoot">The persistence data directory.</param>
    /// <param name="groupId">The replica group identifier.</param>
    /// <param name="image">The published snapshot image, which may be damaged on purpose.</param>
    /// <param name="mirrored">Whether the image is written into both slots; otherwise only into slot 0.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>A task that completes when the files are written.</returns>
    internal static async Task WriteImageAsync(string persistenceRoot, string groupId, ReadOnlyMemory<byte> image, bool mirrored, CancellationToken cancellationToken)
    {
        _ = Directory.CreateDirectory(GroupStoragePaths.GetGroupDirectory(persistenceRoot, groupId));
        var slot = new byte[image.Length + GroupSnapshotSlots.Overhead];
        image.Span.CopyTo(slot.AsSpan(GroupSnapshotSlots.HeaderSize));
        GroupSnapshotSlots.WriteHeader(slot, 1UL);
        GroupSnapshotSlots.Seal(slot, 1UL);
        await File.WriteAllBytesAsync(GroupStoragePaths.GetSnapshotSlotPath(persistenceRoot, groupId, 0), slot, cancellationToken).ConfigureAwait(false);
        if (!mirrored)
            return;

        GroupSnapshotSlots.Seal(slot, 2UL);
        await File.WriteAllBytesAsync(GroupStoragePaths.GetSnapshotSlotPath(persistenceRoot, groupId, 1), slot, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>Cuts a slot file short, as a torn write does.</summary>
    /// <param name="persistenceRoot">The persistence data directory.</param>
    /// <param name="groupId">The replica group identifier.</param>
    /// <param name="slot">The slot index, 0 or 1.</param>
    /// <param name="length">The length to keep in bytes.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>A task that completes when the file is cut.</returns>
    internal static async Task TearAsync(string persistenceRoot, string groupId, int slot, int length, CancellationToken cancellationToken)
    {
        var path = GroupStoragePaths.GetSnapshotSlotPath(persistenceRoot, groupId, slot);
        var bytes = await File.ReadAllBytesAsync(path, cancellationToken).ConfigureAwait(false);
        await File.WriteAllBytesAsync(path, bytes.AsMemory(0, Math.Min(length, bytes.Length)), cancellationToken).ConfigureAwait(false);
    }

    /// <summary>Copies the slot files of one group to the directory of another, as a restored backup would.</summary>
    /// <param name="sourceRoot">The persistence data directory to copy from.</param>
    /// <param name="sourceGroupId">The replica group identifier to copy from.</param>
    /// <param name="targetRoot">The persistence data directory to copy to.</param>
    /// <param name="targetGroupId">The replica group identifier to copy to.</param>
    internal static void Copy(string sourceRoot, string sourceGroupId, string targetRoot, string targetGroupId)
    {
        _ = Directory.CreateDirectory(GroupStoragePaths.GetGroupDirectory(targetRoot, targetGroupId));
        for (var slot = 0; slot < GroupSnapshotSlots.SlotCount; slot++)
        {
            var source = GroupStoragePaths.GetSnapshotSlotPath(sourceRoot, sourceGroupId, slot);
            if (File.Exists(source))
                File.Copy(source, GroupStoragePaths.GetSnapshotSlotPath(targetRoot, targetGroupId, slot), true);
        }
    }

    private static async Task<byte[]?> ReadBytesAsync(string path, CancellationToken cancellationToken)
    {
        if (!File.Exists(path))
            return null;

        using var handle = File.OpenHandle(path, FileMode.Open, FileAccess.Read, FileShare.ReadWrite | FileShare.Delete, FileOptions.Asynchronous);
        var bytes = new byte[RandomAccess.GetLength(handle)];
        var read = 0;
        while (read < bytes.Length)
        {
            var count = await RandomAccess.ReadAsync(handle, bytes.AsMemory(read), read, cancellationToken).ConfigureAwait(false);
            if (count == 0)
                throw new EndOfStreamException($"The snapshot slot file '{path}' ended after {read} bytes.");

            read += count;
        }

        return bytes;
    }
}
