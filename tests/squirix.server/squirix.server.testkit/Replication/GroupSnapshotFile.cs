using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Storage.Replication;
using Squirix.Server.Utils;

namespace Squirix.Server.TestKit.Replication;

/// <summary>Reads and hand-writes the slot files of a replica group snapshot, so tests do not depend on their byte layout.</summary>
internal static class GroupSnapshotFile
{
    /// <summary>Tells whether a snapshot slot file exists for a group.</summary>
    /// <param name="persistenceRoot">The persistence data directory.</param>
    /// <param name="groupId">The replica group identifier.</param>
    /// <returns><see langword="true" /> when either slot file exists.</returns>
    internal static bool Exists(string persistenceRoot, string groupId) =>
        File.Exists(PointerPath(persistenceRoot, groupId));

    /// <summary>Reads the published snapshot image of the slot the store selects, without the slot frame.</summary>
    /// <param name="persistenceRoot">The persistence data directory.</param>
    /// <param name="groupId">The replica group identifier.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The image, as the state transfer carries it.</returns>
    /// <exception cref="InvalidDataException">No slot holds a valid snapshot.</exception>
    internal static async Task<byte[]> ReadImageAsync(string persistenceRoot, string groupId, CancellationToken cancellationToken)
    {
        var snapshot = await new GroupSnapshotStore(persistenceRoot, groupId).ReadPublishedAsync(cancellationToken).ConfigureAwait(false)
            ?? ThrowHelper.Throw<GroupSnapshot>(new InvalidDataException("No valid snapshot generation remains."));
        var image = new byte[GroupSnapshotStore.ComputePublishedLength(in snapshot, GroupSnapshotStore.DefaultMaxSnapshotBytes)];
        GroupSnapshotStore.EncodePublished(image, in snapshot);
        return image;
    }

    /// <summary>Reads the raw bytes of one slot file, for comparing a slot before and after an operation.</summary>
    /// <param name="persistenceRoot">The persistence data directory.</param>
    /// <param name="groupId">The replica group identifier.</param>
    /// <param name="slot">The slot index, 0 or 1.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The bytes of the slot file, or <see langword="null" /> when it does not exist.</returns>
    internal static Task<byte[]?> ReadSlotBytesAsync(string persistenceRoot, string groupId, int slot, CancellationToken cancellationToken) =>
        ReadBytesAsync(SlotPath(persistenceRoot, groupId, slot), cancellationToken);

    /// <summary>Writes a snapshot image into freshly sealed slot files, replacing whatever the slots held.</summary>
    /// <param name="persistenceRoot">The persistence data directory.</param>
    /// <param name="groupId">The replica group identifier.</param>
    /// <param name="image">The published snapshot image, which may be damaged on purpose.</param>
    /// <param name="mirrored">Whether the image is written into both slots; otherwise only into slot 0.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>A task that completes when the files are written.</returns>
    internal static async Task WriteImageAsync(string persistenceRoot, string groupId, ReadOnlyMemory<byte> image, bool mirrored, CancellationToken cancellationToken)
    {
        var directory = GroupStoragePaths.GetGroupDirectory(persistenceRoot, groupId);
        _ = Directory.CreateDirectory(directory);
        var count = mirrored ? 2 : 1;
        for (var slot = 0; slot < count; slot++)
        {
            var generation = Guid.NewGuid();
            await File.WriteAllBytesAsync(Path.Join(directory, $"group.checkpoint-{generation:N}.snapshot"), image.ToArray(), cancellationToken).ConfigureAwait(false);
            var bytes = new byte[GroupSnapshotPointers.SlotSize];
            GroupSnapshotPointers.Encode(bytes, ulong.CreateChecked(slot + 1), generation);
            using var handle = File.OpenHandle(PointerPath(persistenceRoot, groupId), FileMode.OpenOrCreate, FileAccess.Write, FileShare.Read);
            await RandomAccess.WriteAsync(handle, bytes, slot * GroupSnapshotPointers.SlotSize, cancellationToken).ConfigureAwait(false);
            RandomAccess.FlushToDisk(handle);
        }
    }

    /// <summary>Cuts a slot file short, as a torn write does.</summary>
    /// <param name="persistenceRoot">The persistence data directory.</param>
    /// <param name="groupId">The replica group identifier.</param>
    /// <param name="slot">The slot index, 0 or 1.</param>
    /// <param name="length">The length to keep in bytes.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>A task that completes when the file is cut.</returns>
    internal static Task TearAsync(string persistenceRoot, string groupId, int slot, int length, CancellationToken cancellationToken)
    {
        var path = SlotPath(persistenceRoot, groupId, slot);
        cancellationToken.ThrowIfCancellationRequested();
        using var handle = File.OpenHandle(path, FileMode.Open, FileAccess.Write, FileShare.Read);
        ArgumentOutOfRangeException.ThrowIfNegative(length);
        RandomAccess.SetLength(handle, Math.Min(length, RandomAccess.GetLength(handle)));
        return Task.CompletedTask;
    }

    /// <summary>Copies the slot files of one group to the directory of another, as a restored backup would.</summary>
    /// <param name="sourceRoot">The persistence data directory to copy from.</param>
    /// <param name="sourceGroupId">The replica group identifier to copy from.</param>
    /// <param name="targetRoot">The persistence data directory to copy to.</param>
    /// <param name="targetGroupId">The replica group identifier to copy to.</param>
    internal static void Copy(string sourceRoot, string sourceGroupId, string targetRoot, string targetGroupId)
    {
        var target = GroupStoragePaths.GetGroupDirectory(targetRoot, targetGroupId);
        _ = Directory.CreateDirectory(target);
        File.Copy(PointerPath(sourceRoot, sourceGroupId), PointerPath(targetRoot, targetGroupId), true);
        foreach (var pointer in GroupSnapshotPointers.Read(PointerPath(sourceRoot, sourceGroupId)))
        {
            if (pointer.Sequence == 0UL || pointer.Generation == Guid.Empty)
                continue;

            var name = $"group.checkpoint-{pointer.Generation:N}.snapshot";
            File.Copy(Path.Join(GroupStoragePaths.GetGroupDirectory(sourceRoot, sourceGroupId), name), Path.Join(target, name), true);
        }
    }

    internal static string PointerPath(string root, string groupId) =>
        Path.Join(GroupStoragePaths.GetGroupDirectory(root, groupId), "group.snapshot.meta");

    internal static string SlotPath(string root, string groupId, int slot)
    {
        foreach (var pointer in GroupSnapshotPointers.Read(PointerPath(root, groupId)))
        {
            if (pointer.Sequence != 0UL && pointer.Slot == slot)
                return Path.Join(GroupStoragePaths.GetGroupDirectory(root, groupId), $"group.checkpoint-{pointer.Generation:N}.snapshot");
        }

        throw new FileNotFoundException("The snapshot pointer slot has not been published.");
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
