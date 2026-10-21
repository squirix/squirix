using System;
using System.Buffers;
using System.IO;
using Microsoft.Extensions.Logging;
using Microsoft.Win32.SafeHandles;
using Squirix.Server.Utils;

namespace Squirix.Server.Storage.Replication;

/// <summary>Durable group metadata: a two-slot file that is written in place through one persistent handle.</summary>
/// <remarks>
///     <para>
///     A publish writes the record to the slot that does not hold the current one with one positional write and flushes the handle before the
///     slot counts as current, so the previous record stays intact whenever a write is torn or lost. The file exists from its first publish
///     as a complete image, which is created beside it and renamed into place, so it never exists without a valid slot.
///     </para>
///     <para>
///     A vote grant and every record that raises the term are published mirrored, into both slots. The reader falls back to the older slot when the
///     newest fails its checksum, which is right for a torn write, but damage at rest to an acknowledged newest slot looks the same. Rolling back a
///     commit advance by one record is harmless, while rolling back a term could let a deposed leader truncate an acknowledged entry and rolling back
///     a vote could allow a second vote in the same term.
///     </para>
///     <para>
///     Not thread-safe: the owning follower log serializes every call under its gate. The layout lives in <see cref="GroupMetadataSlots" />.
///     </para>
/// </remarks>
internal sealed class GroupMetadataStore : IDisposable
{
    /// <summary>The sharing a reader needs to open the file while the owning node holds it open for writing.</summary>
    private const FileShare ReaderShare = FileShare.ReadWrite | FileShare.Delete;

    private readonly IFollowerLogFaultHooks _faults;
    private readonly string _groupId;
    private readonly ILogger _log;
    private readonly string _path;
    private readonly byte[] _slot = new byte[GroupMetadataSlots.SlotSize];
    private readonly string _tempPath;

    private SafeFileHandle? _handle;
    private int _currentSlot;
    private ulong _sequence;

    internal GroupMetadataStore(string path, string tempPath, string groupId, IFollowerLogFaultHooks faults, ILogger log)
    {
        ArgumentException.ThrowIfNullOrEmpty(path);
        ArgumentException.ThrowIfNullOrEmpty(tempPath);
        ArgumentException.ThrowIfNullOrEmpty(groupId);
        ArgumentNullException.ThrowIfNull(faults);
        ArgumentNullException.ThrowIfNull(log);
        _path = path;
        _tempPath = tempPath;
        _groupId = groupId;
        _faults = faults;
        _log = log;
    }

    /// <inheritdoc />
    public void Dispose()
    {
        _handle?.Dispose();
        _handle = null;
    }

    /// <summary>Builds the message of the failure a refused read stands for.</summary>
    /// <param name="status">The status of the read; not <see cref="GroupMetadataReadStatus.Valid" />.</param>
    /// <param name="groupId">The replica group identifier.</param>
    /// <returns>The message.</returns>
    internal static string DescribeFailure(GroupMetadataReadStatus status, string groupId) =>
        status == GroupMetadataReadStatus.Unsupported
            ? $"Replica group '{groupId}' metadata was written by an earlier build and is not supported; the file was left unchanged."
            : $"Replica group '{groupId}' metadata is corrupt.";

    /// <summary>Reads a metadata file without owning it, also while a node holds it open for writing.</summary>
    /// <param name="path">The metadata file path.</param>
    /// <returns>The newest valid record, or why there is none.</returns>
    /// <exception cref="FileNotFoundException">The file does not exist.</exception>
    internal static GroupMetadataRead Read(string path)
    {
        using var handle = File.OpenHandle(path, FileMode.Open, FileAccess.Read, ReaderShare);
        var length = RandomAccess.GetLength(handle);

        // A file of another size is only told apart by its first bytes, the earlier magic, so no more than that is read of it.
        var wanted = length == GroupMetadataSlots.FileSize ? GroupMetadataSlots.FileSize : int.CreateTruncating(Math.Min(length, sizeof(uint)));
        var image = ArrayPool<byte>.Shared.Rent(GroupMetadataSlots.FileSize);
        try
        {
            var read = 0;
            while (read < wanted)
            {
                var count = RandomAccess.Read(handle, image.AsSpan(read, wanted - read), read);
                if (count == 0)
                    break;

                read += count;
            }

            // A file that shrank while it was read is judged on the bytes that were there.
            return GroupMetadataSlots.Select(image.AsSpan(0, read));
        }
        finally
        {
            ArrayPool<byte>.Shared.ReturnCleared(image);
        }
    }

    /// <summary>Loads the existing file and, when it holds a valid record of this group, opens the persistent handle on it.</summary>
    /// <returns>The newest valid record; any other status means the file was left untouched and no handle is open.</returns>
    internal GroupMetadataRead Open()
    {
        var loaded = Read(_path);
        if (loaded.Status != GroupMetadataReadStatus.Valid)
            return loaded;

        if (!string.Equals(loaded.Metadata.GroupId, _groupId, StringComparison.Ordinal))
            return new GroupMetadataRead(GroupMetadataReadStatus.Corrupt, default, 0, 0UL);

        OpenHandle();
        _currentSlot = loaded.Slot;
        _sequence = loaded.Sequence;
        return loaded;
    }

    /// <summary>Durably publishes a record; the first publish creates the file.</summary>
    /// <param name="meta">The record.</param>
    /// <exception cref="InvalidOperationException">The record does not fit a slot, or the file exists but was not opened.</exception>
    /// <remarks>
    /// The fault hook fires once per attempt after the record is encoded and before any byte of the file changes, so a fault leaves the previous
    /// record as the one a reopen reads. A failed attempt keeps the current slot and sequence, so the next one overwrites the same slot.
    /// </remarks>
    internal void Publish(in GroupLogMetadata meta)
    {
        if (_handle is not { } handle)
        {
            Create(in meta);
            return;
        }

        var next = _sequence + 1UL;
        var target = _currentSlot == 0 ? 1 : 0;
        long offset = target == 0 ? 0 : GroupMetadataSlots.SlotSize;
        GroupMetadataSlots.Encode(_slot, next, in meta);
        _faults.OnMetaPublishing();
        RandomAccess.Write(handle, _slot, offset);
        RandomAccess.FlushToDisk(handle);
        _currentSlot = target;
        _sequence = next;
    }

    /// <summary>Durably publishes a record twice, so that both slots hold it.</summary>
    /// <param name="meta">The record.</param>
    /// <exception cref="InvalidOperationException">The record does not fit a slot, or the file exists but was not opened.</exception>
    /// <remarks>
    /// Each publish is a full <see cref="Publish" /> with its own fault hook call. A crash or failure between the two leaves the first copy as the
    /// current record, which is a valid durable state, and a failure of the second is reported like any failed publish.
    /// </remarks>
    internal void PublishMirrored(in GroupLogMetadata meta)
    {
        Publish(in meta);
        Publish(in meta);
    }

    private void Create(in GroupLogMetadata meta)
    {
        if (File.Exists(_path))
            throw new InvalidOperationException($"Replica group '{_groupId}' metadata exists but was not opened; it is not overwritten.");

        var image = ArrayPool<byte>.Shared.Rent(GroupMetadataSlots.FileSize);
        try
        {
            // The whole image is zeroed first: the second slot stays never written, and a rented array is not clean.
            image.AsSpan(0, GroupMetadataSlots.FileSize).Clear();
            GroupMetadataSlots.Encode(image.AsSpan(0, GroupMetadataSlots.SlotSize), 1UL, in meta);
            _faults.OnMetaPublishing();
            using var temp = File.OpenHandle(_tempPath, FileMode.Create, FileAccess.Write, FileShare.None, FileOptions.WriteThrough);
            RandomAccess.Write(temp, image.AsSpan(0, GroupMetadataSlots.FileSize), 0);
            RandomAccess.FlushToDisk(temp);
        }
        finally
        {
            ArrayPool<byte>.Shared.ReturnCleared(image);
        }

        var result = FileEx.PublishFile(_tempPath, _path, TimeProvider.System);
        if (result.Attempts > 1)
            ServerLog.DurablePublishRetried(_log, _tempPath, result.Attempts, result.Holders ?? FileEx.UnknownHolders);

        OpenHandle();
        _currentSlot = 0;
        _sequence = 1UL;
    }

    private void OpenHandle()
    {
        var options = OperatingSystem.IsWindows() ? FileOptions.WriteThrough : FileOptions.None;
        Dispose();
        _handle = File.OpenHandle(_path, FileMode.Open, FileAccess.ReadWrite, FileShare.Read, options);
    }
}
