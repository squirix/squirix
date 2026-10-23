using System;
using System.Buffers;
using System.IO;
using System.Text;
using Squirix.Server.Utils;

namespace Squirix.Server.Storage.Replication;

/// <summary>Resolves safe on-disk paths for replica-group storage under the persistence root.</summary>
/// <remarks>
/// Each group lives in its own directory whose segment is a stable hexadecimal encoding of the UTF-8
/// <c language="csharp">group_id</c>. Raw <c language="csharp">NodeId</c>, cache name, and cache keys are never used as path segments. All paths
/// are validated to remain under the storage root.
/// </remarks>
internal static class GroupStoragePaths
{
    private const string RootSegment = "replication";
    private const string DirectoryPrefix = "grp-";
    private const string MetadataFileName = "group.meta";
    private const string MetadataTempName = "group.meta.tmp";
    private const string LogLegacyFileName = "group.log";
    private const string LogSlot0FileName = "group.log.0";
    private const string LogSlot1FileName = "group.log.1";
    private const string SnapshotLegacyFileName = "group.snapshot";
    private const string SnapshotSlot0FileName = "group.snapshot.0";
    private const string SnapshotSlot1FileName = "group.snapshot.1";

    /// <summary>Resolves the replication storage root directory under the persistence root.</summary>
    /// <param name="persistenceRoot">The persistence data directory.</param>
    /// <returns>The replication storage root path.</returns>
    internal static string GetRoot(string persistenceRoot) => PathEx.Combine(persistenceRoot, RootSegment);

    /// <summary>Encodes a <c language="csharp">group_id</c> into a stable, path-safe directory segment.</summary>
    /// <param name="groupId">The replica group identifier.</param>
    /// <returns>A hexadecimal directory segment.</returns>
    internal static string EncodeGroupSegment(string groupId)
    {
        ArgumentNullException.ThrowIfNull(groupId);
        var rented = ArrayPool<byte>.Shared.Rent(Encoding.UTF8.GetMaxByteCount(groupId.Length));
        try
        {
            var written = Encoding.UTF8.GetBytes(groupId, rented);
            return DirectoryPrefix + Convert.ToHexString(rented.AsSpan(0, written));
        }
        finally
        {
            ArrayPool<byte>.Shared.ReturnCleared(rented);
        }
    }

    /// <summary>Resolves the group directory under the storage root, throwing when the path would escape it.</summary>
    /// <param name="persistenceRoot">The persistence data directory.</param>
    /// <param name="groupId">The replica group identifier.</param>
    /// <returns>The group directory path.</returns>
    internal static string GetGroupDirectory(string persistenceRoot, string groupId) => PathEx.Combine(GetRoot(persistenceRoot), EncodeGroupSegment(groupId));

    /// <summary>Resolves the durable metadata file path for a group.</summary>
    /// <param name="persistenceRoot">The persistence data directory.</param>
    /// <param name="groupId">The replica group identifier.</param>
    /// <returns>The metadata file path.</returns>
    internal static string GetMetadataPath(string persistenceRoot, string groupId) => Path.Join(GetGroupDirectory(persistenceRoot, groupId), MetadataFileName);

    /// <summary>Resolves the temporary metadata file path used while the metadata file is first created.</summary>
    /// <param name="persistenceRoot">The persistence data directory.</param>
    /// <param name="groupId">The replica group identifier.</param>
    /// <returns>The temporary metadata file path.</returns>
    internal static string GetMetadataTempPath(string persistenceRoot, string groupId) => Path.Join(GetGroupDirectory(persistenceRoot, groupId), MetadataTempName);

    /// <summary>Resolves the path of one of the two group log generation files for a group.</summary>
    /// <param name="persistenceRoot">The persistence data directory.</param>
    /// <param name="groupId">The replica group identifier.</param>
    /// <param name="slot">The slot index, 0 or 1.</param>
    /// <returns>The log slot file path.</returns>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="slot" /> is not 0 or 1.</exception>
    internal static string GetLogSlotPath(string persistenceRoot, string groupId, int slot)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(slot);
        ArgumentOutOfRangeException.ThrowIfGreaterThanOrEqual(slot, GroupLogSlots.SlotCount);
        return Path.Join(GetGroupDirectory(persistenceRoot, groupId), slot == 0 ? LogSlot0FileName : LogSlot1FileName);
    }

    /// <summary>Resolves the path of the single log file written by earlier builds, which this build refuses to read.</summary>
    /// <param name="persistenceRoot">The persistence data directory.</param>
    /// <param name="groupId">The replica group identifier.</param>
    /// <returns>The earlier log file path.</returns>
    internal static string GetLegacyLogPath(string persistenceRoot, string groupId) => Path.Join(GetGroupDirectory(persistenceRoot, groupId), LogLegacyFileName);

    /// <summary>Resolves the path of one of the two durable snapshot slot files for a group.</summary>
    /// <param name="persistenceRoot">The persistence data directory.</param>
    /// <param name="groupId">The replica group identifier.</param>
    /// <param name="slot">The slot index, 0 or 1.</param>
    /// <returns>The snapshot slot file path.</returns>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="slot" /> is not 0 or 1.</exception>
    internal static string GetSnapshotSlotPath(string persistenceRoot, string groupId, int slot)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(slot);
        ArgumentOutOfRangeException.ThrowIfGreaterThanOrEqual(slot, GroupSnapshotSlots.SlotCount);
        return Path.Join(GetGroupDirectory(persistenceRoot, groupId), slot == 0 ? SnapshotSlot0FileName : SnapshotSlot1FileName);
    }

    /// <summary>Resolves the path of the single snapshot file written by earlier builds, which this build refuses to read.</summary>
    /// <param name="persistenceRoot">The persistence data directory.</param>
    /// <param name="groupId">The replica group identifier.</param>
    /// <returns>The earlier snapshot file path.</returns>
    internal static string GetLegacySnapshotPath(string persistenceRoot, string groupId) => Path.Join(GetGroupDirectory(persistenceRoot, groupId), SnapshotLegacyFileName);
}
