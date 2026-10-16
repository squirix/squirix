using System;
using Squirix.Server.Attributes;

namespace Squirix.Server.Storage.Replication;

/// <summary>Durable file paths for one replica-group log.</summary>
/// <param name="GroupDirectory">The directory holding the group's durable state.</param>
/// <param name="MetadataPath">The path of the durable group metadata file.</param>
/// <param name="MetadataTempPath">The path of the metadata temp file used while the metadata file is first created.</param>
/// <param name="LogSlot0Path">The path of the first group log generation file.</param>
/// <param name="LogSlot1Path">The path of the second group log generation file.</param>
/// <param name="LegacyLogPath">The path of the single log file written by earlier builds, which this build refuses to read.</param>
[Immutable]
internal sealed record FollowerLogPaths(string GroupDirectory, string MetadataPath, string MetadataTempPath, string LogSlot0Path, string LogSlot1Path, string LegacyLogPath)
{
    /// <summary>Builds the durable paths for a replica group.</summary>
    /// <param name="persistenceRoot">The persistence root directory.</param>
    /// <param name="groupId">The replica-group identifier.</param>
    /// <returns>The resolved paths.</returns>
    internal static FollowerLogPaths Create(string persistenceRoot, string groupId) => new(
        GroupStoragePaths.GetGroupDirectory(persistenceRoot, groupId),
        GroupStoragePaths.GetMetadataPath(persistenceRoot, groupId),
        GroupStoragePaths.GetMetadataTempPath(persistenceRoot, groupId),
        GroupStoragePaths.GetLogSlotPath(persistenceRoot, groupId, 0),
        GroupStoragePaths.GetLogSlotPath(persistenceRoot, groupId, 1),
        GroupStoragePaths.GetLegacyLogPath(persistenceRoot, groupId));

    /// <summary>Gets the path of one of the two group log generation files.</summary>
    /// <param name="slot">The slot index, 0 or 1.</param>
    /// <returns>The generation file path.</returns>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="slot" /> is not 0 or 1.</exception>
    internal string GetLogSlotPath(int slot)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(slot);
        ArgumentOutOfRangeException.ThrowIfGreaterThanOrEqual(slot, GroupLogSlots.SlotCount);
        return slot == 0 ? LogSlot0Path : LogSlot1Path;
    }
}
