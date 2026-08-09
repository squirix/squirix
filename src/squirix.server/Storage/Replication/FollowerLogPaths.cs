using Squirix.Server.Attributes;

namespace Squirix.Server.Storage.Replication;

/// <summary>Durable file paths for one replica-group log.</summary>
/// <param name="GroupDirectory">The directory holding the group's durable state.</param>
/// <param name="MetadataPath">The path of the durable group metadata file.</param>
/// <param name="MetadataTempPath">The path of the metadata temp file used during atomic metadata publication.</param>
/// <param name="LogPath">The path of the durable journal file.</param>
/// <param name="LogTempPath">The path of the journal temp file used during atomic log replacement.</param>
[Immutable]
internal sealed record FollowerLogPaths(string GroupDirectory, string MetadataPath, string MetadataTempPath, string LogPath, string LogTempPath)
{
    /// <summary>Builds the durable paths for a replica group.</summary>
    /// <param name="persistenceRoot">The persistence root directory.</param>
    /// <param name="groupId">The replica-group identifier.</param>
    /// <returns>The resolved paths.</returns>
    internal static FollowerLogPaths Create(string persistenceRoot, string groupId) => new(
        GroupStoragePaths.GetGroupDirectory(persistenceRoot, groupId),
        GroupStoragePaths.GetMetadataPath(persistenceRoot, groupId),
        GroupStoragePaths.GetMetadataTempPath(persistenceRoot, groupId),
        GroupStoragePaths.GetLogPath(persistenceRoot, groupId),
        GroupStoragePaths.GetLogTempPath(persistenceRoot, groupId));
}
