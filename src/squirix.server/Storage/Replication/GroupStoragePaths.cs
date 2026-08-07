using System;
using System.IO;
using System.Text;
using Squirix.Server.Utils;

namespace Squirix.Server.Storage.Replication;

/// <summary>Resolves safe on-disk paths for replica-group storage under the persistence root.</summary>
/// <remarks>
/// Each group lives in its own directory whose segment is a stable hexadecimal encoding of the UTF-8
/// <c>group_id</c>. Raw <c>NodeId</c>, cache name, and cache keys are never used as path segments. All paths
/// are validated to remain under the storage root.
/// </remarks>
internal static class GroupStoragePaths
{
    private const string RootSegment = "replication";
    private const string DirectoryPrefix = "grp-";
    private const string MetadataFileName = "group.meta";
    private const string MetadataTempName = "group.meta.tmp";
    private const string LogFileName = "group.log";

    /// <summary>Resolves the replication storage root directory under the persistence root.</summary>
    /// <param name="persistenceRoot">The persistence data directory.</param>
    /// <returns>The replication storage root path.</returns>
    internal static string GetRoot(string persistenceRoot) => PathEx.Combine(persistenceRoot, RootSegment);

    /// <summary>Encodes a <c>group_id</c> into a stable, path-safe directory segment.</summary>
    /// <param name="groupId">The replica group identifier.</param>
    /// <returns>A hexadecimal directory segment.</returns>
    internal static string EncodeGroupSegment(string groupId)
    {
        ArgumentNullException.ThrowIfNull(groupId);
        var utf8 = Encoding.UTF8.GetBytes(groupId);
        return DirectoryPrefix + Convert.ToHexString(utf8);
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

    /// <summary>Resolves the temporary metadata file path used for atomic replacement.</summary>
    /// <param name="persistenceRoot">The persistence data directory.</param>
    /// <param name="groupId">The replica group identifier.</param>
    /// <returns>The temporary metadata file path.</returns>
    internal static string GetMetadataTempPath(string persistenceRoot, string groupId) => Path.Join(GetGroupDirectory(persistenceRoot, groupId), MetadataTempName);

    /// <summary>Resolves the append-only log file path for a group.</summary>
    /// <param name="persistenceRoot">The persistence data directory.</param>
    /// <param name="groupId">The replica group identifier.</param>
    /// <returns>The log file path.</returns>
    internal static string GetLogPath(string persistenceRoot, string groupId) => Path.Join(GetGroupDirectory(persistenceRoot, groupId), LogFileName);
}
