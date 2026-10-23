using System.Text.Json.Serialization;
using Squirix.Server.Attributes;

namespace Squirix.Server.Adapters.Rest;

[Immutable]
internal sealed class HealthReplicaGroupDetails
{
    internal HealthReplicaGroupDetails(string groupId, long logBytes, int retainedEntries, ulong snapshotIndex)
    {
        GroupId = groupId;
        LogBytes = logBytes;
        RetainedEntries = retainedEntries;
        SnapshotIndex = snapshotIndex;
    }

    [JsonInclude]
    internal string GroupId { get; }

    [JsonInclude]
    internal long LogBytes { get; }

    [JsonInclude]
    internal int RetainedEntries { get; }

    [JsonInclude]
    internal ulong SnapshotIndex { get; }
}
