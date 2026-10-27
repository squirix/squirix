using System.Text.Json.Serialization;
using Squirix.Server.Attributes;

namespace Squirix.Server.Adapters.Rest;

[Immutable]
internal sealed class HealthReplicaCopyDetails
{
    internal HealthReplicaCopyDetails(string nodeId, string state, ulong lagEntries)
    {
        NodeId = nodeId;
        State = state;
        LagEntries = lagEntries;
    }

    [JsonInclude]
    internal ulong LagEntries { get; }

    [JsonInclude]
    internal string NodeId { get; }

    [JsonInclude]
    internal string State { get; }
}
