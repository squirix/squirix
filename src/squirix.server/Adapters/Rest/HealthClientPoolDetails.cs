using System.Text.Json.Serialization;
using Squirix.Attributes;

namespace Squirix.Server.Adapters.Rest;

[Immutable]
internal sealed record HealthClientPoolDetails
{
    [JsonConstructor]
    internal HealthClientPoolDetails(bool configured, int peers)
    {
        Configured = configured;
        Peers = peers;
    }

    [JsonInclude]
    internal bool Configured { get; }

    [JsonInclude]
    internal int Peers { get; }
}
