using System;
using System.Text.Json.Serialization;
using Squirix.Server.Attributes;

namespace Squirix.Server.Adapters.Rest;

[Immutable]
internal sealed class HealthCompactionDetails
{
    internal HealthCompactionDetails(string state, DateTime? lastRunUtc, bool inFlight)
    {
        State = state;
        LastRunUtc = lastRunUtc;
        InFlight = inFlight;
    }

    [JsonInclude]
    internal bool InFlight { get; }

    [JsonInclude]
    internal DateTime? LastRunUtc { get; }

    [JsonInclude]
    internal string State { get; }
}
