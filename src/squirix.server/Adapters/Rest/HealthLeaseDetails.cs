using System.Text.Json.Serialization;

namespace Squirix.Server.Adapters.Rest;

internal sealed record HealthLeaseDetails
{
    [JsonConstructor]
    internal HealthLeaseDetails(bool configured, int active, int expired, int renewals)
    {
        Configured = configured;
        Active = active;
        Expired = expired;
        Renewals = renewals;
    }

    [JsonInclude]
    internal int Active { get; }

    [JsonInclude]
    internal bool Configured { get; }

    [JsonInclude]
    internal int Expired { get; }

    [JsonInclude]
    internal int Renewals { get; }
}
