using System.Text.Json.Serialization;

namespace Squirix.Server.Adapters.Rest;

internal sealed record HealthLeaseDetails
{
    [JsonConstructor]
    internal HealthLeaseDetails(bool configured, int active, int pendingGrants, int pendingReleases)
    {
        Configured = configured;
        Active = active;
        PendingGrants = pendingGrants;
        PendingReleases = pendingReleases;
    }

    [JsonInclude]
    internal int Active { get; }

    [JsonInclude]
    internal bool Configured { get; }

    [JsonInclude]
    internal int PendingGrants { get; }

    [JsonInclude]
    internal int PendingReleases { get; }
}
