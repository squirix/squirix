using System.Text.Json.Serialization;

namespace Squirix.Server.Adapters.Rest;

internal sealed record HealthCoordinationDetails
{
    [JsonConstructor]
    internal HealthCoordinationDetails(HealthLeaseDetails leases, HealthWatchDetails watches)
    {
        Leases = leases;
        Watches = watches;
    }

    [JsonInclude]
    internal HealthLeaseDetails Leases { get; }

    [JsonInclude]
    internal HealthWatchDetails Watches { get; }
}
