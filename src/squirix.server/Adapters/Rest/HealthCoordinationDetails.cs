using System.Text.Json.Serialization;
using Squirix.Server.Attributes;

namespace Squirix.Server.Adapters.Rest;

[Immutable]
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
