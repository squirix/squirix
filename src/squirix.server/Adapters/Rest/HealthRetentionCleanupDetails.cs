using System;
using System.Text.Json.Serialization;
using Squirix.Server.Attributes;

namespace Squirix.Server.Adapters.Rest;

[Immutable]
internal sealed record HealthRetentionCleanupDetails
{
    [JsonConstructor]
    internal HealthRetentionCleanupDetails(bool degraded, int consecutiveWriteFailures, int recentFailureCount, DateTime? lastFailureUtc)
    {
        Degraded = degraded;
        ConsecutiveWriteFailures = consecutiveWriteFailures;
        RecentFailureCount = recentFailureCount;
        LastFailureUtc = lastFailureUtc;
    }

    [JsonInclude]
    internal int ConsecutiveWriteFailures { get; }

    [JsonInclude]
    internal bool Degraded { get; }

    [JsonInclude]
    internal DateTime? LastFailureUtc { get; }

    [JsonInclude]
    internal int RecentFailureCount { get; }
}
