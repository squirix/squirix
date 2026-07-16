using System.Text.Json.Serialization;

namespace Squirix.Server.Adapters.Rest;

internal sealed class HealthMemoryPressureDetails
{
    internal HealthMemoryPressureDetails(string state, long maxEstimatedCacheBytes, long estimatedCacheBytes, long entryCount, long rejectedWriteCount, bool writeRejectionActive)
    {
        State = state;
        MaxEstimatedCacheBytes = maxEstimatedCacheBytes;
        EstimatedCacheBytes = estimatedCacheBytes;
        EntryCount = entryCount;
        RejectedWriteCount = rejectedWriteCount;
        WriteRejectionActive = writeRejectionActive;
    }

    [JsonInclude]
    internal long EntryCount { get; }

    [JsonInclude]
    internal long EstimatedCacheBytes { get; }

    [JsonInclude]
    internal long MaxEstimatedCacheBytes { get; }

    [JsonInclude]
    internal long RejectedWriteCount { get; }

    [JsonInclude]
    internal string State { get; }

    [JsonInclude]
    internal bool WriteRejectionActive { get; }
}
