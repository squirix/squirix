using System.Text.Json.Serialization;

namespace Squirix.Server.Adapters.Rest;

internal sealed class HealthJournalDiskDetails
{
    internal HealthJournalDiskDetails(string state, long maxBytes, long usedBytes, long highWaterBytes, bool writeRejectionActive)
    {
        State = state;
        MaxBytes = maxBytes;
        UsedBytes = usedBytes;
        HighWaterBytes = highWaterBytes;
        WriteRejectionActive = writeRejectionActive;
    }

    [JsonInclude]
    internal long HighWaterBytes { get; }

    [JsonInclude]
    internal long MaxBytes { get; }

    [JsonInclude]
    internal string State { get; }

    [JsonInclude]
    internal long UsedBytes { get; }

    [JsonInclude]
    internal bool WriteRejectionActive { get; }
}
