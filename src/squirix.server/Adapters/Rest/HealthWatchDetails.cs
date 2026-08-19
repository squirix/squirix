using System.Text.Json.Serialization;
using Squirix.Server.Attributes;

namespace Squirix.Server.Adapters.Rest;

[Immutable]
internal sealed record HealthWatchDetails
{
    [JsonConstructor]
    internal HealthWatchDetails(bool configured, int active, int droppedEvents, int bufferedEvents)
    {
        Configured = configured;
        Active = active;
        DroppedEvents = droppedEvents;
        BufferedEvents = bufferedEvents;
    }

    [JsonInclude]
    internal int Active { get; }

    [JsonInclude]
    internal int BufferedEvents { get; }

    [JsonInclude]
    internal bool Configured { get; }

    [JsonInclude]
    internal int DroppedEvents { get; }
}
