using System.Text.Json.Serialization;

namespace Squirix.Server.Adapters.Rest;

internal sealed record IncrementResponse
{
    [JsonConstructor]
    internal IncrementResponse(long value)
    {
        Value = value;
    }

    [JsonInclude]
    internal long Value { get; }
}
