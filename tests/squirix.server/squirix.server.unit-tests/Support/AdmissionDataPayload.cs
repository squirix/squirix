using System.Text.Json.Serialization;
using Squirix.Server.Attributes;

namespace Squirix.Server.UnitTests.Support;

/// <summary>Payload for admission test.</summary>
[Immutable]
internal sealed class AdmissionDataPayload
{
    [JsonInclude]
    internal string Data { get; init; } = string.Empty;
}
