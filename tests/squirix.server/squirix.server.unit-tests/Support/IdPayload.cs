using System.Text.Json.Serialization;
using Squirix.Server.Attributes;

namespace Squirix.Server.UnitTests.Support;

/// <summary>Simple payload with Id.</summary>
[Immutable]
internal sealed class IdPayload
{
    [JsonInclude]
    internal int Id { get; init; }
}
