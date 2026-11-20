using System.Text.Json.Serialization;
using Squirix.Server.Attributes;

namespace Squirix.Server.UnitTests.Support;

/// <summary>Test payload for cache value mapping.</summary>
[Immutable]
internal sealed class SamplePayload
{
    [JsonInclude]
    internal int Id { get; init; }

    [JsonInclude]
    internal string Name { get; init; } = string.Empty;

    [JsonInclude]
    internal string[] Tags { get; init; } = [];
}
