using Squirix.Server.Attributes;

namespace Squirix.Server.UnitTests;

/// <summary>Test payload for cache value mapping.</summary>
[Immutable]
internal sealed class SamplePayload
{
    public int Id { get; init; }

    public string Name { get; init; } = string.Empty;

    public string[] Tags { get; init; } = [];
}
