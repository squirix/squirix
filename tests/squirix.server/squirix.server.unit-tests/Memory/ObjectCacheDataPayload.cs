using Squirix.Server.Attributes;

namespace Squirix.Server.UnitTests.Memory;

/// <summary>Payload for object cache estimator test.</summary>
[Immutable]
internal sealed class ObjectCacheDataPayload
{
    public string Data { get; init; } = string.Empty;
}
