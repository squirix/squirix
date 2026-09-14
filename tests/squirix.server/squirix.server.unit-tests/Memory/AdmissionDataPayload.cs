using Squirix.Server.Attributes;

namespace Squirix.Server.UnitTests.Memory;

/// <summary>Payload for admission test.</summary>
[Immutable]
internal sealed class AdmissionDataPayload
{
    public string Data { get; init; } = string.Empty;
}
