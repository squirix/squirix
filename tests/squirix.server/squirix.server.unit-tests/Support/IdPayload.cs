using Squirix.Server.Attributes;

namespace Squirix.Server.UnitTests.Support;

/// <summary>Simple payload with Id.</summary>
[Immutable]
internal sealed class IdPayload
{
    public int Id { get; init; }
}
