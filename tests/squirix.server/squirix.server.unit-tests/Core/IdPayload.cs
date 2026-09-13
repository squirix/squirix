using Squirix.Server.Attributes;

namespace Squirix.Server.UnitTests.Core;

/// <summary>Simple payload with Id.</summary>
[Immutable]
internal sealed class IdPayload
{
    public int Id { get; init; }
}
