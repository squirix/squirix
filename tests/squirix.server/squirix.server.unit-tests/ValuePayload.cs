using Squirix.Server.Attributes;

namespace Squirix.Server.UnitTests;

/// <summary>Test payload with single value.</summary>
[Immutable]
internal sealed class ValuePayload
{
    public string Value { get; init; } = string.Empty;
}
