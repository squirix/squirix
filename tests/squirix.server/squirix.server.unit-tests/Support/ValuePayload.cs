using System.Text.Json.Serialization;
using Squirix.Server.Attributes;

namespace Squirix.Server.UnitTests.Support;

/// <summary>Test payload with single value.</summary>
[Immutable]
internal sealed class ValuePayload
{
    [JsonInclude]
    internal string Value { get; init; } = string.Empty;
}
