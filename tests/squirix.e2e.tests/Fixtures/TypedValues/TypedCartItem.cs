using System.Text.Json.Serialization;
using Squirix.Attributes;

namespace Squirix.E2ETests.Fixtures.TypedValues;

[Immutable]
internal sealed class TypedCartItem
{
    [JsonInclude]
    internal decimal Price { get; init; }

    [JsonInclude]
    internal int Quantity { get; init; }

    [JsonInclude]
    internal string Sku { get; init; } = string.Empty;
}
