using System.Text.Json.Serialization;

namespace Squirix.E2ETests.Fixtures.TypedValues;

internal sealed class TypedCartItem
{
    [JsonInclude]
    internal decimal Price { get; init; }

    [JsonInclude]
    internal int Quantity { get; init; }

    [JsonInclude]
    internal string Sku { get; init; } = string.Empty;
}
