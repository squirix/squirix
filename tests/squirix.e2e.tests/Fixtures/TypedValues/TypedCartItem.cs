using System.Text.Json.Serialization;

namespace Squirix.E2ETests.Fixtures.TypedValues;

internal sealed class TypedCartItem
{
    public int Quantity { get; init; }

    public string Sku { get; init; } = string.Empty;

    public decimal Price { get; init; }
}
