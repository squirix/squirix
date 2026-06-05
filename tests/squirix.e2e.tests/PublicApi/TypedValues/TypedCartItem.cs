namespace Squirix.E2ETests.PublicApi.TypedValues;

internal sealed class TypedCartItem
{
    public string Sku { get; init; } = string.Empty;

    public int Quantity { get; init; }

    public decimal Price { get; init; }
}
