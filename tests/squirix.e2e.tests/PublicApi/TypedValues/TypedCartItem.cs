namespace Squirix.E2ETests.PublicApi.TypedValues;

internal sealed class TypedCartItem
{
    public decimal Price { get; init; }

    public int Quantity { get; init; }

    public string Sku { get; init; } = string.Empty;
}
