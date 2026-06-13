using System;
using System.Collections.Generic;

namespace Squirix.E2ETests.PublicApi.TypedValues;

internal sealed class TypedMutableCart
{
    public string? CouponCode { get; init; }

    public string Id { get; init; } = string.Empty;

    public List<TypedCartItem> Items { get; init; } = [];

    public decimal Total { get; init; }

    public DateTimeOffset UpdatedAt { get; init; }
}
