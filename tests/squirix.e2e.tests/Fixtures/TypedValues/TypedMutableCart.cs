using System;
using System.Collections.Generic;
using System.Text.Json.Serialization;

namespace Squirix.E2ETests.Fixtures.TypedValues;

internal sealed class TypedMutableCart
{
    public string Id { get; init; } = string.Empty;

    [JsonInclude]
    internal List<TypedCartItem> Items { get; init; } = [];

    [JsonInclude]
    internal decimal Total { get; init; }

    public DateTimeOffset UpdatedAt { get; init; }

    public string? CouponCode { get; init; }
}
