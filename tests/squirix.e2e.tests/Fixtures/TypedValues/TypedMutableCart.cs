using System;
using System.Collections.Generic;
using System.Text.Json.Serialization;

namespace Squirix.E2ETests.Fixtures.TypedValues;

internal sealed class TypedMutableCart
{
    [JsonInclude]
    internal string? CouponCode { get; init; }

    [JsonInclude]
    internal string Id { get; init; } = string.Empty;

    [JsonInclude]
    internal List<TypedCartItem> Items { get; init; } = [];

    [JsonInclude]
    internal decimal Total { get; init; }

    [JsonInclude]
    internal DateTimeOffset UpdatedAt { get; init; }
}
