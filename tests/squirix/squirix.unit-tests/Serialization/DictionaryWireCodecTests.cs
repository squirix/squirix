using System;
using System.Collections.Generic;
using Squirix.Internal.Cluster.Transport.Binary;
using Squirix.Serialization;
using Xunit;

namespace Squirix.UnitTests.Serialization;

/// <summary>Structured wire codec coverage for dictionary and mutable POCOs.</summary>
public sealed class DictionaryWireCodecTests
{
    /// <summary>Dictionary properties round-trip through metadata wire encoding.</summary>
    [Fact]
    public void DictionaryShouldRoundTripThroughMetadataCodec()
    {
        var serializer = new SystemTextJsonSerializer();
        var original = new Dictionary<string, string>(StringComparer.Ordinal)
        {
            ["region"] = "west",
            ["tier"] = "gold",
        };

        var bytes = CacheValueWireCodec.EncodeWireValueToOwned(original, serializer);
        Assert.True(CacheValueWireCodec.TryReadWireValue(bytes, serializer, out Dictionary<string, string>? roundTrip));
        Assert.NotNull(roundTrip);
        Assert.Equal("west", roundTrip["region"]);
        Assert.Equal("gold", roundTrip["tier"]);
    }

    /// <summary>Record properties typed as IReadOnlyList round-trip through metadata wire encoding.</summary>
    [Fact]
    public void ReadOnlyListPropertyShouldRoundTripThroughMetadataCodec()
    {
        var serializer = new SystemTextJsonSerializer();
        var original = new ReadOnlyListProfileFixture(
            7,
            "alice",
            ["admin", "buyer"],
            new DateTimeOffset(2026, 6, 6, 8, 0, 0, TimeSpan.Zero));

        var bytes = CacheValueWireCodec.EncodeWireValueToOwned(original, serializer);
        Assert.True(CacheValueWireCodec.TryReadWireValue(bytes, serializer, out ReadOnlyListProfileFixture? roundTrip));
        Assert.NotNull(roundTrip);
        Assert.Equal(original.Id, roundTrip.Id);
        Assert.Equal(original.Name, roundTrip.Name);
        Assert.Equal(original.Roles, roundTrip.Roles);
        Assert.Equal(original.CreatedAt, roundTrip.CreatedAt);
    }

    /// <summary>Mutable classes with list properties round-trip through metadata wire encoding.</summary>
    [Fact]
    public void MutableClassWithListShouldRoundTripThroughMetadataCodec()
    {
        var serializer = new SystemTextJsonSerializer();
        var original = new MutableCartWireFixture
        {
            Id = "cart-1",
            CouponCode = "SAVE10",
            Total = 32.25m,
            UpdatedAt = new DateTimeOffset(2026, 6, 6, 9, 15, 30, TimeSpan.Zero),
            Items =
            [
                new CartItemWireFixture { Sku = "SKU-001", Quantity = 2, Price = 12.50m },
                new CartItemWireFixture { Sku = "SKU-002", Quantity = 1, Price = 7.25m },
            ],
        };

        var bytes = CacheValueWireCodec.EncodeWireValueToOwned(original, serializer);
        Assert.True(CacheValueWireCodec.TryReadWireValue(bytes, serializer, out MutableCartWireFixture? roundTrip));
        Assert.NotNull(roundTrip);
        Assert.Equal(original.Id, roundTrip.Id);
        Assert.Equal(original.CouponCode, roundTrip.CouponCode);
        Assert.Equal(original.Total, roundTrip.Total);
        Assert.Equal(original.UpdatedAt, roundTrip.UpdatedAt);
        Assert.Equal(2, roundTrip.Items.Count);
        Assert.Equal("SKU-001", roundTrip.Items[0].Sku);
    }

    private sealed class MutableCartWireFixture
    {
        public string? CouponCode { get; init; }

        public string Id { get; init; } = string.Empty;

        public List<CartItemWireFixture> Items { get; init; } = [];

        public decimal Total { get; init; }

        public DateTimeOffset UpdatedAt { get; init; }
    }

    private sealed class CartItemWireFixture
    {
        public decimal Price { get; init; }

        public int Quantity { get; init; }

        public string Sku { get; init; } = string.Empty;
    }

    private sealed record ReadOnlyListProfileFixture(
        long Id,
        string Name,
        IReadOnlyList<string> Roles,
        DateTimeOffset CreatedAt);
}
