using Squirix.Server.Storage.Entries.Binary;
using Squirix.Server.Storage.Journaling.Entries;
using Xunit;

namespace Squirix.Server.UnitTests.Journaling;

/// <summary>Set wire passthrough for journal entry encoding.</summary>
public sealed class JournalEntryWirePassthroughTests
{
    /// <summary>Ingress value bytes are appended without re-encoding the decoded value.</summary>
    [Fact]
    public void PrepareEncodeShouldPassthroughIngressWireValuePayload()
    {
        var cart = CreateCart();
        var ingressPayload = CacheEntryCodec.EncodeWireValueToOwned(cart);
        var baseline = CacheEntryCodec.EncodeToOwned(new CacheEntry<object?> { Value = cart, Version = 1 });
        var passthroughEntry = new CacheEntry<object?>
        {
            Value = cart,
            WireValuePayload = ingressPayload,
            Version = 1,
        };
        var passthrough = CacheEntryCodec.EncodeToOwned(passthroughEntry);

        Assert.Equal(baseline, passthrough);
        Assert.Equal(baseline, PreparedJournalEntry.From(passthroughEntry).EncodedBytes);
    }

    /// <summary>Size guard uses passthrough payload length without walking the value tree.</summary>
    [Fact]
    public void ComputeEncodedLengthShouldUseWireValuePayloadLength()
    {
        var cart = CreateCart();
        var ingressPayload = CacheEntryCodec.EncodeWireValueToOwned(cart);
        var entry = new CacheEntry<object?> { Value = cart, WireValuePayload = ingressPayload, Version = 1 };

        Assert.Equal(CacheEntryCodec.EncodeToOwned(entry).Length, JournalEntryPayload.ComputeEncodedLength(entry));
    }

    private static WireTestCart CreateCart() => new()
    {
        Id = "cart-1",
        Total = 32.25m,
        Items =
        [
            new WireTestCartItem { Sku = "SKU-001", Quantity = 2, Price = 12.50m },
        ],
    };

    private sealed class WireTestCart
    {
        public string Id { get; init; } = string.Empty;

        public System.Collections.Generic.List<WireTestCartItem> Items { get; init; } = [];

        public decimal Total { get; init; }
    }

    private sealed class WireTestCartItem
    {
        public decimal Price { get; init; }

        public int Quantity { get; init; }

        public string Sku { get; init; } = string.Empty;
    }
}
