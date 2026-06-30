using System;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Core;
using Squirix.Server.LocalCache;
using Squirix.Server.Storage.Entries.Binary;
using Squirix.Server.Utils;
using Xunit;

namespace Squirix.Server.UnitTests.LocalCache;

/// <summary>Get wire payload cache behavior on <see cref="PhysicalCache{T}" />.</summary>
public sealed class PhysicalCacheWirePayloadTests
{
    /// <summary>Structured Get reuses ingress payload bytes stored at Set time.</summary>
    [Fact]
    public async Task GetValueShouldReuseIngressWirePayload()
    {
        var cart = CreateCart();
        var ingressPayload = CacheEntryCodec.EncodeWireValueToOwned(cart);
        await using var cache = new PhysicalCache<object?>();
        var key = new CacheKey("default", "cart-key");
        await cache.SetAsync(
            key,
            new CacheEntry<object?>
            {
                Value = cart,
                WireValuePayload = ingressPayload,
                Version = 1,
            },
            CancellationToken.None);

        var result = await cache.GetValueAsync(key, CancellationToken.None);

        Assert.True(result.Found);
        Assert.Same(ingressPayload, GetWirePayloadArray(result.WireValuePayload));
        var grpcValue = CacheWireMapper.CacheValueToGrpcValue(result.Value, result.WireValuePayload);
        Assert.Equal(ingressPayload, grpcValue.Payload.ToByteArray());
    }

    /// <summary>Structured Get captures wire payload once when ingress bytes are absent.</summary>
    [Fact]
    public async Task GetValueShouldCaptureWirePayloadOnStoreWhenMissing()
    {
        var cart = CreateCart();
        await using var cache = new PhysicalCache<object?>();
        var key = new CacheKey("default", "cart-key");
        await cache.SetAsync(
            key,
            new CacheEntry<object?> { Value = cart, Version = 1 },
            CancellationToken.None);

        var first = await cache.GetValueAsync(key, CancellationToken.None);
        var second = await cache.GetValueAsync(key, CancellationToken.None);

        Assert.True(first.Found);
        Assert.False(first.WireValuePayload.IsEmpty);
        Assert.Same(GetWirePayloadArray(first.WireValuePayload), GetWirePayloadArray(second.WireValuePayload));
    }

    private static byte[] GetWirePayloadArray(ReadOnlyMemory<byte> wirePayload)
    {
        if (wirePayload.IsEmpty)
            return [];

        if (System.Runtime.InteropServices.MemoryMarshal.TryGetArray(wirePayload, out var segment) && segment.Array is not null)
            return segment.Array;

        return wirePayload.ToArray();
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
