using System;
using System.Collections.Generic;
using Google.Protobuf;
using Squirix.Server.Adapters.Grpc;
using Squirix.Server.Storage.Entries.Binary;
using Squirix.Transport.Grpc.Cache;
using Xunit;

namespace Squirix.Server.UnitTests.Adapters.Grpc;

/// <summary>Binary wire codec round-trip tests on the server adapter path.</summary>
public sealed class CacheWireCodecTests
{
    private static readonly byte[] Int42WireBytes = [ValueKind.Int64, 42, 0, 0, 0, 0, 0, 0, 0];

    /// <summary>Round-trips DateTimeOffset as Unix milliseconds without a JsonElement bridge.</summary>
    [Fact]
    public void DateTimeOffsetRoundTripsAsUnixMilliseconds()
    {
        var instant = new DateTimeOffset(2026, 6, 6, 12, 0, 0, TimeSpan.Zero);
        var bytes = CacheEntryCodec.EncodeWireValueToOwned(instant);
        Assert.Equal(ValueKind.Int64, bytes[0]);
        Assert.True(CacheEntryCodec.TryReadWireValue(bytes, out DateTimeOffset decoded));
        Assert.Equal(instant, decoded);
    }

    /// <summary>Encodes int entry payloads as the shared binary int64 wire blob.</summary>
    [Fact]
    public void EntryWireEncodesIntAsBinaryPayload()
    {
        var bytes = CacheEntryCodec.EncodeWireValueToOwned(42);
        Assert.Equal(Int42WireBytes, bytes);
    }

    /// <summary>Uses protobuf oneof fields for scalar GetValue encoding on the server wire path.</summary>
    [Fact]
    public void GetValueScalarUsesProtoOneof()
    {
        var grpcValue = CacheWireCodec.ToCacheValue(42);
        Assert.Equal(CacheValue.KindOneofCase.Int64Value, grpcValue.KindCase);
        Assert.Equal(42, CacheWireCodec.FromCacheValue<int>(grpcValue));
    }

    /// <summary>Mutable classes survive the server object-erasure Set/Get gRPC path used by E2E.</summary>
    [Fact]
    public void MutableClassSurvivesObjectErasureGrpcPath()
    {
        var cart = CreateMutableCart();
        var entry = new CacheEntryWire { Payload = ByteString.CopyFrom(CacheEntryCodec.EncodeWireValueToOwned(cart)) };

        var stored = CacheWireCodec.FromEntryWireValue<object?>(entry);
        var grpcValue = CacheWireCodec.ToCacheValue(stored);

        Assert.Equal(CacheValue.KindOneofCase.Payload, grpcValue.KindCase);
        Assert.True(CacheEntryCodec.TryReadWireValue(grpcValue.Payload.Span, out WireTestMutableCart? decoded));
        Assert.NotNull(decoded);
        Assert.Equal(cart.Id, decoded.Id);
        Assert.Equal(2, decoded.Items.Count);
    }

    /// <summary>Round-trips mutable classes with list properties on the server wire path.</summary>
    [Fact]
    public void MutableClassWithListRoundTripsThroughCacheValuePayload()
    {
        var cart = new WireTestMutableCart
        {
            Id = "cart-1",
            CouponCode = "SAVE10",
            Total = 32.25m,
            UpdatedAt = new DateTimeOffset(2026, 6, 6, 9, 15, 30, TimeSpan.Zero),
            Items =
            [
                new WireTestCartItem { Sku = "SKU-001", Quantity = 2, Price = 12.50m },
            ],
        };

        var grpcValue = CacheWireCodec.ToCacheValue(cart);
        Assert.Equal(CacheValue.KindOneofCase.Payload, grpcValue.KindCase);

        var decoded = CacheWireCodec.FromCacheValue<WireTestMutableCart>(grpcValue);
        Assert.NotNull(decoded);
        Assert.Equal(cart.Id, decoded.Id);
        Assert.Equal(cart.CouponCode, decoded.CouponCode);
        var item = Assert.Single(decoded.Items);
        Assert.Equal("SKU-001", item.Sku);
    }

    /// <summary>Records survive the server object-erasure Set/Get gRPC path used by E2E.</summary>
    [Fact]
    public void RecordSurvivesObjectErasureGrpcPath()
    {
        var profile = new WireTestProfile(7, "alice", ["admin"]);
        var entry = new CacheEntryWire { Payload = ByteString.CopyFrom(CacheEntryCodec.EncodeWireValueToOwned(profile)) };

        var stored = CacheWireCodec.FromEntryWireValue<object?>(entry);
        var grpcValue = CacheWireCodec.ToCacheValue(stored);

        Assert.Equal(CacheValue.KindOneofCase.Payload, grpcValue.KindCase);
        Assert.True(CacheEntryCodec.TryReadWireValue(grpcValue.Payload.Span, out WireTestProfile? decoded));
        Assert.NotNull(decoded);
        Assert.Equal(profile.Id, decoded.Id);
        Assert.Equal(profile.Name, decoded.Name);
        Assert.Equal(profile.Roles, decoded.Roles);
    }

    /// <summary>Round-trips structured payloads through CacheValue.payload on the server wire path.</summary>
    [Fact]
    public void StructuredPayloadRoundTripsThroughCacheValuePayload()
    {
        var profile = new WireTestProfile(7, "alice", ["admin"]);
        var bytes = CacheEntryCodec.EncodeWireValueToOwned(profile);
        var grpcValue = CacheWireCodec.ToCacheValue(profile);
        Assert.Equal(CacheValue.KindOneofCase.Payload, grpcValue.KindCase);
        Assert.Equal(bytes, grpcValue.Payload.ToByteArray());

        var decoded = CacheWireCodec.FromCacheValue<WireTestProfile>(grpcValue);
        Assert.NotNull(decoded);
        Assert.Equal(profile.Id, decoded.Id);
        Assert.Equal(profile.Name, decoded.Name);
        Assert.Equal(profile.Roles, decoded.Roles);
    }

    private static WireTestMutableCart CreateMutableCart() => new()
    {
        Id = "cart-1",
        CouponCode = "SAVE10",
        Total = 32.25m,
        UpdatedAt = new DateTimeOffset(2026, 6, 6, 9, 15, 30, TimeSpan.Zero),
        Items =
        [
            new WireTestCartItem { Sku = "SKU-001", Quantity = 2, Price = 12.50m },
            new WireTestCartItem { Sku = "SKU-002", Quantity = 1, Price = 7.25m },
        ],
    };

    private sealed class WireTestCartItem
    {
        public decimal Price { get; init; }

        public int Quantity { get; init; }

        public string Sku { get; init; } = string.Empty;
    }

    private sealed class WireTestMutableCart
    {
        public string? CouponCode { get; init; }

        public string Id { get; init; } = string.Empty;

        public List<WireTestCartItem> Items { get; init; } = [];

        public decimal Total { get; init; }

        public DateTimeOffset UpdatedAt { get; init; }
    }

    private sealed record WireTestProfile(long Id, string Name, string[] Roles);
}
