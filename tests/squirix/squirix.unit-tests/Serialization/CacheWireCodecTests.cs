using System;
using Squirix.Internal.Cluster.Transport;
using Squirix.Internal.Cluster.Transport.Binary;
using Squirix.Serialization;
using Squirix.Transport.Grpc.Cache;
using Xunit;

namespace Squirix.UnitTests.Serialization;

/// <summary>Binary wire codec round-trip tests on the client adapter path.</summary>
public sealed class CacheWireCodecTests
{
    private static readonly byte[] Int42WireBytes = [ValueKind.Int64, 42, 0, 0, 0, 0, 0, 0, 0];
    private static readonly ISquirixSerializer Serializer = new SystemTextJsonSerializer();

    /// <summary>Round-trips DateTimeOffset as Unix milliseconds without a JsonElement bridge.</summary>
    [Fact]
    public void DateTimeOffsetRoundTripsAsUnixMilliseconds()
    {
        var instant = new DateTimeOffset(2026, 6, 6, 12, 0, 0, TimeSpan.Zero);
        var bytes = CacheValueWireCodec.EncodeWireValueToOwned(instant, Serializer);
        Assert.Equal(ValueKind.Int64, bytes[0]);
        Assert.True(CacheValueWireCodec.TryReadWireValue(bytes, Serializer, out DateTimeOffset decoded));
        Assert.Equal(instant, decoded);
    }

    /// <summary>Encodes int entry payloads as the shared binary int64 wire blob.</summary>
    [Fact]
    public void EntryWireEncodesIntAsBinaryPayload()
    {
        var bytes = CacheValueWireCodec.EncodeWireValueToOwned(42, Serializer);
        Assert.Equal(Int42WireBytes, bytes);
    }

    /// <summary>Round-trips structured payloads through CacheValue.payload on the client wire path.</summary>
    [Fact]
    public void StructuredPayloadRoundTripsThroughCacheValuePayload()
    {
        var profile = new WireTestProfile(7, "alice", ["admin"]);
        var bytes = CacheValueWireCodec.EncodeWireValueToOwned(profile, Serializer);
        var grpcValue = CacheWireCodec.ToCacheValue(profile, Serializer);
        Assert.Equal(CacheValue.KindOneofCase.Payload, grpcValue.KindCase);
        Assert.Equal(bytes, grpcValue.Payload.ToByteArray());

        var decoded = CacheWireCodec.FromCacheValue<WireTestProfile>(grpcValue, Serializer);
        Assert.NotNull(decoded);
        Assert.Equal(profile.Id, decoded.Id);
        Assert.Equal(profile.Name, decoded.Name);
        Assert.Equal(profile.Roles, decoded.Roles);
    }

    private sealed record WireTestProfile(long Id, string Name, string[] Roles);
}
