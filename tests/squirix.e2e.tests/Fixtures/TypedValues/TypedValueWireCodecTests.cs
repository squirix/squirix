using Squirix.Internal.Cluster.Transport.Binary;
using Squirix.Serialization;
using Xunit;

namespace Squirix.E2ETests.Fixtures.TypedValues;

/// <summary>In-process wire codec checks for E2E typed value fixtures.</summary>
public sealed class TypedValueWireCodecTests
{
    /// <summary>E2E cart fixtures round-trip through the client metadata wire codec.</summary>
    [Fact]
    public void MutableCartFixtureShouldRoundTripThroughClientWireCodec()
    {
        var serializer = new SystemTextJsonSerializer();
        var expected = TypedValueFactory.CreateCart("cart");

        var bytes = CacheValueWireCodec.EncodeWireValueToOwned(expected, serializer);
        Assert.True(CacheValueWireCodec.TryReadWireValue(bytes, serializer, out TypedMutableCart? roundTrip));
        Assert.NotNull(roundTrip);
        TypedValueAssertions.AssertCartEquals(expected, roundTrip);
    }
}
