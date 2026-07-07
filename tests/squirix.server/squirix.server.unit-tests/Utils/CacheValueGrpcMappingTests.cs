using System.Threading.Tasks;
using Squirix.Server.Utils;
using Squirix.Transport.Grpc.Cache;
using Xunit;

namespace Squirix.Server.UnitTests.Utils;

/// <summary>Tests compact <see cref="CacheValue" /> gRPC scalar mapping.</summary>
public sealed class CacheValueGrpcMappingTests
{
    /// <summary>CLR <see cref="int" /> values use the dedicated int32 wire arm.</summary>
    [Fact]
    public void Int32EncodesAsInt32ValueWireForm()
    {
        var wire = ServerProtoEx.CacheValueToGrpcValue(42);

        Assert.Equal(CacheValue.KindOneofCase.Int32Value, wire.KindCase);
        Assert.Equal(42, wire.Int32Value);
    }

    /// <summary>CLR <see cref="long" /> values outside int32 range keep the int64 wire arm.</summary>
    [Fact]
    public void Int64EncodesAsInt64ValueWireForm()
    {
        const long value = int.MaxValue + 1L;
        var wire = ServerProtoEx.CacheValueToGrpcValue(value);

        Assert.Equal(CacheValue.KindOneofCase.Int64Value, wire.KindCase);
        Assert.Equal(value, wire.Int64Value);
    }

    /// <summary>int32 wire values decode to typed <see cref="int" /> reads.</summary>
    [Fact]
    public async Task Int32ValueRoundTripsAsInt()
    {
        var wire = new CacheValue { Int32Value = 7 };

        Assert.Equal(7, await ServerProtoEx.MapCacheValueAsync<int>(wire));
    }
}
