using System.Threading.Tasks;
using Google.Protobuf.WellKnownTypes;
using Squirix.Internal;
using Squirix.Transport.Grpc.Cache;
using Xunit;

namespace Squirix.UnitTests.Internal;

/// <summary>Covers ProtoEx object mapping including ToUntypedValueAsync switch arms.</summary>
public sealed class ProtoExTests
{
    /// <summary>Struct-wrapped protobuf values deserialize to untyped objects.</summary>
    /// <param name="kind">Value kind to wrap.</param>
    [Theory]
    [InlineData("string")]
    [InlineData("bool")]
    [InlineData("number")]
    [InlineData("null")]
    public async Task FromCacheValueAsyncObjectReadsWrappedValues(string kind)
    {
        var serializer = new SystemTextJsonSerializer();
        var wrapped = kind switch
        {
            "string" => Value.ForString("hello"),
            "bool" => Value.ForBool(true),
            "number" => Value.ForNumber(3.5),
            _ => Value.ForNull(),
        };

        var cacheValue = new CacheValue
        {
            StructValue = new Struct
            {
                Fields = { ["value"] = wrapped },
            },
        };

        var result = await ProtoEx.FromCacheValueAsync<object>(cacheValue, serializer);
        switch (kind)
        {
            case "string":
                Assert.Equal("hello", result);
                break;
            case "bool":
                Assert.True(Assert.IsType<bool>(result));
                break;
            case "number":
                Assert.Equal(3.5d, result);
                break;
            default:
                Assert.Null(result);
                break;
        }
    }
}
