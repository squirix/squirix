using System;
using System.Text.Json;
using System.Threading.Tasks;
using Google.Protobuf.WellKnownTypes;
using Squirix.Server.Core;
using Squirix.Server.Utils;
using Squirix.Transport.Grpc.Cache;
using Xunit;

namespace Squirix.Server.UnitTests;

/// <summary>Tests compact <see cref="CacheValue" /> gRPC scalar mapping.</summary>
public sealed class CacheValueGrpcMappingTests
{
    /// <summary>Complex object payloads round-trip through MapToProto / MapFromProto.</summary>
    [Fact]
    public async Task ComplexObjectRoundTripsThroughEntryMapping()
    {
        var source = new NodeCacheEntry<SamplePayload>
        {
            Value = new SamplePayload { Id = 7, Name = "alpha", Tags = ["a", "b"] },
            Version = 3,
            ExpiresUtc = new DateTime(2026, 8, 1, 12, 0, 0, DateTimeKind.Utc),
            Expiration = TimeSpan.FromMinutes(5),
        };

        var wire = source.MapToProto();
        var roundTrip = await wire.MapFromProtoAsync<SamplePayload>();

        Assert.NotNull(roundTrip.Value);
        Assert.Equal(7, roundTrip.Value.Id);
        Assert.Equal("alpha", roundTrip.Value.Name);
        Assert.Equal(2, roundTrip.Value.Tags.Length);
        Assert.Equal("a", roundTrip.Value.Tags[0]);
        Assert.Equal("b", roundTrip.Value.Tags[1]);
        Assert.Equal(source.ExpiresUtc, roundTrip.ExpiresUtc);
        Assert.Equal(source.Expiration, roundTrip.Expiration);
    }

    /// <summary>Exact primitive wire forms decode without struct wrapping.</summary>
    /// <param name="kind">Wire kind under test.</param>
    [Theory]
    [InlineData("string")]
    [InlineData("bool")]
    [InlineData("long")]
    [InlineData("double")]
    [InlineData("null")]
    public async Task ExactPrimitiveWireFormsDecode(string kind)
    {
        var wire = kind switch
        {
            "string" => new CacheValue { StringValue = "hello" },
            "bool" => new CacheValue { BoolValue = true },
            "long" => new CacheValue { Int64Value = 99L },
            "double" => new CacheValue { DoubleValue = 1.5d },
            _ => new CacheValue { NullValue = NullValue.NullValue },
        };

        switch (kind)
        {
            case "string":
                Assert.Equal("hello", await ServerProtoEx.MapCacheValueAsync<string>(wire));
                break;
            case "bool":
                Assert.True(await ServerProtoEx.MapCacheValueAsync<bool>(wire));
                break;
            case "long":
                Assert.Equal(99L, await ServerProtoEx.MapCacheValueAsync<long>(wire));
                break;
            case "double":
                Assert.Equal(1.5d, await ServerProtoEx.MapCacheValueAsync<double>(wire));
                break;
            default:
                Assert.Null(await ServerProtoEx.MapCacheValueAsync<string>(wire));
                break;
        }
    }

    /// <summary>CLR <see cref="int" /> values use the dedicated int32 wire arm.</summary>
    [Fact]
    public void Int32EncodesAsInt32ValueWireForm()
    {
        var wire = ServerProtoEx.CacheValueToGrpcValue(42);

        Assert.Equal(CacheValue.KindOneofCase.Int32Value, wire.KindCase);
        Assert.Equal(42, wire.Int32Value);
    }

    /// <summary>int32 wire values decode to typed <see cref="int" /> reads.</summary>
    [Fact]
    public async Task Int32ValueRoundTripsAsInt()
    {
        var wire = new CacheValue { Int32Value = 7 };

        Assert.Equal(7, await ServerProtoEx.MapCacheValueAsync<int>(wire));
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

    /// <summary>JsonElement payloads encode and decode through struct mapping.</summary>
    [Fact]
    public async Task JsonElementPayloadRoundTripsThroughEntryMapping()
    {
        using var document = JsonDocument.Parse("""{"n":1,"ok":true,"items":[1,2]}""");
        var source = new NodeCacheEntry<JsonElement>
        {
            Value = document.RootElement.Clone(),
            Version = 1,
        };

        var wire = source.MapToProto();
        var roundTrip = await wire.MapFromProtoAsync<JsonElement>();

        Assert.Equal(1, roundTrip.Value.GetProperty("n").GetInt32());
        Assert.True(roundTrip.Value.GetProperty("ok").GetBoolean());
        var items = roundTrip.Value.GetProperty("items");
        Assert.Equal(2, items.GetArrayLength());
        Assert.Equal(1, items[0].GetInt32());
        Assert.Equal(2, items[1].GetInt32());
    }

    /// <summary>Non-object JsonElement values encode through the single-field envelope.</summary>
    /// <param name="json">JSON literal under test.</param>
    [Theory]
    [InlineData("\"text\"")]
    [InlineData("true")]
    [InlineData("false")]
    [InlineData("null")]
    [InlineData("12")]
    [InlineData("1.25")]
    [InlineData("[1,{\"k\":2},null]")]
    public async Task NonObjectJsonElementRoundTripsThroughEntryMapping(string json)
    {
        using var document = JsonDocument.Parse(json);
        var source = new NodeCacheEntry<JsonElement> { Value = document.RootElement.Clone(), Version = 1 };

        var wire = source.MapToProto();
        var roundTrip = await wire.MapFromProtoAsync<JsonElement>();

        Assert.Equal(document.RootElement.ToString(), roundTrip.Value.ToString());
    }

    /// <summary>Primitive wire with a mismatched CLR type falls back through struct wrapping.</summary>
    [Fact]
    public async Task MismatchedPrimitiveWireFallsBackThroughStructWrap()
    {
        Assert.Equal(42, await ServerProtoEx.MapCacheValueAsync<int>(new CacheValue { Int64Value = 42L }));
        Assert.Equal(7, await ServerProtoEx.MapCacheValueAsync<int>(new CacheValue { Int32Value = 7 }));
        Assert.Equal("x", await ServerProtoEx.MapCacheValueAsync<string>(new CacheValue { StringValue = "x" }));
        Assert.True(await ServerProtoEx.MapCacheValueAsync<bool>(new CacheValue { BoolValue = true }));
        Assert.Equal(1.5d, await ServerProtoEx.MapCacheValueAsync<double>(new CacheValue { DoubleValue = 1.5d }));
    }

    /// <summary>Object-typed reads cover every compact wire kind.</summary>
    /// <param name="kind">Wire kind under test.</param>
    [Theory]
    [InlineData("string")]
    [InlineData("bool")]
    [InlineData("int32")]
    [InlineData("int64")]
    [InlineData("double")]
    [InlineData("null")]
    [InlineData("none")]
    public async Task ObjectTypedReadsCoverWireKinds(string kind)
    {
        var wire = kind switch
        {
            "string" => new CacheValue { StringValue = "obj" },
            "bool" => new CacheValue { BoolValue = true },
            "int32" => new CacheValue { Int32Value = 3 },
            "int64" => new CacheValue { Int64Value = 9L },
            "double" => new CacheValue { DoubleValue = 2.5d },
            "null" => new CacheValue { NullValue = NullValue.NullValue },
            _ => new CacheValue(),
        };

        var mapped = await ServerProtoEx.MapCacheValueAsync<object>(wire);
        switch (kind)
        {
            case "string":
                Assert.Equal("obj", mapped);
                break;
            case "bool":
                Assert.True(Assert.IsType<bool>(mapped));
                break;
            case "int32":
                Assert.Equal(3, mapped);
                break;
            case "int64":
                Assert.Equal(9L, mapped);
                break;
            case "double":
                Assert.Equal(2.5d, mapped);
                break;
            default:
                Assert.Null(mapped);
                break;
        }
    }

    /// <summary>Compact value encoding covers every CLR primitive arm.</summary>
    [Fact]
    public void CacheValueToGrpcValueCoversPrimitiveArms()
    {
        Assert.Equal(CacheValue.KindOneofCase.NullValue, ServerProtoEx.CacheValueToGrpcValue<string>(null).KindCase);
        Assert.Equal("s", ServerProtoEx.CacheValueToGrpcValue("s").StringValue);
        Assert.True(ServerProtoEx.CacheValueToGrpcValue(true).BoolValue);
        Assert.Equal(1.25d, ServerProtoEx.CacheValueToGrpcValue(1.25d).DoubleValue);
        Assert.Equal(CacheValue.KindOneofCase.StructValue, ServerProtoEx.CacheValueToGrpcValue(new SamplePayload { Id = 1, Name = "n" }).KindCase);
    }

    /// <summary>Entry mapping encodes and decodes primitive CLR values through the struct envelope.</summary>
    /// <param name="kind">Primitive kind under test.</param>
    [Theory]
    [InlineData("null")]
    [InlineData("string")]
    [InlineData("int")]
    [InlineData("long")]
    [InlineData("double")]
    [InlineData("bool")]
    public async Task PrimitiveEntryValuesRoundTripThroughStructEnvelope(string kind)
    {
        switch (kind)
        {
            case "null":
            {
                var wire = new NodeCacheEntry<object?> { Value = null, Version = 1 }.MapToProto();
                var roundTrip = await wire.MapFromProtoAsync<object?>();
                Assert.Null(roundTrip.Value);
                break;
            }

            case "string":
            {
                var wire = new NodeCacheEntry<string> { Value = "text", Version = 1 }.MapToProto();
                Assert.Equal("text", (await wire.MapFromProtoAsync<string>()).Value);
                break;
            }

            case "int":
            {
                var wire = new NodeCacheEntry<int> { Value = 11, Version = 1 }.MapToProto();
                Assert.Equal(11, (await wire.MapFromProtoAsync<int>()).Value);
                break;
            }

            case "long":
            {
                var wire = new NodeCacheEntry<long> { Value = 12L, Version = 1 }.MapToProto();
                Assert.Equal(12L, (await wire.MapFromProtoAsync<long>()).Value);
                break;
            }

            case "double":
            {
                var wire = new NodeCacheEntry<double> { Value = 3.5d, Version = 1 }.MapToProto();
                Assert.Equal(3.5d, (await wire.MapFromProtoAsync<double>()).Value);
                break;
            }

            default:
            {
                var wire = new NodeCacheEntry<bool> { Value = true, Version = 1 }.MapToProto();
                Assert.True((await wire.MapFromProtoAsync<bool>()).Value);
                break;
            }
        }
    }

    /// <summary>Multi-field structs deserialize for object and typed targets.</summary>
    [Fact]
    public async Task MultiFieldStructDeserializesForObjectAndTyped()
    {
        var multi = new Struct
        {
            Fields =
            {
                ["Id"] = Value.ForNumber(5),
                ["Name"] = Value.ForString("multi"),
                ["Tags"] = new Value { ListValue = new ListValue { Values = { Value.ForString("t") } } },
            },
        };
        var wire = new CacheValue { StructValue = multi };

        var asObject = Assert.IsType<JsonElement>(await ServerProtoEx.MapCacheValueAsync<object>(wire));
        Assert.Equal(5, asObject.GetProperty("Id").GetInt32());
        Assert.Equal("multi", asObject.GetProperty("Name").GetString());

        var typed = await ServerProtoEx.MapCacheValueAsync<SamplePayload>(wire);
        Assert.NotNull(typed);
        Assert.Equal(5, typed.Id);
        Assert.Equal("multi", typed.Name);
        Assert.Equal(["t"], typed.Tags);
    }

    /// <summary>Wrapped list and nested struct values decode for object targets.</summary>
    [Fact]
    public async Task WrappedListAndStructValuesDecodeAsJsonElement()
    {
        var listWire = new CacheValue
        {
            StructValue = new Struct
            {
                Fields =
                {
                    ["value"] = new Value
                    {
                        ListValue = new ListValue
                        {
                            Values =
                            {
                                Value.ForNumber(1),
                                Value.ForBool(false),
                                Value.ForNull(),
                            },
                        },
                    },
                },
            },
        };
        var listElement = Assert.IsType<JsonElement>(await ServerProtoEx.MapCacheValueAsync<object>(listWire));
        Assert.Equal(JsonValueKind.Array, listElement.ValueKind);
        Assert.Equal(3, listElement.GetArrayLength());

        var structWire = new CacheValue
        {
            StructValue = new Struct
            {
                Fields =
                {
                    ["value"] = Value.ForStruct(new Struct { Fields = { ["inner"] = Value.ForString("x") } }),
                },
            },
        };
        var structElement = Assert.IsType<JsonElement>(await ServerProtoEx.MapCacheValueAsync<object>(structWire));
        Assert.Equal("x", structElement.GetProperty("inner").GetString());
    }

    /// <summary>Wrapped protobuf null and unset values decode as null for object targets.</summary>
    [Fact]
    public async Task WrappedNullAndUnsetValuesDecodeAsNull()
    {
        var nullWire = new CacheValue
        {
            StructValue = new Struct { Fields = { ["value"] = Value.ForNull() } },
        };
        Assert.Null(await ServerProtoEx.MapCacheValueAsync<object>(nullWire));

        var unsetWire = new CacheValue
        {
            StructValue = new Struct { Fields = { ["value"] = new Value() } },
        };
        Assert.Null(await ServerProtoEx.MapCacheValueAsync<object>(unsetWire));
    }

    /// <summary>Struct-wrapped values decode for object and typed targets.</summary>
    [Fact]
    public async Task StructWrappedValuesDecode()
    {
        var wire = new CacheValue
        {
            StructValue = new Struct
            {
                Fields = { ["value"] = Value.ForString("wrapped") },
            },
        };

        Assert.Equal("wrapped", await ServerProtoEx.MapCacheValueAsync<string>(wire));
        Assert.Equal("wrapped", await ServerProtoEx.MapCacheValueAsync<object>(wire));
        var numberWire = new CacheValue { StructValue = new Struct { Fields = { ["value"] = Value.ForNumber(1.5d) } } };
        Assert.Equal(1.5d, await ServerProtoEx.MapCacheValueAsync<double>(numberWire));
        var boolWire = new CacheValue { StructValue = new Struct { Fields = { ["value"] = Value.ForBool(true) } } };
        Assert.True(await ServerProtoEx.MapCacheValueAsync<bool>(boolWire));
    }

    /// <summary>MapFromProto ignores zero timestamps and preserves relative expiration.</summary>
    [Fact]
    public async Task MapFromProtoIgnoresZeroTimestampAndKeepsExpiration()
    {
        var wire = new CacheEntryWire
        {
            Value = new Struct { Fields = { ["value"] = Value.ForString("exp") } },
            ExpiresUtc = new Timestamp { Seconds = 0, Nanos = 0 },
            Expiration = Duration.FromTimeSpan(TimeSpan.FromSeconds(9)),
        };

        var entry = await wire.MapFromProtoAsync<string>();
        Assert.Equal("exp", entry.Value);
        Assert.Null(entry.ExpiresUtc);
        Assert.Equal(TimeSpan.FromSeconds(9), entry.Expiration);
    }

    /// <summary>Unset KindCase maps to the typed default.</summary>
    [Fact]
    public async Task UnsetKindCaseReturnsTypedDefault()
    {
        var wire = new CacheValue();

        Assert.Equal(0, await ServerProtoEx.MapCacheValueAsync<int>(wire));
        Assert.Null(await ServerProtoEx.MapCacheValueAsync<string>(wire));
    }

    private sealed class SamplePayload
    {
        public int Id { get; init; }

        public string Name { get; init; } = string.Empty;

        public string[] Tags { get; init; } = [];
    }
}
