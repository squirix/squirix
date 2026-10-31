using System;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.Core;
using Squirix.Server.Serialization.Grpc;
using Squirix.Server.UnitTests.Support;
using Squirix.Transport.Grpc.Cache;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests;

/// <summary>Maps stored cache values to their wire form.</summary>
[Immutable]
public sealed class CacheValueGrpcMappingTests : ServerUnitTestBase
{
    /// <summary>Every scalar maps to its own kind, and an integer of any width travels as an int64.</summary>
    [Test]
    public async Task ScalarsMapToNativeKinds()
    {
        _ = await Assert.That(ServerProtoEx.CacheValueToGrpcValue<string>(null).KindCase).IsEqualTo(CacheValue.KindOneofCase.NullValue);
        _ = await Assert.That(ServerProtoEx.CacheValueToGrpcValue("s").StringValue).IsEqualTo("s");
        _ = await Assert.That(ServerProtoEx.CacheValueToGrpcValue(true).BoolValue).IsTrue();
        _ = await Assert.That(ServerProtoEx.CacheValueToGrpcValue(1.25d).DoubleValue).IsEqualTo(1.25d);
        _ = await Assert.That(ServerProtoEx.CacheValueToGrpcValue(1.5f).DoubleValue).IsEqualTo(1.5d);
        _ = await Assert.That(ServerProtoEx.CacheValueToGrpcValue(42).Int64Value).IsEqualTo(42L);
        _ = await Assert.That(ServerProtoEx.CacheValueToGrpcValue(long.MaxValue).Int64Value).IsEqualTo(long.MaxValue);
        _ = await Assert.That(ServerProtoEx.CacheValueToGrpcValue<short>(-3).Int64Value).IsEqualTo(-3L);
        _ = await Assert.That(ServerProtoEx.CacheValueToGrpcValue(uint.MaxValue).Int64Value).IsEqualTo(uint.MaxValue);
        _ = await Assert.That(ServerProtoEx.CacheValueToGrpcValue<byte>(7).Int64Value).IsEqualTo(7L);
    }

    /// <summary>Default-valued value types are scalars, not null.</summary>
    [Test]
    public async Task DefaultedValueTypesAreNotNull()
    {
        _ = await Assert.That(new NodeCacheEntry<int> { Value = 0, Version = 1 }.MapToProto().Value.KindCase).IsEqualTo(CacheValue.KindOneofCase.Int64Value);
        _ = await Assert.That(new NodeCacheEntry<bool> { Value = false, Version = 1 }.MapToProto().Value.KindCase).IsEqualTo(CacheValue.KindOneofCase.BoolValue);
        _ = await Assert.That(new NodeCacheEntry<double> { Value = 0.0, Version = 1 }.MapToProto().Value.KindCase).IsEqualTo(CacheValue.KindOneofCase.DoubleValue);
    }

    /// <summary>A stored JSON value goes to the wire as the very bytes it holds, not as a copy.</summary>
    [Test]
    public async Task StoredJsonBytesAreSharedWithTheWire()
    {
        var stored = JsonValueKit.Create("""{"id":1}""");

        var wire = ServerProtoEx.CacheValueToGrpcValue<object?>(stored);

        _ = await Assert.That(wire.KindCase).IsEqualTo(CacheValue.KindOneofCase.JsonUtf8);
        _ = await Assert.That(wire.JsonUtf8.Memory.Span.Overlaps(stored.Bytes.Span, out var offset)).IsTrue();
        _ = await Assert.That(offset).IsEqualTo(0);
        _ = await Assert.That(wire.JsonUtf8.Length).IsEqualTo(stored.Length);
    }

    /// <summary>A scalar element maps to its native kind, and a number, an object or an array to its own text.</summary>
    /// <param name="text">The JSON text of the element.</param>
    /// <param name="kind">The expected wire kind.</param>
    [Test]
    [Arguments("\"s\"", "StringValue")]
    [Arguments("true", "BoolValue")]
    [Arguments("false", "BoolValue")]
    [Arguments("null", "NullValue")]
    [Arguments("12345678901234567890", "JsonUtf8")]
    [Arguments("1.25", "JsonUtf8")]
    [Arguments("[1,{\"k\":2},null]", "JsonUtf8")]
    [Arguments("{\"n\":1}", "JsonUtf8")]
    public async Task ElementsMapByKind(string text, string kind)
    {
        var wire = ServerProtoEx.CacheValueToGrpcValue(JsonElement.Parse(text));

        _ = await Assert.That(wire.KindCase).IsEqualTo(System.Enum.Parse<CacheValue.KindOneofCase>(kind));
        _ = await Assert.That(!string.Equals(kind, nameof(CacheValue.KindOneofCase.JsonUtf8), StringComparison.Ordinal) || string.Equals(wire.JsonUtf8.ToStringUtf8(), text, StringComparison.Ordinal)).IsTrue();
    }

    /// <summary>An element that is the default value maps to null.</summary>
    [Test]
    public async Task UndefinedElementMapsToNull()
    {
        var wire = ServerProtoEx.CacheValueToGrpcValue(default(JsonElement));

        _ = await Assert.That(wire.KindCase).IsEqualTo(CacheValue.KindOneofCase.NullValue);
    }

    /// <summary>An arbitrary object is serialized to JSON text.</summary>
    [Test]
    public async Task ObjectSerializesToJsonKind()
    {
        var wire = ServerProtoEx.CacheValueToGrpcValue(new SamplePayload { Id = 1, Name = "n" });

        _ = await Assert.That(wire.KindCase).IsEqualTo(CacheValue.KindOneofCase.JsonUtf8);
        using var document = JsonDocument.Parse(wire.JsonUtf8.Memory);
        _ = await Assert.That(document.RootElement.GetProperty("Id").GetInt32()).IsEqualTo(1);
    }

    /// <summary>A decimal keeps every digit on the wire.</summary>
    [Test]
    public async Task DecimalKeepsAllDigits()
    {
        var wire = ServerProtoEx.CacheValueToGrpcValue(12345678901234567.123456789m);

        _ = await Assert.That(wire.KindCase).IsEqualTo(CacheValue.KindOneofCase.JsonUtf8);
        _ = await Assert.That(Encoding.UTF8.GetString(wire.JsonUtf8.Span)).IsEqualTo("12345678901234567.123456789");
    }

    /// <summary>An entry maps its value and both deadlines.</summary>
    [Test]
    public async Task EntryMapsValueAndDeadlines()
    {
        var source = new NodeCacheEntry<object?>
        {
            Value = "text",
            Version = 3,
            ExpiresUtc = new DateTime(2026, 8, 1, 12, 0, 0, DateTimeKind.Utc),
            Expiration = TimeSpan.FromMinutes(5),
        };

        var wire = source.MapToProto();

        _ = await Assert.That(wire.Value.StringValue).IsEqualTo("text");
        _ = await Assert.That(wire.ExpiresUtc.ToDateTime()).IsEqualTo(source.ExpiresUtc.Value);
        _ = await Assert.That(wire.Expiration.ToTimeSpan()).IsEqualTo(TimeSpan.FromMinutes(5));
    }

    /// <summary>An entry without deadlines maps none.</summary>
    [Test]
    public async Task EntryWithoutDeadlinesMapsNone()
    {
        var wire = new NodeCacheEntry<object?> { Value = 1, Version = 1 }.MapToProto();

        _ = await Assert.That(wire.ExpiresUtc).IsNull();
        _ = await Assert.That(wire.Expiration).IsNull();
    }

    /// <summary>An element parsed with comments and trailing commas maps to strict JSON text.</summary>
    [Test]
    public async Task LenientElementMapsToStrictText()
    {
        var element = JsonElement.Parse("[1, /* note */ 2,]", new JsonDocumentOptions { CommentHandling = JsonCommentHandling.Skip, AllowTrailingCommas = true });

        var wire = ServerProtoEx.CacheValueToGrpcValue(element);

        _ = await Assert.That(wire.KindCase).IsEqualTo(CacheValue.KindOneofCase.JsonUtf8);
        _ = await Assert.That(wire.JsonUtf8.ToStringUtf8()).IsEqualTo("[1,2]");
        _ = await Assert.That(JsonUtf8Validator.IsValid(wire.JsonUtf8.Span)).IsTrue();
    }
}
