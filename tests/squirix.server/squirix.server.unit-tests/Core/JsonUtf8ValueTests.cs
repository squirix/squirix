using System;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.Core;
using Squirix.Server.TestKit;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Core;

/// <summary>Tests for <see cref="JsonUtf8Value" /> and how entries normalize to it.</summary>
[Immutable]
public sealed class JsonUtf8ValueTests : ServerUnitTestBase
{
    private const string ObjectText = """{"id":7,"name":"n"}""";

    /// <summary>Wrapping owned bytes keeps the very array and does not copy it.</summary>
    [Test]
    public async Task OwnedBytesAreWrappedWithoutCopy()
    {
        var bytes = Encoding.UTF8.GetBytes(ObjectText);

        _ = await Assert.That(JsonUtf8Value.TryCreate(bytes, out var value)).IsTrue();

        _ = await Assert.That(value!.Bytes.Span.Overlaps(bytes, out var offset)).IsTrue();
        _ = await Assert.That(offset).IsEqualTo(0);
    }

    /// <summary>Copying detaches the value from a buffer that is overwritten later.</summary>
    [Test]
    public async Task CopiedBytesSurviveSourceOverwrite()
    {
        var bytes = Encoding.UTF8.GetBytes(ObjectText);
        _ = await Assert.That(JsonUtf8Value.TryCreateCopy(bytes, out var value)).IsTrue();

        bytes.AsSpan().Clear();

        _ = await Assert.That(Encoding.UTF8.GetString(value!.Bytes.Span)).IsEqualTo(ObjectText);
    }

    /// <summary>Bytes that are not one JSON value do not produce a value.</summary>
    [Test]
    public async Task InvalidBytesProduceNoValue()
    {
        var bytes = Encoding.UTF8.GetBytes("{\"a\":1} {}");

        _ = await Assert.That(JsonUtf8Value.TryCreate(bytes, out var owned)).IsFalse();
        _ = await Assert.That(owned).IsNull();
        _ = await Assert.That(JsonUtf8Value.TryCreateCopy(bytes, out var copied)).IsFalse();
        _ = await Assert.That(copied).IsNull();
    }

    /// <summary>The element of a value is self-contained and has the text of the bytes.</summary>
    [Test]
    public async Task ElementHasTheTextOfTheBytes()
    {
        var element = JsonValueKit.Create(ObjectText).ToJsonElement();

        _ = await Assert.That(element.GetRawText()).IsEqualTo(ObjectText);
        _ = await Assert.That(element.GetProperty("id").GetInt32()).IsEqualTo(7);
    }

    /// <summary>Normalizing an object, array or number element copies its text into a value, and the copy does not follow the document.</summary>
    /// <param name="text">The JSON text of the element.</param>
    [Test]
    [Arguments(ObjectText)]
    [Arguments("[1,2,3]")]
    [Arguments("12345678901234567890")]
    public async Task StructuredElementNormalizesToValue(string text)
    {
        var element = JsonElement.Parse(text);

        var normalized = NodeCacheEntry<object?>.NormalizeValue(element);

        var value = (await Assert.That(normalized).IsTypeOf<JsonUtf8Value>())!;
        _ = await Assert.That(Encoding.UTF8.GetString(value.Bytes.Span)).IsEqualTo(text);
    }

    /// <summary>Normalizing a scalar element gives the primitive it holds.</summary>
    [Test]
    public async Task ScalarElementNormalizesToPrimitive()
    {
        _ = await Assert.That(NodeCacheEntry<object?>.NormalizeValue(JsonElement.Parse("\"s\""))).IsEqualTo("s");
        _ = await Assert.That(NodeCacheEntry<object?>.NormalizeValue(JsonElement.Parse("true")) is true).IsTrue();
        _ = await Assert.That(NodeCacheEntry<object?>.NormalizeValue(JsonElement.Parse("false")) is false).IsTrue();
        _ = await Assert.That(NodeCacheEntry<object?>.NormalizeValue(JsonElement.Parse("null"))).IsNull();
        _ = await Assert.That(NodeCacheEntry<object?>.NormalizeValue(default(JsonElement))).IsNull();
    }

    /// <summary>A value that is already JSON bytes normalizes to itself, never to a copy.</summary>
    [Test]
    public async Task ValueNormalizesToItself()
    {
        var value = JsonValueKit.Create(ObjectText);

        _ = await Assert.That(NodeCacheEntry<object?>.NormalizeValue(value)).IsSameReferenceAs(value);
    }

    /// <summary>A serializer that produces something other than one JSON value cannot reach storage.</summary>
    [Test]
    public void FaultySerializerOutputIsRefused() =>
        _ = NodeExceptionAssert.For<InvalidOperationException>().Throws(Encoding.UTF8.GetBytes("{}{}"), static bytes => _ = JsonUtf8Value.FromSerialized(bytes));
}
