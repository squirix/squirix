using System;
using System.IO;
using System.Text.Json;
using FakeItEasy;
using Squirix.Serialization;
using Squirix.TestKit;
using Xunit;

namespace Squirix.UnitTests.Serialization;

/// <summary>
/// Tests for <see cref="MetricsDecoratedSerializer" /> metric emission and delegation semantics.
/// </summary>
public sealed class MetricsDecoratedSerializerTests
{
    /// <summary>
    /// Verifies the decorator forwards to the inner serializer without changing serialized bytes or deserialized values.
    /// </summary>
    [Fact]
    public void DecoratedSerializerMatchesInnerPayloadAndValue()
    {
        var inner = new SystemTextJsonSerializer();
        var decorated = new MetricsDecoratedSerializer(inner);
        var model = new { Z = 42 };

        var expectedBytes = inner.SerializeToUtf8Bytes(model);
        var actualBytes = decorated.SerializeToUtf8Bytes(model);

        Assert.Equal(expectedBytes, actualBytes);

        var expectedElement = inner.Deserialize<JsonElement>(expectedBytes);
        var actualElement = decorated.Deserialize<JsonElement>(expectedBytes);

        Assert.Equal(expectedElement.GetRawText(), actualElement.GetRawText());
    }

    /// <summary>
    /// Verifies deserialize failure records error ops, failure counter, and preserves the thrown exception type.
    /// </summary>
    [Fact]
    public void DeserializeFailureRecordsErrorMetrics()
    {
        using var sink = new MeasurementSink("Squirix");
        var inner = new SystemTextJsonSerializer();
        var decorated = new MetricsDecoratedSerializer(inner);

        _ = Assert.Throws<JsonException>(() => decorated.Deserialize<int>("not-json"));

        const string impl = nameof(SystemTextJsonSerializer);
        Assert.True(sink.HasEvent("squirix_serializer_ops_total", ("op", "deserialize"), ("result", "error"), ("impl", impl)));
        Assert.True(sink.HasEvent("squirix_serializer_failures_total", ("op", "deserialize"), ("exception_type", "JsonException"), ("impl", impl)));
    }

    /// <summary>
    /// Verifies successful serialize and deserialize paths record ok counters and durations.
    /// </summary>
    [Fact]
    public void SerializeAndDeserializeSuccessRecordMetrics()
    {
        using var sink = new MeasurementSink("Squirix");
        var inner = new SystemTextJsonSerializer();
        var decorated = new MetricsDecoratedSerializer(inner);
        var payload = new { A = 1, B = "x" };

        var bytes = decorated.SerializeToUtf8Bytes(payload);
        var element = decorated.Deserialize<JsonElement>(bytes);

        Assert.Equal(1, element.GetProperty("a").GetInt32());
        Assert.Equal("x", element.GetProperty("b").GetString());

        const string impl = nameof(SystemTextJsonSerializer);
        Assert.True(sink.HasEvent("squirix_serializer_ops_total", ("op", "serialize"), ("result", "ok"), ("impl", impl)));
        Assert.True(sink.HasEvent("squirix_serializer_ops_total", ("op", "deserialize"), ("result", "ok"), ("impl", impl)));
    }

    /// <summary>
    /// Verifies serialize failure records error metrics when the inner serializer throws.
    /// </summary>
    [Fact]
    public void SerializeFailureRecordsErrorMetrics()
    {
        using var sink = new MeasurementSink("Squirix");
        var inner = A.Fake<ISquirixSerializer>();
        _ = A.CallTo(() => inner.Serialize(A<Stream>._, A<object?>._)).Throws(new InvalidOperationException("fail"));
        var decorated = new MetricsDecoratedSerializer(inner);

        using var ms = new MemoryStream();
        _ = Assert.Throws<InvalidOperationException>(() => decorated.Serialize(ms, new object()));

        var impl = inner.GetType().Name;
        Assert.True(sink.HasEvent("squirix_serializer_ops_total", ("op", "serialize"), ("result", "error"), ("impl", impl)));
        Assert.True(sink.HasEvent("squirix_serializer_failures_total", ("op", "serialize"), ("exception_type", "InvalidOperationException"), ("impl", impl)));
    }
}
