using System;
using System.IO;
using System.Text.Json;
using Squirix.Serialization;
using Squirix.TestKit.Diagnostics;
using Xunit;

namespace Squirix.UnitTests.Serialization;

/// <summary>Tests to validate serializer metrics emission (counters, histograms, failures) via SerializationProvider.</summary>
public sealed class SerializerMetricsTests
{
    /// <summary>Ensures failed deserialization records failures_total with exception_type and appropriate ops_total/error and duration metrics.</summary>
    [Fact]
    public void FailureMetricsIncludeExceptionType()
    {
        using var sink = new MeasurementSink("Squirix");

        var inner = new SystemTextJsonSerializer();
        var serializer = new StreamDeserializeFaultSerializer(inner);

        var scoped = SerializationProvider.Create(serializer);

        var bytes = scoped.SerializeToUtf8Bytes("ping");
        Assert.NotEmpty(bytes);

        using var ms = new MemoryStream(bytes);
        _ = Assert.Throws<InvalidOperationException>(() => { _ = scoped.Deserialize<JsonElement>(ms); });

        const string impl = nameof(StreamDeserializeFaultSerializer);
        Assert.True(sink.HasEvent("squirix_serializer_ops_total", ("op", "deserialize"), ("result", "error"), ("impl", impl)));
        Assert.True(
            sink.HasEvent("squirix_serializer_failures_total", ("op", "deserialize"), ("exception_type", "InvalidOperationException"), ("impl", impl)));
        Assert.True(sink.HasEvent("squirix_serializer_op_duration_seconds", ("op", "deserialize"), ("impl", impl)));
    }

    /// <summary>Ensures successful serialize/deserialize operations produce ops_total and duration metrics with expected labels.</summary>
    [Fact]
    public void SuccessMetricsAreRecordedForSerializeAndDeserialize()
    {
        using var sink = new MeasurementSink("Squirix");

        var payload = new { A = 1, B = "x" };
        var bytes = SerializationProvider.SerializeToUtf8Bytes(payload);
        var obj = SerializationProvider.Deserialize<JsonElement>(bytes);

        Assert.Equal(JsonValueKind.Object, obj.ValueKind);

        Assert.True(sink.HasEvent("squirix_serializer_ops_total", ("op", "serialize"), ("result", "ok"), ("impl", "SystemTextJsonSerializer")));
        Assert.True(sink.HasEvent("squirix_serializer_op_duration_seconds", ("op", "serialize"), ("impl", "SystemTextJsonSerializer")));

        Assert.True(sink.HasEvent("squirix_serializer_ops_total", ("op", "deserialize"), ("result", "ok"), ("impl", "SystemTextJsonSerializer")));
        Assert.True(sink.HasEvent("squirix_serializer_op_duration_seconds", ("op", "deserialize"), ("impl", "SystemTextJsonSerializer")));
    }
}
