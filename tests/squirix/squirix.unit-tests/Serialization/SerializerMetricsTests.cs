using System;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text.Json;
using FakeItEasy;
using FakeItEasy.Core;
using Squirix.Serialization;
using Squirix.TestKit;
using Xunit;

namespace Squirix.UnitTests.Serialization;

/// <summary>
/// Tests to validate serializer metrics emission (counters, histograms, failures) via SerializationProvider.
/// </summary>
public sealed class SerializerMetricsTests
{
    /// <summary>
    /// Ensures failed deserialization records failures_total with exception_type and appropriate ops_total/error and duration metrics.
    /// </summary>
    [Fact]
    public void FailureMetricsIncludeExceptionType()
    {
        using var sink = new MeasurementSink("Squirix");

        var inner = new SystemTextJsonSerializer();

        var serializer = A.Fake<ISquirixSerializer>();

        _ = A.CallTo(serializer).Where(static call => call.Method.Name == nameof(ISquirixSerializer.SerializeToUtf8Bytes) && call.Method.IsGenericMethod).WithReturnType<byte[]>()
             .ReturnsLazily(call => (byte[])InvokeGeneric(inner, nameof(SystemTextJsonSerializer.SerializeToUtf8Bytes), call)!);

        _ = A.CallTo(serializer).Where(static call => call.Method.Name == nameof(ISquirixSerializer.SerializeToElement) && call.Method.IsGenericMethod)
             .WithReturnType<JsonElement>().ReturnsLazily(call => (JsonElement)InvokeGeneric(inner, nameof(SystemTextJsonSerializer.SerializeToElement), call)!);

        _ = A.CallTo(() => serializer.Deserialize<JsonElement>(A<string>._)).ReturnsLazily((string s) => inner.Deserialize<JsonElement>(s));
        _ = A.CallTo(() => serializer.Deserialize<JsonElement>(A<JsonElement>._)).ReturnsLazily((JsonElement el) => inner.Deserialize<JsonElement>(el));

        _ = A.CallTo(() => serializer.Deserialize<JsonElement>(A<Stream>._)).Throws(static _ => new InvalidOperationException("boom"));

        _ = A.CallTo(() => serializer.Serialize(A<Stream>._, A<object?>._)).Invokes((Stream destination, object? value) => inner.Serialize(destination, value));

        var scoped = SerializationProvider.Create(serializer);

        var bytes = scoped.SerializeToUtf8Bytes(new { Z = 7 });
        Assert.NotEmpty(bytes);

        using var ms = new MemoryStream(bytes);
        _ = Assert.Throws<InvalidOperationException>(() => scoped.Deserialize<JsonElement>(ms));

        var implName = serializer.GetType().Name;
        Assert.True(sink.HasEvent("squirix_serializer_ops_total", ("op", "deserialize"), ("result", "error"), ("impl", implName)));
        Assert.True(sink.HasEvent("squirix_serializer_failures_total", ("op", "deserialize"), ("exception_type", "InvalidOperationException"), ("impl", implName)));
        Assert.True(sink.HasEvent("squirix_serializer_op_duration_seconds", ("op", "deserialize"), ("impl", implName)));
    }

    /// <summary>
    /// Ensures successful serialize/deserialize operations produce ops_total and duration metrics with expected labels.
    /// </summary>
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

    private static object? InvokeGeneric(SystemTextJsonSerializer inner, string methodName, IFakeObjectCall call)
    {
        var arg = call.Arguments[0];
        var argType = arg?.GetType() ?? typeof(object);
        var genericDef = typeof(SystemTextJsonSerializer).GetMethods(BindingFlags.Public | BindingFlags.Instance).Single(m => m.Name == methodName && m.IsGenericMethodDefinition);
        var gm = genericDef.MakeGenericMethod(argType);
        return gm.Invoke(inner, [arg]);
    }
}
