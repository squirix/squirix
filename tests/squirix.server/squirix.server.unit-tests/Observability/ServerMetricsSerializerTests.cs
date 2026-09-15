using System;
using System.Collections.Generic;
using System.Diagnostics.Metrics;
using System.IO;
using System.Text.Json;
using System.Threading.Tasks;
using Rocks;
using Squirix.Server.Attributes;
using Squirix.Server.Core;
using Squirix.Server.Node.Observability;
using Squirix.Server.TestKit;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Observability;

/// <summary>Covers metrics decorator paths around <see cref="ServerMetricsSerializer" />.</summary>
[Immutable]
public sealed class ServerMetricsSerializerTests : ServerUnitTestBase
{
    private static readonly Meter TestMeter = new("Squirix");

    /// <summary>Json failures are recorded and rethrown.</summary>
    [Test]
    public void InvalidJsonRethrowsJsonException()
    {
        var serializer = new ServerMetricsSerializer(new ServerJsonSerializer(), TestMeter);
        _ = NodeExceptionAssert.For<JsonException>().ThrowsAny(serializer, static value => value.Deserialize<Dictionary<string, int>>("{not-json"));
    }

    /// <summary>Successful serialize/deserialize overloads record without throwing.</summary>
    [Test]
    public async Task RoundTripOverloadsSucceed()
    {
        var serializer = new ServerMetricsSerializer(new ServerJsonSerializer(), TestMeter);
        var original = new Dictionary<string, int>(StringComparer.Ordinal) { ["value"] = 7 };
        const string payload = """{"value":7}""";

        var fromString = serializer.Deserialize<Dictionary<string, int>>(payload);
        _ = await Assert.That(fromString).IsNotNull();
        _ = await Assert.That(fromString["value"]).IsEqualTo(7);

        var element = serializer.SerializeToElement(original);
        var fromElement = serializer.Deserialize<Dictionary<string, int>>(element);
        _ = await Assert.That(fromElement!["value"]).IsEqualTo(7);

        var utf8 = serializer.SerializeToUtf8Bytes(original);
        var fromBytes = serializer.Deserialize<Dictionary<string, int>>(utf8.AsSpan());
        _ = await Assert.That(fromBytes!["value"]).IsEqualTo(7);

        await using var stream = new MemoryStream(utf8);
        var fromStream = serializer.Deserialize<Dictionary<string, int>>(stream);
        _ = await Assert.That(fromStream!["value"]).IsEqualTo(7);

        await using var destination = new MemoryStream();
        serializer.Serialize(destination, original);
        _ = await Assert.That(destination.Length > 0).IsTrue();
    }

    /// <summary>Inner NotSupportedException failures are recorded and rethrown.</summary>
    [Test]
    public void SerializeFailureFromInnerIsRethrown()
    {
        var innerExpectations = new IServerSerializerCreateExpectations();
        _ = innerExpectations.Setups.SerializeToUtf8Bytes(Arg.Any<string?>()).Throws<NotSupportedException>();
        var serializer = new ServerMetricsSerializer(innerExpectations.Instance(), TestMeter);
        _ = NodeExceptionAssert.For<NotSupportedException>().Throws(serializer, static value => value.SerializeToUtf8Bytes("x"));
    }

    /// <summary>IOException failures are recorded and rethrown.</summary>
    [Test]
    public void SerializeIoFailureFromInnerIsRethrown()
    {
        var innerExpectations = new IServerSerializerCreateExpectations();
        _ = innerExpectations.Setups.SerializeToUtf8Bytes(Arg.Any<string?>()).Throws<IOException>();
        var serializer = new ServerMetricsSerializer(innerExpectations.Instance(), TestMeter);
        _ = NodeExceptionAssert.For<IOException>().Throws(serializer, static value => value.SerializeToUtf8Bytes("x"));
    }

    /// <summary>Unhandled exception types are not filtered by the metrics decorator.</summary>
    [Test]
    public void UnhandledExceptionBypassesFailureFilter()
    {
        var innerExpectations = new IServerSerializerCreateExpectations();
        _ = innerExpectations.Setups.SerializeToUtf8Bytes(Arg.Any<string?>()).Throws<InvalidCastException>();
        var serializer = new ServerMetricsSerializer(innerExpectations.Instance(), TestMeter);
        _ = NodeExceptionAssert.For<InvalidCastException>().Throws(serializer, static value => value.SerializeToUtf8Bytes("x"));
    }
}
