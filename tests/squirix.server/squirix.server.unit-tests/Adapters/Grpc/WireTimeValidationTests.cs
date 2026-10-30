using System;
using System.Threading.Tasks;
using Google.Protobuf.WellKnownTypes;
using Grpc.Core;
using Squirix.Server.Serialization.Grpc;
using Squirix.Server.TestKit;
using Squirix.Server.UnitTests.Support;
using Squirix.Transport.Grpc.Cache;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Adapters.Grpc;

/// <summary>Non-normalized protobuf timestamps and durations from the wire are rejected as invalid arguments.</summary>
public sealed class WireTimeValidationTests
{
    /// <summary>An entry with a malformed expiry timestamp is rejected.</summary>
    /// <param name="seconds">The wire seconds.</param>
    /// <param name="nanos">The wire nanoseconds.</param>
    [Test]
    [Arguments(1L, 1_000_000_000)]
    [Arguments(1L, -1)]
    [Arguments(-62_135_596_801L, 0)]
    [Arguments(253_402_300_800L, 0)]
    public async Task MalformedEntryTimestampIsInvalidArgument(long seconds, int nanos)
    {
        var entry = CreateEntry();
        entry.ExpiresUtc = new Timestamp { Seconds = seconds, Nanos = nanos };

        var failure = NodeExceptionAssert.For<RpcException>().Throws(entry, static value => _ = value.MapFromProtoAsync<object?>().AsTask());

        _ = await Assert.That(failure.StatusCode).IsEqualTo(StatusCode.InvalidArgument);
    }

    /// <summary>An entry with a malformed relative expiration is rejected.</summary>
    /// <param name="seconds">The wire seconds.</param>
    /// <param name="nanos">The wire nanoseconds.</param>
    [Test]
    [Arguments(0L, 1_000_000_000)]
    [Arguments(0L, -1_000_000_000)]
    [Arguments(1L, -1)]
    [Arguments(-1L, 1)]
    public async Task MalformedEntryDurationIsInvalidArgument(long seconds, int nanos)
    {
        var entry = CreateEntry();
        entry.Expiration = new Duration { Seconds = seconds, Nanos = nanos };

        var failure = NodeExceptionAssert.For<RpcException>().Throws(entry, static value => _ = value.MapFromProtoAsync<object?>().AsTask());

        _ = await Assert.That(failure.StatusCode).IsEqualTo(StatusCode.InvalidArgument);
    }

    /// <summary>A standalone malformed duration, as sent by touch, is rejected.</summary>
    [Test]
    public async Task MalformedTouchDurationIsInvalid()
    {
        var failure = NodeExceptionAssert.For<RpcException>().Throws(new Duration { Seconds = -1L, Nanos = 1 }, static value => _ = ServerProtoEx.RequireDuration(value));

        _ = await Assert.That(failure.StatusCode).IsEqualTo(StatusCode.InvalidArgument);
    }

    /// <summary>Boundary values map without failing.</summary>
    [Test]
    public async Task NormalizedBoundariesAreAccepted()
    {
        var entry = CreateEntry();
        entry.ExpiresUtc = new Timestamp { Seconds = 253_402_300_799L, Nanos = 999_999_999 };
        entry.Expiration = new Duration { Seconds = -2L, Nanos = -500_000_000 };

        var mapped = await entry.MapFromProtoAsync<object?>();

        _ = await Assert.That(mapped.ExpiresUtc!.Value.Year).IsEqualTo(9999);
        _ = await Assert.That(mapped.Expiration).IsEqualTo(TimeSpan.FromSeconds(-2.5));
    }

    private static CacheEntryWire CreateEntry() => new() { Value = JsonValueKit.Wire("{\"__v\":\"v\"}") };
}
