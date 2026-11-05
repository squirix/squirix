using System;
using System.Collections.Frozen;
using System.Collections.Generic;
using System.Threading.Tasks;
using Google.Protobuf;
using Google.Protobuf.WellKnownTypes;
using Grpc.Core;
using Squirix.Server.Core;
using Squirix.Server.Node.Services;
using Squirix.Server.Serialization.Grpc;
using Squirix.Server.TestKit;
using Squirix.Server.UnitTests.Support;
using Squirix.Transport.Grpc.Cache;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Node.Services;

/// <summary>Protects the canonical bytes stored for replicated mutation outcomes.</summary>
public sealed class ReplicaOutcomeCodecTests : ServerUnitTestBase
{
    /// <summary>Both flags keep their canonical byte and little-endian length for empty and multi-byte lengths.</summary>
    /// <param name="applied">Whether the mutation applied.</param>
    /// <param name="length">Previous payload length.</param>
    [Test]
    [Arguments(false, 0)]
    [Arguments(true, 0)]
    [Arguments(false, 257)]
    [Arguments(true, 257)]
    [Arguments(false, 65536)]
    [Arguments(true, 65536)]
    public async Task EncodeKeepsCanonicalLayout(bool applied, int length)
    {
        var previous = new byte[length];
        previous.AsSpan().Fill(0xA5);
        var expected = CanonicalOutcome(applied, previous);

        var encoded = ReplicaOutcomeCodec.Encode(applied, previous);

        await SequenceAssert.EqualAsync(expected, encoded);
        _ = await Assert.That(ReplicaOutcomeCodec.TryDecode(encoded, out var decodedApplied, out var decodedPrevious)).IsTrue();
        _ = await Assert.That(decodedApplied).IsEqualTo(applied);
        await SequenceAssert.EqualAsync(previous, decodedPrevious.ToArray());
    }

    /// <summary>Encoding a memory slice copies only that slice and owns the resulting payload.</summary>
    [Test]
    public async Task EncodeOwnsPreviousSlice()
    {
        byte[] source = [99, 1, 2, 3, 99];
        var encoded = ReplicaOutcomeCodec.Encode(true, source.AsMemory(1, 3));
        source.AsSpan().Clear();

        await SequenceAssert.EqualAsync<byte>([1, 3, 0, 0, 0, 1, 2, 3], encoded);
    }

    /// <summary>Removing a live entry preserves the protobuf bytes, including null values, Unicode, tags, version and deadline.</summary>
    /// <param name="valueLength">Value length, or negative for a stored null.</param>
    [Test]
    [Arguments(-1)]
    [Arguments(0)]
    [Arguments(257)]
    [Arguments(65536)]
    public async Task RemoveKeepsCanonicalEntry(int valueLength)
    {
        var now = new DateTime(2026, 1, 1, 0, 0, 0, DateTimeKind.Utc);
        var tags = new Dictionary<string, string>(StringComparer.Ordinal) { ["tag"] = "значение" }.ToFrozenDictionary(StringComparer.Ordinal);
        var entry = new NodeCacheEntry<object?>(valueLength < 0 ? null : new string('я', valueLength), 123, now.AddHours(1), tags: tags);
        var expected = CanonicalOutcome(true, entry.MapToProto().ToByteArray());

        var decision = ReplicaMutationDecisions.DecideRemove(entry, now);
        var decoded = ReplicaOutcomeCodec.DecodeRemove(decision.Outcome);

        await SequenceAssert.EqualMemoryAsync(expected, decision.Outcome);
        _ = await Assert.That(decoded.Removed).IsTrue();
        _ = await Assert.That(decoded.Value).IsEqualTo(entry.Value);
    }

    /// <summary>Absent and expired entries keep the canonical unsuccessful remove outcome.</summary>
    /// <param name="expired">Whether a stored entry has expired.</param>
    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task MissingRemoveKeepsCanonicalLayout(bool expired)
    {
        var now = new DateTime(2026, 1, 1, 0, 0, 0, DateTimeKind.Utc);
        var entry = expired ? new NodeCacheEntry<object?>("old", 1, now) : null;

        var decision = ReplicaMutationDecisions.DecideRemove(entry, now);
        var decoded = ReplicaOutcomeCodec.DecodeRemove(decision.Outcome);

        await SequenceAssert.EqualMemoryAsync(new byte[5], decision.Outcome);
        _ = await Assert.That(decoded.Removed).IsFalse();
        _ = await Assert.That(decoded.Value).IsNull();
    }

    /// <summary>Malformed envelopes and a successful remove without a previous entry retain the stable corruption failure.</summary>
    /// <param name="kind">The malformed envelope case.</param>
    [Test]
    [Arguments("truncated")]
    [Arguments("length")]
    [Arguments("missing")]
    public async Task RemoveRejectsMalformedEnvelope(string kind)
    {
        var payload = MalformedEnvelope(kind);

        var failure = NodeExceptionAssert.For<InvalidOperationException>().Throws(payload, static value => _ = ReplicaOutcomeCodec.DecodeRemove(value));

        _ = await Assert.That(failure.Message).IsEqualTo("Committed remove outcome payload is malformed.");
        _ = await Assert.That(failure.InnerException).IsNull();
    }

    /// <summary>Invalid protobuf remains a protobuf parse failure rather than being translated as a wire-value error.</summary>
    [Test]
    public void RemovePreservesProtobufParseFailure()
    {
        var payload = CanonicalOutcome(true, [0x80]);

        _ = NodeExceptionAssert.For<InvalidProtocolBufferException>().Throws(payload, static value => _ = ReplicaOutcomeCodec.DecodeRemove(value));
    }

    /// <summary>Invalid values and time metadata are translated to corruption failures while retaining the original RPC cause.</summary>
    /// <param name="kind">The malformed previous-entry case.</param>
    [Test]
    [Arguments("missing")]
    [Arguments("json")]
    [Arguments("timestamp")]
    [Arguments("duration")]
    public async Task RemoveTranslatesInvalidPreviousEntry(string kind)
    {
        var previous = InvalidPreviousEntry(kind);
        var payload = CanonicalOutcome(true, previous.ToByteArray());

        var failure = NodeExceptionAssert.For<InvalidOperationException>().Throws(payload, static value => _ = ReplicaOutcomeCodec.DecodeRemove(value));

        _ = await Assert.That(failure.Message).IsEqualTo("Committed remove outcome payload is malformed.");
        var cause = await Assert.That(failure.InnerException).IsTypeOf<RpcException>();
        _ = await Assert.That(cause!.StatusCode).IsEqualTo(StatusCode.InvalidArgument);
    }

    private static byte[] MalformedEnvelope(string kind) => kind switch
    {
        "truncated" => [1, 0],
        "length" => [1, 2, 0, 0, 0, 42],
        "missing" => [1, 0, 0, 0, 0],
        _ => throw new ArgumentOutOfRangeException(nameof(kind)),
    };

    private static CacheEntryWire InvalidPreviousEntry(string kind) => kind switch
    {
        "missing" => new CacheEntryWire { Value = new CacheValue() },
        "json" => new CacheEntryWire { Value = new CacheValue { JsonUtf8 = ByteString.CopyFromUtf8("{bad") } },
        "timestamp" => new CacheEntryWire { Value = new CacheValue { StringValue = "value" }, ExpiresUtc = new Timestamp { Seconds = 1, Nanos = -1 } },
        "duration" => new CacheEntryWire { Value = new CacheValue { StringValue = "value" }, Expiration = new Duration { Seconds = 1, Nanos = -1 } },
        _ => throw new ArgumentOutOfRangeException(nameof(kind)),
    };

    private static byte[] CanonicalOutcome(bool applied, byte[] previous)
    {
        byte[] header =
        [
            Convert.ToByte(applied),
            Convert.ToByte(previous.Length & 255),
            Convert.ToByte((previous.Length >> 8) & 255),
            Convert.ToByte((previous.Length >> 16) & 255),
            Convert.ToByte((previous.Length >> 24) & 255),
        ];
        return [.. header, .. previous];
    }
}
