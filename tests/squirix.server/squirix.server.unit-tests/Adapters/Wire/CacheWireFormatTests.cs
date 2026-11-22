using System;
using System.Buffers;
using System.IO;
using System.Runtime.InteropServices;
using System.Threading.Tasks;
using Squirix.Server.TestKit;
using Squirix.Transport.Wire.Cache;
using Squirix.Transport.Wire.Codec;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Adapters.Wire;

/// <summary>Verifies entry presence, signed tick values and exact frame boundaries.</summary>
public sealed class CacheWireFormatTests
{
    /// <summary>Absent fields, tagged null and zero-valued metadata have distinct encodings.</summary>
    [Test]
    public async Task ZeroTicksAndNullHaveExplicitPresence()
    {
        var empty = CacheEntryWire.Parser.ParseFrom([0]);
        _ = await Assert.That(empty.Value).IsNull();
        _ = await Assert.That(empty.ExpiresUtc).IsNull();
        _ = await Assert.That(empty.Expiration).IsNull();

        byte[] frame = [7, 1, 0, 0];
        var entry = CacheEntryWire.Parser.ParseFrom(frame);
        _ = await Assert.That(entry.Value!.KindCase).IsEqualTo(CacheValue.KindOneofCase.NullValue);
        _ = await Assert.That(entry.ExpiresUtc!.Value.Ticks).IsEqualTo(0L);
        _ = await Assert.That(entry.ExpiresUtc.Value.Kind).IsEqualTo(DateTimeKind.Utc);
        _ = await Assert.That(entry.Expiration).IsEqualTo(TimeSpan.Zero);
        _ = await Assert.That(entry.ToByteArray().AsSpan().SequenceEqual(frame)).IsTrue();
    }

    /// <summary>The full duration range and UTC timestamp range survive a wire round trip.</summary>
    /// <param name="ticks">The signed duration.</param>
    [Test]
    [Arguments(long.MinValue)]
    [Arguments(-1L)]
    [Arguments(long.MaxValue)]
    public async Task EntryPreservesTimeBoundaries(long ticks)
    {
        var expires = DateTime.SpecifyKind(ticks < 0 ? DateTime.MinValue : DateTime.MaxValue, DateTimeKind.Utc);
        var entry = new CacheEntryWire { ExpiresUtc = expires, Expiration = TimeSpan.FromTicks(ticks) };
        var parsed = CacheEntryWire.Parser.ParseFrom(entry.ToByteArray());
        _ = await Assert.That(parsed.ExpiresUtc).IsEqualTo(expires);
        _ = await Assert.That(parsed.Expiration!.Value.Ticks).IsEqualTo(ticks);
    }

    /// <summary>Operation identity preserves the exact caller string and the compact SDK representation.</summary>
    /// <param name="operationId">The identity supplied by the caller.</param>
    /// <param name="wireBytes">The full request encoding, including the presence flag.</param>
    [Test]
    [Arguments("", "000000")]
    [Arguments("é", "01000003C3A9")]
    [Arguments("0123456789abcdef0123456789abcdef", "010000000123456789ABCDEF0123456789ABCDEF")]
    [Arguments("ABCDEF", "01000007414243444546")]
    public async Task MutationIdentityHasStableEncoding(string operationId, string wireBytes)
    {
        var request = new RemoveAsyncRequest { OperationId = operationId };
        var expected = Convert.FromHexString(wireBytes);
        _ = await Assert.That(request.ToByteArray().AsSpan().SequenceEqual(expected)).IsTrue();
        _ = await Assert.That(RemoveAsyncRequest.Parser.ParseFrom(expected).OperationId).IsEqualTo(operationId);
    }

    /// <summary>Distributed write metadata precedes the value so the frame boundary delimits its bytes.</summary>
    /// <param name="includeOperationId">Whether the request carries a replay identity.</param>
    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task DistributedWriteEndsWithUnprefixedValue(bool includeOperationId)
    {
        byte[] value = [0, 128, 255];
        byte[] expected = includeOperationId
            ? [13, 1, 99, 1, 107, 1, 2, 120, 0, 0, 128, 255]
            : [9, 1, 99, 1, 107, 1, 0, 0, 128, 255];
        var write = new SetDistributedAsyncRequest { CacheName = "c", Key = "k", ExpiresAtUnixMs = -1, OperationId = includeOperationId ? "x" : string.Empty, TtlTicks = 0, Value = value };
        _ = await Assert.That(write.ToByteArray().AsSpan().SequenceEqual(expected)).IsTrue();

        var decoded = SetDistributedAsyncRequest.Parser.ParseBorrowed(expected);
        _ = await Assert.That(decoded.Value.Span.Overlaps(expected.AsSpan(expected.Length - value.Length), out var offset) && offset == 0).IsTrue();
        _ = await Assert.That(decoded.Value.Span.SequenceEqual(value)).IsTrue();
        _ = await Assert.That(decoded.OperationId).IsEqualTo(includeOperationId ? "x" : string.Empty);
        _ = await Assert.That(decoded.HasTtlTicks).IsTrue();
        _ = await Assert.That(decoded.HasSlidingTicks).IsFalse();
    }

    /// <summary>A missing or incomplete policy field cannot be mistaken for an empty value.</summary>
    [Test]
    public void DistributedWriteRequiresCompleteMetadata()
    {
        foreach (var bytes in new byte[][] { [1, 0, 0], [2, 0, 0, 128], [4, 0, 0, 2], [8, 0, 0, 128] })
            _ = NodeExceptionAssert.For<InvalidDataException>().Throws(bytes, static frame => _ = SetDistributedAsyncRequest.Parser.ParseFrom(frame));
    }

    /// <summary>The response retains a hit on an empty value, including when the caller requests an owned copy.</summary>
    [Test]
    public async Task DistributedReadUsesFrameBoundaryForValue()
    {
        var miss = new GetDistributedAsyncResponse();
        var emptyHit = new GetDistributedAsyncResponse { Found = true };
        _ = await Assert.That(miss.CalculateSize()).IsEqualTo(0);
        _ = await Assert.That(emptyHit.ToByteArray().AsSpan().SequenceEqual(new byte[] { 1 })).IsTrue();
        _ = await Assert.That(GetDistributedAsyncResponse.Parser.ParseFrom([1]).Found).IsTrue();

        byte[] payload = [1, 128, 255];
        var hit = GetDistributedAsyncResponse.Parser.ParseFrom(payload);
        payload[1] = 0;
        _ = await Assert.That(hit.Value.Span.SequenceEqual(new byte[] { 128, 255 })).IsTrue();
    }

    /// <summary>An owned read transfers its exact decoded array independently of the reusable TLS frame.</summary>
    [Test]
    public async Task DistributedReadTransfersOwnedArray()
    {
        byte[] frame = [1, 63, 75];
        var reply = GetDistributedAsyncResponse.Parser.ParseFrom(frame);
        _ = await Assert.That(MemoryMarshal.TryGetArray(reply.Value, out var segment)).IsTrue();
        var transferred = reply.TakeValue();
        _ = await Assert.That(ReferenceEquals(segment.Array, transferred)).IsTrue();
        _ = await Assert.That(reply.Value.IsEmpty).IsTrue();
        frame[1] = 0;
        _ = await Assert.That(transferred[0]).IsEqualTo(byte.CreateChecked(63));
        transferred[1] = 0;
        _ = await Assert.That(frame[2]).IsEqualTo(byte.CreateChecked(75));
    }

    /// <summary>A caller cannot mutate a borrowed frame or a value supplied by a reusable handler.</summary>
    /// <param name="borrowed">Whether the source is a borrowed frame.</param>
    [Test]
    [Arguments(true)]
    [Arguments(false)]
    public async Task DistributedReadCopiesSharedValue(bool borrowed)
    {
        byte[] frame = [1, 63, 75];
        var reply = borrowed
            ? GetDistributedAsyncResponse.Parser.ParseBorrowed(frame)
            : new GetDistributedAsyncResponse { Found = true, Value = frame.AsMemory(1) };
        var first = reply.TakeValue();
        first[0] = 0;
        var second = reply.TakeValue();
        _ = await Assert.That(second[0]).IsEqualTo(byte.CreateChecked(63));
        _ = await Assert.That(frame[1]).IsEqualTo(byte.CreateChecked(63));
    }

    /// <summary>Each canonical integer reaches a buffer writer in a single contiguous segment.</summary>
    /// <param name="value">The unsigned integer to encode.</param>
    /// <param name="hex">The expected wire bytes.</param>
    [Test]
    [Arguments(127UL, "7F")]
    [Arguments(128UL, "8001")]
    [Arguments(16384UL, "808001")]
    [Arguments(ulong.MaxValue, "FFFFFFFFFFFFFFFFFF01")]
    public async Task UnsignedIntegerAdvancesOnce(ulong value, string hex)
    {
        var destination = new CountingWriter();
        var writer = new WireWriter(destination);
        writer.Unsigned(value);
        var expected = Convert.FromHexString(hex);
        _ = await Assert.That(destination.Advances).IsEqualTo(1);
        _ = await Assert.That(destination.Bytes.WrittenSpan.SequenceEqual(expected)).IsTrue();
    }

    /// <summary>Encoding a traced request into a buffer writer produces the two trace fields followed by the original RPC body.</summary>
    [Test]
    public async Task TraceEnvelopeStreamsNestedRequest()
    {
        var body = new GetValueAsyncRequest { CacheName = "c", Key = "k" };
        var traced = new BinaryTracedRequest { TraceParent = "p", TraceState = "s", PayloadMessage = body };
        byte[] frame = [0, 1, 112, 1, 115, 1, 99, 1, 107];
        var buffer = new ArrayBufferWriter<byte>();
        traced.WriteTo(buffer);
        _ = await Assert.That(traced.CalculateSize()).IsEqualTo(frame.Length);
        _ = await Assert.That(buffer.WrittenSpan.SequenceEqual(frame)).IsTrue();
        _ = await Assert.That(traced.ToByteArray().AsSpan().SequenceEqual(frame)).IsTrue();

        var received = BinaryTracedRequest.Parser.ParseBorrowed(frame);
        _ = await Assert.That(received.TraceParent).IsEqualTo("p");
        _ = await Assert.That(received.TraceState).IsEqualTo("s");
        _ = await Assert.That(received.Payload.Span.Overlaps(frame.AsSpan(5), out var offset) && offset == 0).IsTrue();
        _ = await Assert.That(GetValueAsyncRequest.Parser.ParseFrom(received.Payload.Span).Key).IsEqualTo("k");
        _ = await Assert.That(received.ToByteArray().AsSpan().SequenceEqual(frame)).IsTrue();
    }

    /// <summary>Malformed entries fail before a value reaches the cache.</summary>
    [Test]
    public void RejectsMalformedFields()
    {
        byte[][] frames =
        [
            [8],                         // Reserved presence flag.
            [2, 1],                      // Negative UTC ticks.
            [4, 128],                    // Truncated duration.
            [4, 128, 0],                 // Noncanonical duration.
            [1, 5, 2],                   // Invalid Boolean.
            [1, 2, 1, 255],              // Invalid UTF-8.
            [1, 7, 255, 255, 255, 127],  // Binary length exceeds the frame.
            [0, 0],                      // Trailing data.
        ];
        foreach (var frame in frames)
            _ = NodeExceptionAssert.For<InvalidDataException>().Throws(frame, static bytes => _ = CacheEntryWire.Parser.ParseFrom(bytes));
    }

    private sealed class CountingWriter : IBufferWriter<byte>
    {
        internal ArrayBufferWriter<byte> Bytes { get; } = new();

        internal int Advances { get; private set; }

        public void Advance(int count)
        {
            Advances++;
            Bytes.Advance(count);
        }

        public Memory<byte> GetMemory(int sizeHint = 0) => Bytes.GetMemory(sizeHint);

        public Span<byte> GetSpan(int sizeHint = 0) => Bytes.GetSpan(sizeHint);
    }
}
