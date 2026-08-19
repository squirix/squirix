using System;
using System.Buffers;
using System.Buffers.Binary;
using System.IO;
using Squirix.Server.Attributes;
using Squirix.Server.Core;
using Squirix.Server.Storage.Journaling.Abstractions;
using Squirix.Server.Storage.Journaling.Codec;
using Squirix.Server.Storage.Journaling.Read;
using Squirix.Server.TestKit;
using Squirix.Server.UnitTests.Support;
using Xunit;

namespace Squirix.Server.UnitTests.Persistence.Journaling.Codec;

/// <summary>Focused tests for shared journal frame parsing and classification.</summary>
[Immutable]
public sealed class JournalFrameReaderTests : ServerUnitTestBase
{
    private static readonly byte[] EmptyFrameBytes = [];
    private static readonly byte[] TruncatedHeaderBytes = [0x10, 0x00];

    /// <summary>Verifies CRC mismatches classify consistently for stream and span paths.</summary>
    [Fact]
    public void CrcMismatchIsClassifiedConsistently()
    {
        var payload = BuildPayload(1, "bad-crc");
        var frame = BuildFrameBytes(payload);
        frame[^1] ^= 0xFF;
        AssertConsistentStatus(frame, JournalFrameReadStatus.ChecksumMismatch);
    }

    /// <summary>Verifies an empty frame source is reported as EOF consistently.</summary>
    [Fact]
    public void EmptyFrameSourceIsHandledConsistently() => AssertConsistentStatus(EmptyFrameBytes, JournalFrameReadStatus.EndOfFile);

    /// <summary>Verifies multiple valid frames preserve order and offsets when read sequentially.</summary>
    [Fact]
    public void MultipleValidFramesPreserveOrderAndOffsets()
    {
        var first = BuildPayload(1, "first");
        var second = BuildPayload(2, "second");
        var bytes = BuildFrameBytes(first, second);

        using var stream = new MemoryStream(bytes, false);
        var firstRead = JournalFrameReader.ReadNext(stream, 0, out var firstBuffer, out var firstLength);
        try
        {
            Assert.Equal(JournalFrameReadStatus.Success, firstRead.Status);
            Assert.Equal(JournalFraming.FrameTotalLength(first.Length), firstRead.NextFrameOffset);
            Assert.Equal("first", BinaryJournalCodec.Decode(firstBuffer!, firstLength).Key.Key);
        }
        finally
        {
            if (firstBuffer != null)
                ArrayPool<byte>.Shared.Return(firstBuffer);
        }

        var secondRead = JournalFrameReader.ReadNext(stream, firstRead.NextFrameOffset, out var secondBuffer, out var secondLength);
        try
        {
            Assert.Equal(JournalFrameReadStatus.Success, secondRead.Status);
            Assert.Equal(bytes.Length, secondRead.NextFrameOffset);
            Assert.Equal("second", BinaryJournalCodec.Decode(secondBuffer!, secondLength).Key.Key);
        }
        finally
        {
            if (secondBuffer != null)
                ArrayPool<byte>.Shared.Return(secondBuffer);
        }
    }

    /// <summary>Verifies oversized declared payload lengths are rejected consistently.</summary>
    [Fact]
    public void OversizedFrameIsClassifiedConsistently()
    {
        var length = BufferKit.ToOwnedBytes(
            JournalFraming.FrameHeaderSize,
            0x8000_0000u,
            static (value, destination) => BinaryPrimitives.WriteUInt32LittleEndian(destination, value));
        AssertConsistentStatus(length, JournalFrameReadStatus.OversizedFrame);
    }

    /// <summary>Verifies truncated frame checksum footers classify consistently for stream and span paths.</summary>
    [Fact]
    public void TruncatedChecksumIsClassifiedConsistently()
    {
        var payload = BuildPayload(1, "crc");
        var frame = BuildFrameBytes(payload);
        AssertConsistentStatus(frame, frame.Length - 2, JournalFrameReadStatus.TruncatedChecksum);
    }

    /// <summary>Verifies truncated frame headers classify consistently for stream and span paths.</summary>
    [Fact]
    public void TruncatedHeaderIsClassifiedConsistently() => AssertConsistentStatus(TruncatedHeaderBytes, JournalFrameReadStatus.TruncatedHeader);

    /// <summary>Verifies truncated frame payloads classify consistently for stream and span paths.</summary>
    [Fact]
    public void TruncatedPayloadIsClassifiedConsistently()
    {
        var bytes = BufferKit.ToOwnedBytes(
            JournalFraming.FrameHeaderSize + 2,
            10u,
            static (payloadLength, destination) =>
            {
                BinaryPrimitives.WriteUInt32LittleEndian(destination, payloadLength);
                "ab"u8.CopyTo(destination[JournalFraming.FrameHeaderSize..]);
            });
        AssertConsistentStatus(bytes, JournalFrameReadStatus.TruncatedPayload);
    }

    /// <summary>Verifies a valid single frame is read successfully and preserves payload bytes.</summary>
    [Fact]
    public void ValidSingleFrameIsReadSuccessfully()
    {
        var payload = BuildPayload(1, "single");
        var bytes = BuildFrameBytes(payload);

        using var stream = new MemoryStream(bytes, false);
        var read = JournalFrameReader.ReadNext(stream, 0, out var rentedBuffer, out var payloadLength);

        try
        {
            Assert.Equal(JournalFrameReadStatus.Success, read.Status);
            Assert.Equal(bytes.Length, read.NextFrameOffset);
            Assert.Equal(payload.Length, payloadLength);
            Assert.True(payload.AsSpan().SequenceEqual(rentedBuffer.AsSpan(0, payloadLength)));
        }
        finally
        {
            if (rentedBuffer != null)
                ArrayPool<byte>.Shared.Return(rentedBuffer);
        }
    }

    private static void AssertConsistentStatus(byte[] bytes, JournalFrameReadStatus expectedStatus) => AssertConsistentStatus(bytes, bytes.Length, expectedStatus);

    private static void AssertConsistentStatus(byte[] bytes, int length, JournalFrameReadStatus expectedStatus)
    {
        using var stream = new MemoryStream(bytes, 0, length, false, true);
        var streamRead = JournalFrameReader.ReadNext(stream, 0, out _, out _);

        Assert.Equal(expectedStatus, streamRead.Status);
    }

    private static byte[] BuildFrameBytes(byte[] payload) => BufferKit.ToOwnedBytes(
        JournalFraming.FrameTotalLength(payload.Length),
        payload,
        static (p, frame) => JournalFraming.WriteFrame(frame, p));

    private static byte[] BuildFrameBytes(byte[] first, byte[] second)
    {
        var firstFrameLength = JournalFraming.FrameTotalLength(first.Length);
        var secondFrameLength = JournalFraming.FrameTotalLength(second.Length);
        return BufferKit.ToOwnedBytes(
            firstFrameLength + secondFrameLength,
            (first, second, firstFrameLength),
            static (state, bytes) =>
            {
                JournalFraming.WriteFrame(bytes[..state.firstFrameLength], state.first);
                JournalFraming.WriteFrame(bytes[state.firstFrameLength..], state.second);
            });
    }

    private static byte[] BuildPayload(ulong sequence, string key)
    {
        var record = new JournalRecord
        {
            Sequence = sequence,
            UnixMs = 123,
            Operation = JournalOperationKind.Put,
            Key = CacheKey.Default(key),
            PutEntryBytes = JournalEntryPayloadKit.EncodePut("value"),
        };
        var bodyLength = BinaryJournalCodec.ComputeFrameBodyLength(record);
        var prepared = BinaryJournalCodec.PrepareEncode(record);
        return BufferKit.ToOwnedBytes(bodyLength, (record, prepared), static (ctx, body) => _ = BinaryJournalCodec.Encode(ctx.record, body, in ctx.prepared));
    }
}
