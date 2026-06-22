using System;
using System.Buffers;
using System.Buffers.Binary;
using System.IO;
using Squirix.Server.Core;
using Squirix.Server.Storage.Journaling.Abstractions;
using Squirix.Server.Storage.Journaling.Codec;
using Squirix.Server.Storage.Journaling.Framing;
using Squirix.Server.Storage.Journaling.Observability;
using Squirix.Server.TestKit.Journaling;
using Squirix.Server.UnitTests.Support;
using Xunit;

namespace Squirix.Server.UnitTests.Persistence.Journaling;

/// <summary>Focused tests for shared journal frame parsing and classification.</summary>
public sealed class JournalFrameReaderTests : UnitTestBase
{
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
    public void EmptyFrameSourceIsHandledConsistently() => AssertConsistentStatus([], JournalFrameReadStatus.EndOfFile);

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
            if (firstBuffer is not null)
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
            if (secondBuffer is not null)
                ArrayPool<byte>.Shared.Return(secondBuffer);
        }
    }

    /// <summary>Verifies oversized declared payload lengths are rejected consistently.</summary>
    [Fact]
    public void OversizedFrameIsClassifiedConsistently()
    {
        Span<byte> length = stackalloc byte[JournalFraming.FrameHeaderSize];
        BinaryPrimitives.WriteUInt32LittleEndian(length, 0x8000_0000u);
        AssertConsistentStatus([.. length], JournalFrameReadStatus.OversizedFrame);
    }

    /// <summary>Verifies verifier-facing and mapped-reader-facing frame parsing classify the same corrupted byte streams the same way.</summary>
    /// <param name="kind">The corruption variant to classify through both parsing paths.</param>
    [Theory]
    [InlineData("truncated-header")]
    [InlineData("truncated-payload")]
    [InlineData("truncated-crc")]
    [InlineData("crc-mismatch")]
    [InlineData("oversized")]
    public void StreamAndMappedFramePathsClassifyCorruptionConsistently(string kind)
    {
        var bytes = kind switch
        {
            "truncated-header" => [0x10, 0x00],
            "truncated-payload" => BuildTruncatedPayload(),
            "truncated-crc" => BuildFrameBytes(BuildPayload(1, "crc"))[..^2],
            "crc-mismatch" => BuildCrcMismatchFrame(),
            "oversized" => BuildOversizedFrame(),
            _ => throw new ArgumentOutOfRangeException(nameof(kind), kind, "Unknown corruption kind."),
        };

        using var stream = new MemoryStream(bytes, false);
        var streamRead = JournalFrameReader.ReadNext(stream, 0, out _, out _);
        var spanRead = JournalFrameReader.ReadNext(bytes, 0);

        Assert.Equal(streamRead.Status, spanRead.Status);
    }

    /// <summary>Verifies trailing bytes after a full frame are classified consistently as a truncated header for the next frame.</summary>
    [Fact]
    public void TrailingBytesAfterLastFrameAreHandledConsistently()
    {
        var frame = BuildFrameBytes(BuildPayload(1, "tail"));
        var bytes = new byte[frame.Length + 2];
        Buffer.BlockCopy(frame, 0, bytes, 0, frame.Length);
        bytes[^2] = 0xAA;
        bytes[^1] = 0xBB;

        using var stream = new MemoryStream(bytes, false);
        var firstRead = JournalFrameReader.ReadNext(stream, 0, out var rentedBuffer, out _);
        try
        {
            Assert.Equal(JournalFrameReadStatus.Success, firstRead.Status);
        }
        finally
        {
            if (rentedBuffer is not null)
                ArrayPool<byte>.Shared.Return(rentedBuffer);
        }

        var secondRead = JournalFrameReader.ReadNext(stream, firstRead.NextFrameOffset, out _, out _);
        var spanRead = JournalFrameReader.ReadNext(bytes.AsSpan(int.CreateTruncating(firstRead.NextFrameOffset)), firstRead.NextFrameOffset);

        Assert.Equal(JournalFrameReadStatus.TruncatedHeader, secondRead.Status);
        Assert.Equal(secondRead.Status, spanRead.Status);
    }

    /// <summary>Verifies truncated frame checksum footers classify consistently for stream and span paths.</summary>
    [Fact]
    public void TruncatedChecksumIsClassifiedConsistently()
    {
        var payload = BuildPayload(1, "crc");
        var frame = BuildFrameBytes(payload);
        AssertConsistentStatus(frame[..^2], JournalFrameReadStatus.TruncatedChecksum);
    }

    /// <summary>Verifies truncated frame headers classify consistently for stream and span paths.</summary>
    [Fact]
    public void TruncatedHeaderIsClassifiedConsistently() => AssertConsistentStatus([0x10, 0x00], JournalFrameReadStatus.TruncatedHeader);

    /// <summary>Verifies truncated frame payloads classify consistently for stream and span paths.</summary>
    [Fact]
    public void TruncatedPayloadIsClassifiedConsistently()
    {
        Span<byte> length = stackalloc byte[JournalFraming.FrameHeaderSize];
        BinaryPrimitives.WriteUInt32LittleEndian(length, 10);
        AssertConsistentStatus([.. length, .. "ab"u8], JournalFrameReadStatus.TruncatedPayload);
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
            if (rentedBuffer is not null)
                ArrayPool<byte>.Shared.Return(rentedBuffer);
        }
    }

    private static void AssertConsistentStatus(byte[] bytes, JournalFrameReadStatus expectedStatus)
    {
        using var stream = new MemoryStream(bytes, false);
        var streamRead = JournalFrameReader.ReadNext(stream, 0, out _, out _);
        var spanRead = JournalFrameReader.ReadNext(bytes, 0);

        Assert.Equal(expectedStatus, streamRead.Status);
        Assert.Equal(expectedStatus, spanRead.Status);
    }

    private static byte[] BuildCrcMismatchFrame()
    {
        var frame = BuildFrameBytes(BuildPayload(1, "bad-crc"));
        frame[^1] ^= 0xFF;
        return frame;
    }

    private static byte[] BuildFrameBytes(byte[] payload)
    {
        var frameLength = JournalFraming.FrameTotalLength(payload.Length);
        var bytes = new byte[frameLength];
        JournalFraming.WriteFrame(bytes, payload);
        return bytes;
    }

    private static byte[] BuildFrameBytes(byte[] first, byte[] second)
    {
        var firstFrameLength = JournalFraming.FrameTotalLength(first.Length);
        var secondFrameLength = JournalFraming.FrameTotalLength(second.Length);
        var bytes = new byte[firstFrameLength + secondFrameLength];
        JournalFraming.WriteFrame(bytes.AsSpan(0, firstFrameLength), first);
        JournalFraming.WriteFrame(bytes.AsSpan(firstFrameLength, secondFrameLength), second);
        return bytes;
    }

    private static byte[] BuildOversizedFrame()
    {
        Span<byte> length = stackalloc byte[JournalFraming.FrameHeaderSize];
        BinaryPrimitives.WriteUInt32LittleEndian(length, 0x8000_0000u);
        return [.. length];
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
        var body = new byte[bodyLength];
        _ = BinaryJournalCodec.Encode(record, body);
        return body;
    }

    private static byte[] BuildTruncatedPayload()
    {
        Span<byte> length = stackalloc byte[JournalFraming.FrameHeaderSize];
        BinaryPrimitives.WriteUInt32LittleEndian(length, 10);
        return [.. length, .. "ab"u8];
    }
}
