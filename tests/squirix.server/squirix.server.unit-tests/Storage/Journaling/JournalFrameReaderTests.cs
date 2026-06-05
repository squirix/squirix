using System;
using System.Buffers;
using System.Buffers.Binary;
using System.IO;
using Google.Protobuf;
using Squirix.Server.Core;
using Squirix.Server.Storage.Journaling;
using Squirix.Server.Storage.Journaling.Json;
using Squirix.Server.Storage.JournalProto;
using Xunit;

namespace Squirix.Server.UnitTests.Storage.Journaling;

/// <summary>
/// Focused tests for shared journal frame parsing and classification.
/// </summary>
public sealed class JournalFrameReaderTests : ServerUnitTestBase
{
    /// <summary>
    /// Verifies CRC mismatches classify consistently for stream and span paths.
    /// </summary>
    [Fact]
    public void CrcMismatchIsClassifiedConsistently()
    {
        var payload = BuildPayload(1, "bad-crc");
        var frame = BuildFrameBytes(payload);
        frame[^1] ^= 0xFF;
        AssertConsistentStatus(frame, JournalFrameReadStatus.ChecksumMismatch);
    }

    /// <summary>
    /// Verifies an empty frame source is reported as EOF consistently.
    /// </summary>
    [Fact]
    public void EmptyFrameSourceIsHandledConsistently() => AssertConsistentStatus([], JournalFrameReadStatus.EndOfFile);

    /// <summary>
    /// Verifies multiple valid frames preserve order and offsets when read sequentially.
    /// </summary>
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
            Assert.Equal(first.Length + JournalFraming.FrameHeaderSize + JournalFraming.FrameFooterSize, firstRead.NextFrameOffset);
            Assert.Equal("first", RecordCodec.Deserialize(firstBuffer.AsSpan(0, firstLength)).Put.Item.Key);
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
            Assert.Equal("second", RecordCodec.Deserialize(secondBuffer.AsSpan(0, secondLength)).Put.Item.Key);
        }
        finally
        {
            if (secondBuffer is not null)
                ArrayPool<byte>.Shared.Return(secondBuffer);
        }
    }

    /// <summary>
    /// Verifies oversized declared payload lengths are rejected consistently.
    /// </summary>
    [Fact]
    public void OversizedFrameIsClassifiedConsistently()
    {
        Span<byte> length = stackalloc byte[JournalFraming.FrameHeaderSize];
        BinaryPrimitives.WriteUInt32LittleEndian(length, 0x8000_0000u);
        AssertConsistentStatus([.. length], JournalFrameReadStatus.OversizedFrame);
    }

    /// <summary>
    /// Verifies verifier-facing and mapped-reader-facing frame parsing classify the same corrupted byte streams the same way.
    /// </summary>
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

    /// <summary>
    /// Verifies trailing bytes after a full frame are classified consistently as a truncated header for the next frame.
    /// </summary>
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
        var spanRead = JournalFrameReader.ReadNext(bytes.AsSpan((int)firstRead.NextFrameOffset), firstRead.NextFrameOffset);

        Assert.Equal(JournalFrameReadStatus.TruncatedHeader, secondRead.Status);
        Assert.Equal(secondRead.Status, spanRead.Status);
    }

    /// <summary>
    /// Verifies truncated frame checksum footers classify consistently for stream and span paths.
    /// </summary>
    [Fact]
    public void TruncatedChecksumIsClassifiedConsistently()
    {
        var payload = BuildPayload(1, "crc");
        var frame = BuildFrameBytes(payload);
        AssertConsistentStatus(frame[..^2], JournalFrameReadStatus.TruncatedChecksum);
    }

    /// <summary>
    /// Verifies truncated frame headers classify consistently for stream and span paths.
    /// </summary>
    [Fact]
    public void TruncatedHeaderIsClassifiedConsistently() => AssertConsistentStatus([0x10, 0x00], JournalFrameReadStatus.TruncatedHeader);

    /// <summary>
    /// Verifies truncated frame payloads classify consistently for stream and span paths.
    /// </summary>
    [Fact]
    public void TruncatedPayloadIsClassifiedConsistently()
    {
        Span<byte> length = stackalloc byte[JournalFraming.FrameHeaderSize];
        BinaryPrimitives.WriteUInt32LittleEndian(length, 10);
        AssertConsistentStatus([.. length.ToArray(), (byte)'a', (byte)'b'], JournalFrameReadStatus.TruncatedPayload);
    }

    /// <summary>
    /// Verifies a valid single frame is read successfully and preserves payload bytes.
    /// </summary>
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
            Assert.Equal(payload, rentedBuffer.AsSpan(0, payloadLength).ToArray());
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

    private static byte[] BuildFrameBytes(params byte[][] payloads)
    {
        using var stream = new MemoryStream();
        foreach (var payload in payloads)
            JournalFraming.WriteFrame(stream, payload);

        return stream.ToArray();
    }

    private static byte[] BuildOversizedFrame()
    {
        Span<byte> length = stackalloc byte[JournalFraming.FrameHeaderSize];
        BinaryPrimitives.WriteUInt32LittleEndian(length, 0x8000_0000u);
        return [.. length];
    }

    private static byte[] BuildPayload(ulong sequence, string key)
    {
        var body = DiscriminatedEntryJsonWriter.BuildEntryJson("value", null, null, 1, null);
        return RecordCodec.Serialize(
            new JournalEnvelope
            {
                Seq = sequence,
                UnixMs = 123,
                Put = new Put
                {
                    Item = new EntryPair
                    {
                        Key = key,
                        Namespace = CacheNames.DefaultNamespace,
                        EntryJson = ByteString.CopyFrom(body),
                    },
                },
            });
    }

    private static byte[] BuildTruncatedPayload()
    {
        Span<byte> length = stackalloc byte[JournalFraming.FrameHeaderSize];
        BinaryPrimitives.WriteUInt32LittleEndian(length, 10);
        return [.. length.ToArray(), (byte)'a', (byte)'b'];
    }
}
