using System;
using System.Buffers;
using System.Buffers.Binary;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Win32.SafeHandles;
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

    /// <summary>Verifies CRC mismatches classify consistently.</summary>
    [Fact]
    public Task CrcMismatchIsClassifiedConsistently()
    {
        var payload = BuildPayload(1, "bad-crc");
        var frame = BuildFrameBytes(payload);
        frame[^1] ^= 0xFF;
        return AssertStatusAsync(frame, frame.Length, JournalFrameReadStatus.ChecksumMismatch);
    }

    /// <summary>Verifies an empty frame source is reported as EOF consistently.</summary>
    [Fact]
    public Task EmptyFrameSourceIsHandledConsistently() =>
        AssertStatusAsync(EmptyFrameBytes, EmptyFrameBytes.Length, JournalFrameReadStatus.EndOfFile);

    /// <summary>Verifies multiple valid frames preserve order and offsets when read sequentially.</summary>
    [Fact]
    public Task MultipleValidFramesPreserveOrderAndOffsets()
    {
        var first = BuildPayload(1, "first");
        var second = BuildPayload(2, "second");
        var bytes = BuildFrameBytes(first, second);

        return WithFrameFileAsync(bytes, bytes.Length, handle =>
        {
            var firstRead = JournalFrameReader.ReadNext(handle, 0, out var firstBuffer, out var firstLength);
            Assert.Equal(JournalFrameReadStatus.Success, firstRead.Status);
            Assert.Equal(JournalFraming.FrameTotalLength(first.Length), firstRead.NextFrameOffset);
            Assert.Equal("first", BinaryJournalCodec.Decode(firstBuffer!, firstLength).Key.Key);

            var secondRead = JournalFrameReader.ReadNext(handle, firstRead.NextFrameOffset, out var secondBuffer, out var secondLength);
            Assert.Equal(JournalFrameReadStatus.Success, secondRead.Status);
            Assert.Equal(bytes.Length, secondRead.NextFrameOffset);
            Assert.Equal("second", BinaryJournalCodec.Decode(secondBuffer!, secondLength).Key.Key);

            if (firstBuffer != null)
                ArrayPool<byte>.Shared.Return(firstBuffer);

            if (secondBuffer != null)
                ArrayPool<byte>.Shared.Return(secondBuffer);
        });
    }

    /// <summary>Verifies oversized declared payload lengths are rejected consistently.</summary>
    [Fact]
    public Task OversizedFrameIsClassifiedConsistently()
    {
        var length = BufferKit.ToOwnedBytes(
            JournalFraming.FrameHeaderSize,
            0x8000_0000u,
            static (value, destination) => BinaryPrimitives.WriteUInt32LittleEndian(destination, value));
        return AssertStatusAsync(length, length.Length, JournalFrameReadStatus.OversizedFrame);
    }

    /// <summary>Verifies truncated frame checksum footers classify consistently.</summary>
    [Fact]
    public Task TruncatedChecksumIsClassifiedConsistently()
    {
        var payload = BuildPayload(1, "crc");
        var frame = BuildFrameBytes(payload);
        return AssertStatusAsync(frame, frame.Length - 2, JournalFrameReadStatus.TruncatedChecksum);
    }

    /// <summary>Verifies truncated frame headers classify consistently.</summary>
    [Fact]
    public Task TruncatedHeaderIsClassifiedConsistently() =>
        AssertStatusAsync(TruncatedHeaderBytes, TruncatedHeaderBytes.Length, JournalFrameReadStatus.TruncatedHeader);

    /// <summary>Verifies truncated frame payloads classify consistently.</summary>
    [Fact]
    public Task TruncatedPayloadIsClassifiedConsistently()
    {
        var bytes = BufferKit.ToOwnedBytes(
            JournalFraming.FrameHeaderSize + 2,
            10u,
            static (payloadLength, destination) =>
            {
                BinaryPrimitives.WriteUInt32LittleEndian(destination, payloadLength);
                "ab"u8.CopyTo(destination[JournalFraming.FrameHeaderSize..]);
            });
        return AssertStatusAsync(bytes, bytes.Length, JournalFrameReadStatus.TruncatedPayload);
    }

    /// <summary>Verifies a valid single frame is read successfully and preserves payload bytes.</summary>
    [Fact]
    public Task ValidSingleFrameIsReadSuccessfully()
    {
        var payload = BuildPayload(1, "single");
        var bytes = BuildFrameBytes(payload);

        return WithFrameFileAsync(bytes, bytes.Length, handle =>
        {
            var read = JournalFrameReader.ReadNext(handle, 0, out var rentedBuffer, out var payloadLength);

            Assert.Equal(JournalFrameReadStatus.Success, read.Status);
            Assert.Equal(bytes.Length, read.NextFrameOffset);
            Assert.Equal(payload.Length, payloadLength);
            Assert.True(payload.AsSpan().SequenceEqual(rentedBuffer!.AsSpan(0, payloadLength)));

            if (rentedBuffer != null)
                ArrayPool<byte>.Shared.Return(rentedBuffer);
        });
    }

    private static Task AssertStatusAsync(byte[] bytes, int visibleLength, JournalFrameReadStatus expectedStatus) =>
        WithFrameFileAsync(bytes, visibleLength, handle =>
        {
            var read = JournalFrameReader.ReadNext(handle, 0, out _, out _);
            Assert.Equal(expectedStatus, read.Status);
        });

    /// <summary>Writes the first <paramref name="visibleLength" /> bytes to a temporary segment file and runs the assertion against an open handle.</summary>
    /// <param name="bytes">Full file content.</param>
    /// <param name="visibleLength">Number of leading bytes written to the file (simulates truncation).</param>
    /// <param name="assertion">Assertion executed against an open read handle.</param>
    private static async Task WithFrameFileAsync(byte[] bytes, int visibleLength, Action<SafeFileHandle> assertion)
    {
        var path = Path.Join(Path.GetTempPath(), "jfr-" + Guid.NewGuid().ToString("N"));
        try
        {
            await File.WriteAllBytesAsync(path, bytes[0..visibleLength], CancellationToken.None);
            using var handle = File.OpenHandle(path, FileMode.Open, FileAccess.Read, FileShare.Read | FileShare.Delete, FileOptions.SequentialScan);
            assertion(handle);
        }
        finally
        {
            File.Delete(path);
        }
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
