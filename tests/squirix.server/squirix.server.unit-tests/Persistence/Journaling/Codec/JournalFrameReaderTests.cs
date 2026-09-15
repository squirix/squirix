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
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Persistence.Journaling.Codec;

/// <summary>Focused tests for shared journal frame parsing and classification.</summary>
[Immutable]
public sealed class JournalFrameReaderTests : ServerUnitTestBase
{
    private static readonly byte[] EmptyFrameBytes = [];
    private static readonly byte[] TruncatedHeaderBytes = [0x10, 0x00];

    /// <summary>Verifies CRC mismatches classify consistently.</summary>
    [Test]
    public Task CrcMismatchIsClassifiedConsistently()
    {
        var payload = BuildPayload(1, "bad-crc");
        var frame = BuildFrameBytes(payload);
        frame[^1] ^= 0xFF;
        return AssertStatusAsync(frame, frame.Length, JournalFrameReadStatus.ChecksumMismatch);
    }

    /// <summary>Verifies an empty frame source is reported as EOF consistently.</summary>
    [Test]
    public Task EmptyFrameSourceIsHandledConsistently() => AssertStatusAsync(EmptyFrameBytes, EmptyFrameBytes.Length, JournalFrameReadStatus.EndOfFile);

    /// <summary>Verifies a large positive declared length (corrupt header) is rejected as oversized before any buffer is rented.</summary>
    [Test]
    public Task LargePositiveFrameLengthIsOversized()
    {
        var length = BufferKit.ToOwnedBytes(
            JournalFraming.FrameHeaderSize,
            0x7FFF_FFF0u,
            static (value, destination) => BinaryPrimitives.WriteUInt32LittleEndian(destination, value));
        return AssertStatusAsync(length, length.Length, JournalFrameReadStatus.OversizedFrame);
    }

    /// <summary>Verifies oversized declared payload lengths are rejected consistently.</summary>
    [Test]
    public Task OversizedFrameIsClassifiedConsistently()
    {
        var length = BufferKit.ToOwnedBytes(
            JournalFraming.FrameHeaderSize,
            0x8000_0000u,
            static (value, destination) => BinaryPrimitives.WriteUInt32LittleEndian(destination, value));
        return AssertStatusAsync(length, length.Length, JournalFrameReadStatus.OversizedFrame);
    }

    /// <summary>Verifies truncated frame checksum footers classify consistently.</summary>
    [Test]
    public Task TruncatedChecksumClassifiedStably()
    {
        var payload = BuildPayload(1, "crc");
        var frame = BuildFrameBytes(payload);
        return AssertStatusAsync(frame, frame.Length - 2, JournalFrameReadStatus.TruncatedChecksum);
    }

    /// <summary>Verifies truncated frame headers classify consistently.</summary>
    [Test]
    public Task TruncatedHeaderIsClassifiedConsistently() => AssertStatusAsync(TruncatedHeaderBytes, TruncatedHeaderBytes.Length, JournalFrameReadStatus.TruncatedHeader);

    /// <summary>Verifies truncated frame payloads classify consistently.</summary>
    [Test]
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

    /// <summary>Verifies multiple valid frames preserve order and offsets when read sequentially.</summary>
    [Test]
    public async Task ValidFramesPreserveOrderAndOffsets()
    {
        var first = BuildPayload(1, "first");
        var second = BuildPayload(2, "second");
        var bytes = BuildFrameBytes(first, second);
        var capture = new OrderedFramesCapture();

        await WithFrameFileAsync(
            bytes,
            bytes.Length,
            handle =>
            {
                var firstRead = JournalFrameReader.ReadNext(handle, 0, out var firstBuffer, out var firstLength);
                capture.FirstStatus = firstRead.Status;
                capture.FirstNextOffset = firstRead.NextFrameOffset;
                capture.FirstExpectedOffset = JournalFraming.FrameTotalLength(first.Length);
                capture.FirstKey = firstBuffer == null ? null : BinaryJournalCodec.Decode(firstBuffer, firstLength).Key.Key;

                var secondRead = JournalFrameReader.ReadNext(handle, firstRead.NextFrameOffset, out var secondBuffer, out var secondLength);
                capture.SecondStatus = secondRead.Status;
                capture.SecondNextOffset = secondRead.NextFrameOffset;
                capture.SecondExpectedOffset = bytes.Length;
                capture.SecondKey = secondBuffer == null ? null : BinaryJournalCodec.Decode(secondBuffer, secondLength).Key.Key;

                if (firstBuffer != null)
                    ArrayPool<byte>.Shared.Return(firstBuffer);

                if (secondBuffer != null)
                    ArrayPool<byte>.Shared.Return(secondBuffer);
            });

        _ = await Assert.That(capture.FirstStatus).IsEqualTo(JournalFrameReadStatus.Success);
        _ = await Assert.That(capture.FirstNextOffset).IsEqualTo(capture.FirstExpectedOffset);
        _ = await Assert.That(capture.FirstKey).IsEqualTo("first");
        _ = await Assert.That(capture.SecondStatus).IsEqualTo(JournalFrameReadStatus.Success);
        _ = await Assert.That(capture.SecondNextOffset).IsEqualTo(capture.SecondExpectedOffset);
        _ = await Assert.That(capture.SecondKey).IsEqualTo("second");
    }

    /// <summary>Verifies a valid single frame is read successfully and preserves payload bytes.</summary>
    [Test]
    public async Task ValidSingleFrameIsReadSuccessfully()
    {
        var payload = BuildPayload(1, "single");
        var bytes = BuildFrameBytes(payload);
        var capture = new SingleFrameCapture();

        await WithFrameFileAsync(
            bytes,
            bytes.Length,
            handle =>
            {
                var read = JournalFrameReader.ReadNext(handle, 0, out var rentedBuffer, out var payloadLength);
                capture.Status = read.Status;
                capture.NextOffset = read.NextFrameOffset;
                capture.ExpectedOffset = bytes.Length;
                capture.PayloadLength = payloadLength;
                capture.ExpectedPayloadLength = payload.Length;
                capture.PayloadMatches = rentedBuffer != null && payload.AsSpan().SequenceEqual(rentedBuffer.AsSpan(0, payloadLength));

                if (rentedBuffer != null)
                    ArrayPool<byte>.Shared.Return(rentedBuffer);
            });

        _ = await Assert.That(capture.Status).IsEqualTo(JournalFrameReadStatus.Success);
        _ = await Assert.That(capture.NextOffset).IsEqualTo(capture.ExpectedOffset);
        _ = await Assert.That(capture.PayloadLength).IsEqualTo(capture.ExpectedPayloadLength);
        _ = await Assert.That(capture.PayloadMatches).IsTrue();
    }

    private static async Task AssertStatusAsync(byte[] bytes, int visibleLength, JournalFrameReadStatus expectedStatus)
    {
        var capture = new StatusCapture();
        await WithFrameFileAsync(
            bytes,
            visibleLength,
            handle =>
            {
                var read = JournalFrameReader.ReadNext(handle, 0, out _, out _);
                capture.Status = read.Status;
            });

        _ = await Assert.That(capture.Status).IsEqualTo(expectedStatus);
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
        var prepared = BinaryJournalCodec.PrepareEncode(record);
        return BufferKit.ToOwnedBytes(prepared.BodyLength, (record, prepared), static (ctx, body) => _ = BinaryJournalCodec.Encode(ctx.record, body, in ctx.prepared));
    }

    /// <summary>Writes the first <paramref name="visibleLength" /> bytes to a temporary segment file and runs the assertion against an open handle.</summary>
    /// <param name="bytes">Full file content.</param>
    /// <param name="visibleLength">Number of leading bytes written to the file (simulates truncation).</param>
    /// <param name="assertion">Assertion executed against an open read handle.</param>
    private static async Task WithFrameFileAsync(byte[] bytes, int visibleLength, Action<SafeFileHandle> assertion)
    {
        var path = Path.Join(Path.GetTempPath(), "jfr-" + Guid.NewGuid().ToString("N"));
        try
        {
            await File.WriteAllBytesAsync(path, bytes[..visibleLength], CancellationToken.None);
            using var handle = File.OpenHandle(path, FileMode.Open, FileAccess.Read, FileShare.Read | FileShare.Delete, FileOptions.SequentialScan);
            assertion(handle);
        }
        finally
        {
            File.Delete(path);
        }
    }

    private sealed class OrderedFramesCapture
    {
        public long FirstExpectedOffset { get; set; }

        public string? FirstKey { get; set; }

        public long FirstNextOffset { get; set; }

        public JournalFrameReadStatus FirstStatus { get; set; }

        public long SecondExpectedOffset { get; set; }

        public string? SecondKey { get; set; }

        public long SecondNextOffset { get; set; }

        public JournalFrameReadStatus SecondStatus { get; set; }
    }

    private sealed class SingleFrameCapture
    {
        public long ExpectedOffset { get; set; }

        public int ExpectedPayloadLength { get; set; }

        public long NextOffset { get; set; }

        public int PayloadLength { get; set; }

        public bool PayloadMatches { get; set; }

        public JournalFrameReadStatus Status { get; set; }
    }

    private sealed class StatusCapture
    {
        public JournalFrameReadStatus Status { get; set; }
    }
}
