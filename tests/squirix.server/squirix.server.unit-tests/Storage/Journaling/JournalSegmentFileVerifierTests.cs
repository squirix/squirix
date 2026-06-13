using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using Google.Protobuf;
using Squirix.Server.Core;
using Squirix.Server.Storage;
using Squirix.Server.Storage.Journaling;
using Squirix.Server.Storage.Journaling.Json;
using Squirix.Server.Storage.JournalProto;
using Squirix.Server.TestKit.IO;
using Xunit;

namespace Squirix.Server.UnitTests.Storage.Journaling;

/// <summary>
/// Verifies read-only journal segment validation outcomes for complete and corrupted segment files.
/// </summary>
public sealed class JournalSegmentFileVerifierTests : ServerUnitTestBase
{
    private readonly string _dir = DirectoryKit.CreateTempDirectory("squirix-journal-verifier");

    /// <summary>
    /// Verifies cancellation is surfaced before further frame processing.
    /// </summary>
    [Fact]
    public void CancellationIsNotSwallowed()
    {
        var path = JournalPath(1);
        WriteSegment(path, [BuildPutEnvelope(1, "a")]);
        using var cts = new CancellationTokenSource();
        cts.Cancel();

        using var read = File.OpenRead(path);

        _ = Assert.Throws<OperationCanceledException>(() => JournalSegmentFileVerifier.TryVerify(read, cts.Token, out _, out _, out _));
    }

    /// <summary>
    /// Verifies CRC mismatches fail before a corrupted frame is counted.
    /// </summary>
    [Fact]
    public void CrcMismatchIsReportedSafely()
    {
        var path = JournalPath(1);
        var firstPayload = RecordCodec.Serialize(BuildPutEnvelope(1, "ok"));
        using (var stream = File.Create(path))
        {
            JournalFraming.WriteFileHeader(stream);
            JournalFraming.WriteFrame(stream, firstPayload);
            WriteFrameWithCrc(stream, RecordCodec.Serialize(BuildPutEnvelope(2, "bad")), 0xDEAD_BEEFu);
        }

        using var read = File.OpenRead(path);
        var verified = JournalSegmentFileVerifier.TryVerify(read, DefaultCancellationToken, out var frames, out var lastSequence, out var error);

        Assert.False(verified);
        Assert.Equal(1, frames);
        Assert.Equal(1UL, lastSequence);
        var corruptedFrameOffset = JournalFraming.FileHeaderSize + firstPayload.Length + JournalFraming.FrameHeaderSize + JournalFraming.FrameFooterSize;
        Assert.Equal($"journal frame CRC mismatch at offset {corruptedFrameOffset}", error);
    }

    /// <summary>
    /// Verifies invalid file headers fail without decoded frames.
    /// </summary>
    [Fact]
    public void InvalidHeaderIsReportedSafely()
    {
        var path = JournalPath(1);
        File.WriteAllBytes(path, "NOPE!"u8.ToArray());

        using var stream = File.OpenRead(path);
        var verified = JournalSegmentFileVerifier.TryVerify(stream, DefaultCancellationToken, out var frames, out var lastSequence, out var error);

        Assert.False(verified);
        Assert.Equal(0, frames);
        Assert.Equal(0UL, lastSequence);
        Assert.Equal("invalid or missing journal file header", error);
    }

    /// <summary>
    /// Verifies invalid journal JSON payloads are reported without throwing parser exceptions.
    /// </summary>
    [Fact]
    public void InvalidJsonFrameIsReportedSafely()
    {
        var path = JournalPath(1);
        using (var stream = File.Create(path))
        {
            JournalFraming.WriteFileHeader(stream);
            JournalFraming.WriteFrame(stream, "{not-json"u8);
        }

        using var read = File.OpenRead(path);
        var verified = JournalSegmentFileVerifier.TryVerify(read, DefaultCancellationToken, out var frames, out var lastSequence, out var error);

        Assert.False(verified);
        Assert.Equal(0, frames);
        Assert.Equal(0UL, lastSequence);
        Assert.Equal("journal frame JSON is invalid", error);
    }

    /// <summary>
    /// Verifies callers can report a missing segment without creating it.
    /// </summary>
    [Fact]
    public void MissingJournalSegmentFileIsReportedSafely()
    {
        var path = JournalPath(404);

        Assert.False(File.Exists(path));
        _ = Assert.Throws<FileNotFoundException>(() => File.Open(path, FileMode.Open, FileAccess.Read, FileShare.ReadWrite | FileShare.Delete));
        Assert.False(File.Exists(path));
    }

    /// <summary>
    /// Verifies payload lengths beyond the supported maximum are rejected before allocation.
    /// </summary>
    [Fact]
    public void OversizedPayloadLengthIsReportedSafely()
    {
        var path = JournalPath(1);
        using (var stream = File.Create(path))
        {
            JournalFraming.WriteFileHeader(stream);
            Span<byte> len = stackalloc byte[JournalFraming.FrameHeaderSize];
            BinaryPrimitives.WriteUInt32LittleEndian(len, 0x8000_0000u);
            stream.Write(len);
        }

        using var read = File.OpenRead(path);
        var verified = JournalSegmentFileVerifier.TryVerify(read, DefaultCancellationToken, out var frames, out var lastSequence, out var error);

        Assert.False(verified);
        Assert.Equal(0, frames);
        Assert.Equal(0UL, lastSequence);
        Assert.Equal($"declared journal payload length exceeds supported maximum at offset {JournalFraming.FileHeaderSize}", error);
    }

    /// <summary>
    /// Verifies trailing bytes after a valid frame are not accepted as a valid segment.
    /// </summary>
    [Fact]
    public void TrailingBytesAreReportedSafely()
    {
        var path = JournalPath(1);
        var payload = RecordCodec.Serialize(BuildPutEnvelope(1, "ok"));
        using (var stream = File.Create(path))
        {
            JournalFraming.WriteFileHeader(stream);
            JournalFraming.WriteFrame(stream, payload);
            stream.Write([0xAA, 0xBB]);
        }

        using var read = File.OpenRead(path);
        var verified = JournalSegmentFileVerifier.TryVerify(read, DefaultCancellationToken, out var frames, out var lastSequence, out var error);

        Assert.False(verified);
        Assert.Equal(1, frames);
        Assert.Equal(1UL, lastSequence);
        var trailingOffset = JournalFraming.FileHeaderSize + payload.Length + JournalFraming.FrameHeaderSize + JournalFraming.FrameFooterSize;
        Assert.Equal($"truncated journal frame header at offset {trailingOffset}", error);
    }

    /// <summary>
    /// Verifies a frame cut inside its CRC footer is rejected deterministically.
    /// </summary>
    [Fact]
    public void TruncatedFrameCrcFooterIsReportedSafely()
    {
        var path = JournalPath(1);
        using (var stream = File.Create(path))
        {
            JournalFraming.WriteFileHeader(stream);
            var payload = RecordCodec.Serialize(BuildPutEnvelope(1, "truncated-crc"));
            Span<byte> len = stackalloc byte[JournalFraming.FrameHeaderSize];
            BinaryPrimitives.WriteUInt32LittleEndian(len, (uint)payload.Length);
            stream.Write(len);
            stream.Write(payload);
            stream.Write([0x01, 0x02]);
        }

        using var read = File.OpenRead(path);
        var verified = JournalSegmentFileVerifier.TryVerify(read, DefaultCancellationToken, out var frames, out var lastSequence, out var error);

        Assert.False(verified);
        Assert.Equal(0, frames);
        Assert.Equal(0UL, lastSequence);
        Assert.Equal($"truncated journal frame CRC footer at offset {JournalFraming.FileHeaderSize}", error);
    }

    /// <summary>
    /// Verifies a frame cut inside the length header is not accepted as EOF.
    /// </summary>
    [Fact]
    public void TruncatedFrameHeaderIsReportedSafely()
    {
        var path = JournalPath(1);
        using (var stream = File.Create(path))
        {
            JournalFraming.WriteFileHeader(stream);
            stream.Write([0x10, 0x00]);
        }

        using var read = File.OpenRead(path);
        var verified = JournalSegmentFileVerifier.TryVerify(read, DefaultCancellationToken, out var frames, out var lastSequence, out var error);

        Assert.False(verified);
        Assert.Equal(0, frames);
        Assert.Equal(0UL, lastSequence);
        Assert.Equal($"truncated journal frame header at offset {JournalFraming.FileHeaderSize}", error);
    }

    /// <summary>
    /// Verifies a frame cut inside its payload is rejected deterministically.
    /// </summary>
    [Fact]
    public void TruncatedFramePayloadIsReportedSafely()
    {
        var path = JournalPath(1);
        using (var stream = File.Create(path))
        {
            JournalFraming.WriteFileHeader(stream);
            var payload = RecordCodec.Serialize(BuildPutEnvelope(1, "truncated"));
            Span<byte> len = stackalloc byte[JournalFraming.FrameHeaderSize];
            BinaryPrimitives.WriteUInt32LittleEndian(len, (uint)payload.Length);
            stream.Write(len);
            stream.Write(payload.AsSpan(0, payload.Length / 2));
        }

        using var read = File.OpenRead(path);
        var verified = JournalSegmentFileVerifier.TryVerify(read, DefaultCancellationToken, out var frames, out var lastSequence, out var error);

        Assert.False(verified);
        Assert.Equal(0, frames);
        Assert.Equal(0UL, lastSequence);
        Assert.Equal($"truncated journal frame payload at offset {JournalFraming.FileHeaderSize}", error);
    }

    /// <summary>
    /// Verifies a complete segment reports frame count and highest sequence.
    /// </summary>
    [Fact]
    public void ValidJournalSegmentVerifiesSuccessfully()
    {
        var path = JournalPath(1);
        WriteSegment(path, [BuildPutEnvelope(1, "a"), BuildRemoveEnvelope(3, "b")]);

        using var stream = File.Open(path, FileMode.Open, FileAccess.Read, FileShare.ReadWrite | FileShare.Delete);
        var verified = JournalSegmentFileVerifier.TryVerify(stream, DefaultCancellationToken, out var frames, out var lastSequence, out var error);

        Assert.True(verified);
        Assert.Equal(2, frames);
        Assert.Equal(3UL, lastSequence);
        Assert.Null(error);
    }

    /// <summary>
    /// Verifies verification leaves segment bytes unchanged for valid and invalid inputs.
    /// </summary>
    [Fact]
    public void VerifierNeverMutatesInspectedJournalSegmentFile()
    {
        var validPath = JournalPath(1);
        var invalidPath = JournalPath(2);
        WriteSegment(validPath, [BuildPutEnvelope(1, "valid")]);
        using (var stream = File.Create(invalidPath))
        {
            JournalFraming.WriteFileHeader(stream);
            stream.Write([0x10, 0x00]);
        }

        AssertVerifierDoesNotMutate(validPath);
        AssertVerifierDoesNotMutate(invalidPath);
    }

    /// <inheritdoc />
    protected override void Dispose(bool disposing)
    {
        if (disposing)
            DirectoryKit.TryDeleteDirectory(_dir);

        base.Dispose(disposing);
    }

    private static void AssertVerifierDoesNotMutate(string path)
    {
        var before = File.ReadAllBytes(path);
        using (var stream = File.Open(path, FileMode.Open, FileAccess.Read, FileShare.ReadWrite | FileShare.Delete))
            _ = JournalSegmentFileVerifier.TryVerify(stream, CancellationToken.None, out _, out _, out _);

        var after = File.ReadAllBytes(path);
        Assert.Equal(before, after);
    }

    private static JournalEnvelope BuildPutEnvelope(ulong seq, string key)
    {
        var body = DiscriminatedEntryJsonWriter.BuildEntryJson("value", null, null, 1, null);
        return new JournalEnvelope
        {
            Seq = seq,
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
        };
    }

    private static JournalEnvelope BuildRemoveEnvelope(ulong seq, string key) => new()
    {
        Seq = seq,
        UnixMs = 123,
        Remove = new Remove { Key = key, Namespace = CacheNames.DefaultNamespace },
    };

    private static void WriteFrameWithCrc(Stream stream, ReadOnlySpan<byte> payload, uint crc)
    {
        Span<byte> payloadLength = stackalloc byte[JournalFraming.FrameHeaderSize];
        BinaryPrimitives.WriteUInt32LittleEndian(payloadLength, (uint)payload.Length);
        stream.Write(payloadLength);
        stream.Write(payload);

        Span<byte> footer = stackalloc byte[JournalFraming.FrameFooterSize];
        BinaryPrimitives.WriteUInt32LittleEndian(footer, crc);
        stream.Write(footer);
    }

    private static void WriteSegment(string path, IReadOnlyList<JournalEnvelope> envelopes)
    {
        using var stream = File.Create(path);
        JournalFraming.WriteFileHeader(stream);
        foreach (var envelope in envelopes)
            JournalFraming.WriteFrame(stream, RecordCodec.Serialize(envelope));
    }

    private string JournalPath(int index) => PathKit.Combine(_dir, $"{StorageFilePrefixes.Journal}{index:000000}{StorageFileExtensions.Journal}");
}
