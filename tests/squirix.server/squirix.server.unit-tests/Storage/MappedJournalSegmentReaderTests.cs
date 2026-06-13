using System;
using System.Buffers.Binary;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Google.Protobuf;
using Squirix.Server.Storage;
using Squirix.Server.Storage.Journaling;
using Squirix.Server.Storage.Journaling.Json;
using Squirix.Server.Storage.JournalProto;
using Squirix.Server.TestKit.IO;
using Xunit;

namespace Squirix.Server.UnitTests.Storage;

/// <summary>
/// Tests for the memory-mapped journal reader corruption and lifetime boundaries.
/// </summary>
public sealed class MappedJournalSegmentReaderTests : ServerUnitTestBase, IAsyncLifetime
{
    private string _dir = null!;

    /// <summary>
    /// CRC mismatch in a segment throws <see cref="InvalidDataException" /> instead of silently stopping.
    /// </summary>
    [Fact]
    public void CorruptedSegmentThrowsOnCrcMismatch()
    {
        using (var stream = File.Create(JournalPath(1)))
        {
            JournalFraming.WriteFileHeader(stream);
            JournalFraming.WriteFrame(stream, BuildPayload(1, "first"));
            WriteFrameWithCrc(stream, BuildPayload(2, "bad"), 0xDEAD_BEEFu);
        }

        using (var stream = File.Create(JournalPath(2)))
        {
            JournalFraming.WriteFileHeader(stream);
            JournalFraming.WriteFrame(stream, BuildPayload(3, "next"));
        }

        _ = Assert.Throws<InvalidDataException>(() => JournalReader.ReadAll(_dir, 1, DefaultCancellationToken).ToArray());
    }

    /// <summary>
    /// CRC mismatch throws <see cref="InvalidDataException" /> at the corrupted frame.
    /// </summary>
    [Fact]
    public void CrcMismatchThrows()
    {
        using (var stream = File.Create(JournalPath(1)))
        {
            JournalFraming.WriteFileHeader(stream);
            JournalFraming.WriteFrame(stream, BuildPayload(1, "ok"));
            WriteFrameWithCrc(stream, BuildPayload(2, "bad"), 0xDEAD_BEEFu);
        }

        var ex = Assert.Throws<InvalidDataException>(() => JournalReader.ReadAll(_dir, 1, DefaultCancellationToken).ToArray());
        Assert.Contains("ChecksumMismatch", ex.Message, StringComparison.InvariantCulture);
    }

    /// <summary>
    /// A zero-length segment file yields no records.
    /// </summary>
    [Fact]
    public void EmptySegmentFileYieldsNoRecords()
    {
        File.WriteAllBytes(JournalPath(1), []);

        var records = JournalReader.ReadAll(_dir, 1, DefaultCancellationToken).ToArray();

        Assert.Empty(records);
    }

    /// <summary>
    /// A segment that contains only the file header and no frames yields no records.
    /// </summary>
    [Fact]
    public void HeaderOnlySegmentYieldsNoRecords()
    {
        using (var stream = File.Create(JournalPath(1)))
            JournalFraming.WriteFileHeader(stream);

        var records = JournalReader.ReadAll(_dir, 1, DefaultCancellationToken).ToArray();

        Assert.Empty(records);
    }

    /// <summary>
    /// Invalid journal file headers fail replay instead of being treated as an empty segment.
    /// </summary>
    [Fact]
    public void InvalidHeaderSegmentThrows()
    {
        File.WriteAllBytes(JournalPath(1), "NOPE!"u8.ToArray());

        var ex = Assert.Throws<InvalidDataException>(() => JournalReader.ReadAll(_dir, 1, DefaultCancellationToken).ToArray());

        Assert.Contains("invalid or missing journal file header", ex.Message, StringComparison.Ordinal);
    }

    /// <summary>
    /// Malformed JSON payload throws <see cref="InvalidDataException" /> at the corrupted frame.
    /// </summary>
    [Fact]
    public void MalformedPayloadThrows()
    {
        using (var stream = File.Create(JournalPath(1)))
        {
            JournalFraming.WriteFileHeader(stream);
            JournalFraming.WriteFrame(stream, BuildPayload(1, "ok"));
            JournalFraming.WriteFrame(stream, "{not-json"u8);
        }

        var ex = Assert.Throws<InvalidDataException>(() => JournalReader.ReadAll(_dir, 1, DefaultCancellationToken).ToArray());
        Assert.Contains("JSON corruption", ex.Message, StringComparison.InvariantCulture);
    }

    /// <summary>
    /// Torn frame tails stop the current segment at the last valid frame.
    /// </summary>
    /// <param name="tailKind">The torn-tail variant to append after a valid frame.</param>
    [Theory]
    [InlineData("length")]
    [InlineData("payload")]
    [InlineData("footer")]
    public void TornFrameTailStopsAtValidPrefix(string tailKind)
    {
        using (var stream = File.Create(JournalPath(1)))
        {
            JournalFraming.WriteFileHeader(stream);
            JournalFraming.WriteFrame(stream, BuildPayload(1, "ok"));
            WriteTornTail(stream, tailKind);
        }

        var records = JournalReader.ReadAll(_dir, 1, DefaultCancellationToken).ToArray();

        var record = Assert.Single(records);
        Assert.Equal(1UL, record.Seq);
        Assert.Equal("ok", record.Put.Item.Key);
    }

    /// <summary>
    /// A truncated journal file header fails replay.
    /// </summary>
    [Fact]
    public void TruncatedHeaderSegmentThrows()
    {
        File.WriteAllBytes(JournalPath(1), "SW"u8.ToArray());

        var ex = Assert.Throws<InvalidDataException>(() => JournalReader.ReadAll(_dir, 1, DefaultCancellationToken).ToArray());

        Assert.Contains("truncated file header", ex.Message, StringComparison.Ordinal);
    }

    /// <summary>
    /// Removes the journal directory after each test.
    /// </summary>
    /// <returns>A completed task.</returns>
    public ValueTask DisposeAsync()
    {
        DirectoryKit.TryDeleteDirectory(_dir);
        return ValueTask.CompletedTask;
    }

    /// <summary>
    /// Creates a fresh journal directory before each test.
    /// </summary>
    /// <returns>A completed task.</returns>
    public ValueTask InitializeAsync()
    {
        _dir = DirectoryKit.CreateTempDirectory("squirix-mmf-journal");
        return ValueTask.CompletedTask;
    }

    private static byte[] BuildPayload(ulong seq, string key)
    {
        var envelope = new JournalEnvelope
        {
            Seq = seq,
            UnixMs = 123,
            Put = new Put
            {
                Item = new EntryPair
                {
                    Key = key,
                    EntryJson = ByteString.CopyFrom("{\"v\":{\"$t\":\"s\",\"v\":\"value\"},\"ver\":1}"u8.ToArray()),
                },
            },
        };

        return RecordCodec.Serialize(envelope);
    }

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

    private static void WriteTornTail(Stream stream, string tailKind)
    {
        switch (tailKind)
        {
            case "length":
                stream.Write([0x10, 0x00]);
                break;

            case "payload":
                Span<byte> payloadLength = stackalloc byte[JournalFraming.FrameHeaderSize];
                BinaryPrimitives.WriteUInt32LittleEndian(payloadLength, 10);
                stream.Write(payloadLength);
                stream.Write("abc"u8);
                break;

            case "footer":
                var payload = BuildPayload(2, "tail");
                Span<byte> footerPayloadLength = stackalloc byte[JournalFraming.FrameHeaderSize];
                BinaryPrimitives.WriteUInt32LittleEndian(footerPayloadLength, (uint)payload.Length);
                stream.Write(footerPayloadLength);
                stream.Write(payload);
                stream.Write([0x01, 0x02]);
                break;

            default:
                throw new ArgumentOutOfRangeException(nameof(tailKind), tailKind, "Unknown torn-tail variant.");
        }
    }

    private string JournalPath(int index) => PathKit.Combine(_dir, $"{StorageFilePrefixes.Journal}{index:000000}{StorageFileExtensions.Journal}");
}
