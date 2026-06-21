using System;
using System.Buffers.Binary;
using System.IO;
using System.Text;
using Squirix.Server.Utils;

namespace Squirix.Server.Storage.Manifest.Binary;

/// <summary>Encodes and decodes binary manifest files; on-disk layout is documented in docs/manifest-binary-format.md.</summary>
internal static class BinaryManifestCodec
{
    private const int FileHeaderSize = 5;

    private const int FooterSize = 4;

    private const byte Version = 1;

    private static ReadOnlySpan<byte> Magic => "SQMF"u8;

    public static byte[] Encode(ManifestState manifest)
    {
        var buffer = new byte[ComputeEncodedLength(manifest)];
        WriteEncoded(manifest, buffer);
        return buffer;
    }

    public static int ComputeEncodedLength(ManifestState manifest)
    {
        var pathBytes = string.IsNullOrEmpty(manifest.LastSnapshot?.Path) ? [] : Encoding.UTF8.GetBytes(manifest.LastSnapshot.Path);
        if (pathBytes.Length > ushort.MaxValue)
            throw new InvalidDataException("Manifest snapshot path exceeds maximum encoded length.");

        var bodyLength = 4 + 4 + 8 + 1 + (manifest.LastSnapshot is null ? 0 : 4 + 8 + 4 + 8 + 2 + pathBytes.Length);
        return FileHeaderSize + bodyLength + FooterSize;
    }

    public static void WriteEncoded(ManifestState manifest, Span<byte> destination)
    {
        var pathBytes = string.IsNullOrEmpty(manifest.LastSnapshot?.Path) ? [] : Encoding.UTF8.GetBytes(manifest.LastSnapshot.Path);
        if (pathBytes.Length > ushort.MaxValue)
            throw new InvalidDataException("Manifest snapshot path exceeds maximum encoded length.");

        if (destination.Length < ComputeEncodedLength(manifest))
            throw new ArgumentException("Destination span is too small for the encoded manifest.", nameof(destination));

        var offset = 0;
        Magic.CopyTo(destination);
        offset += Magic.Length;
        destination[offset++] = Version;

        var bodyStart = offset;
        BinaryPrimitives.WriteInt32LittleEndian(destination.Slice(offset), manifest.Format);
        offset += 4;
        BinaryPrimitives.WriteInt32LittleEndian(destination.Slice(offset), manifest.CurrentJournal);
        offset += 4;
        BinaryPrimitives.WriteUInt64LittleEndian(destination.Slice(offset), manifest.NextSequence);
        offset += 8;

        if (manifest.LastSnapshot is null)
        {
            destination[offset++] = 0;
        }
        else
        {
            destination[offset++] = 1;
            BinaryPrimitives.WriteInt32LittleEndian(destination.Slice(offset), manifest.LastSnapshot.Index);
            offset += 4;
            BinaryPrimitives.WriteUInt64LittleEndian(destination.Slice(offset), manifest.LastSnapshot.LastAppliedSequence);
            offset += 8;
            BinaryPrimitives.WriteInt32LittleEndian(destination.Slice(offset), manifest.LastSnapshot.ReplayFromJournalSegment);
            offset += 4;
            var createdMs = new DateTimeOffset(manifest.LastSnapshot.CreatedUtc).ToUnixTimeMilliseconds();
            BinaryPrimitives.WriteInt64LittleEndian(destination.Slice(offset), createdMs);
            offset += 8;
            BinaryPrimitives.WriteUInt16LittleEndian(destination.Slice(offset), ushort.CreateTruncating(pathBytes.Length));
            offset += 2;
            if (pathBytes.Length > 0)
            {
                pathBytes.CopyTo(destination.Slice(offset));
                offset += pathBytes.Length;
            }
        }

        var crcPayload = destination.Slice(bodyStart, offset - bodyStart);
        BinaryPrimitives.WriteUInt32LittleEndian(destination.Slice(offset), Crc32C.Compute(crcPayload));
    }

    public static ManifestState Decode(ReadOnlySpan<byte> fileBytes)
    {
        ValidateFileEnvelope(fileBytes, out var body);

        var offset = 0;
        if (body.Length < 4 + 4 + 8 + 1)
            throw new InvalidDataException("Binary manifest body is truncated.");

        var format = BinaryPrimitives.ReadInt32LittleEndian(body.Slice(offset));
        offset += 4;
        var currentJournal = BinaryPrimitives.ReadInt32LittleEndian(body.Slice(offset));
        offset += 4;
        var nextSequence = BinaryPrimitives.ReadUInt64LittleEndian(body.Slice(offset));
        offset += 8;
        var hasSnapshot = body[offset++] is not 0;
        var lastSnapshot = hasSnapshot ? DecodeSnapshotRef(body, ref offset) : null;

        return new ManifestState
        {
            Format = format,
            CurrentJournal = currentJournal,
            NextSequence = nextSequence,
            LastSnapshot = lastSnapshot,
        };
    }

    private static ManifestState.SnapshotRef DecodeSnapshotRef(ReadOnlySpan<byte> body, ref int offset)
    {
        if (body.Length < offset + 4 + 8 + 4 + 8 + 2)
            throw new InvalidDataException("Binary manifest snapshot section is truncated.");

        var snapshotIndex = BinaryPrimitives.ReadInt32LittleEndian(body.Slice(offset));
        offset += 4;
        var lastAppliedSequence = BinaryPrimitives.ReadUInt64LittleEndian(body.Slice(offset));
        offset += 8;
        var replayFromJournalSegment = BinaryPrimitives.ReadInt32LittleEndian(body.Slice(offset));
        offset += 4;
        var createdMs = BinaryPrimitives.ReadInt64LittleEndian(body.Slice(offset));
        offset += 8;
        var pathLen = BinaryPrimitives.ReadUInt16LittleEndian(body.Slice(offset));
        offset += 2;
        if (body.Length < offset + pathLen)
            throw new InvalidDataException("Binary manifest snapshot path is truncated.");

        string? path = null;
        if (pathLen > 0)
            path = Encoding.UTF8.GetString(body.Slice(offset, pathLen));

        offset += pathLen;
        return new ManifestState.SnapshotRef
        {
            Index = snapshotIndex,
            LastAppliedSequence = lastAppliedSequence,
            ReplayFromJournalSegment = replayFromJournalSegment,
            CreatedUtc = DateTimeOffset.FromUnixTimeMilliseconds(createdMs).UtcDateTime,
            Path = path,
        };
    }

    private static void ValidateFileEnvelope(ReadOnlySpan<byte> fileBytes, out ReadOnlySpan<byte> body)
    {
        if (fileBytes.Length < FileHeaderSize + FooterSize)
            throw new InvalidDataException("Binary manifest file is truncated.");

        if (!fileBytes.StartsWith(Magic))
            throw new InvalidDataException("Binary manifest file has an invalid magic header.");

        if (fileBytes[Magic.Length] != Version)
            throw new InvalidDataException("Binary manifest file has an unsupported version.");

        var bodyEnd = fileBytes.Length - FooterSize;
        body = fileBytes.Slice(FileHeaderSize, bodyEnd - FileHeaderSize);
        var expectedCrc = BinaryPrimitives.ReadUInt32LittleEndian(fileBytes.Slice(bodyEnd));
        if (Crc32C.Compute(body) != expectedCrc)
            throw new InvalidDataException("Binary manifest file failed CRC validation.");
    }
}
