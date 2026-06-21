using System;
using System.Buffers.Binary;
using System.IO;
using System.Text;
using Squirix.Server.Utils;

namespace Squirix.Server.Storage.Manifest;

/// <summary>Encodes and decodes manifest files; on-disk layout is documented in docs/manifest-format.md.</summary>
internal static class ManifestCodec
{
    internal const int RollEncodedWithoutSnapshotLength = 26;

    private const int FileHeaderSize = 5;

    private const int FooterSize = 4;

    private const int RollBodyWithoutSnapshotLength = 4 + 4 + 8 + 1;

    private const int RollSnapshotSectionFixedLength = 4 + 8 + 4 + 8 + 2;

    private const byte Version = 1;

    private static ReadOnlySpan<byte> Magic => "SQMF"u8;

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
            throw new InvalidDataException("Manifest body is truncated.");

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

    public static byte[] Encode(ManifestState manifest)
    {
        var buffer = new byte[ComputeEncodedLength(manifest)];
        WriteEncoded(manifest, buffer);
        return buffer;
    }

    internal static int ComputeRollEncodedLength(ManifestState.SnapshotRef? snapshot, int snapshotPathUtf8Length)
    {
        if (snapshotPathUtf8Length > ushort.MaxValue)
            throw new InvalidDataException("Manifest snapshot path exceeds maximum encoded length.");

        var bodyLength = snapshot is null ? RollBodyWithoutSnapshotLength : RollBodyWithoutSnapshotLength + RollSnapshotSectionFixedLength + snapshotPathUtf8Length;
        return FileHeaderSize + bodyLength + FooterSize;
    }

    /// <summary>Encodes a segment-roll manifest update on the journal hot path (no snapshot path allocations).</summary>
    /// <param name="format">Manifest format field.</param>
    /// <param name="currentJournal">Updated current journal segment index.</param>
    /// <param name="nextSequence">Updated next journal sequence.</param>
    /// <param name="snapshot">Snapshot reference copied from the previous manifest (optional).</param>
    /// <param name="snapshotPathUtf8">UTF-8 bytes for <paramref name="snapshot" /> path when present.</param>
    /// <param name="destination">Output buffer; must be at least <see cref="ComputeRollEncodedLength" /> bytes.</param>
    /// <returns>Total encoded byte length written to <paramref name="destination" />.</returns>
    internal static int WriteRollEncoded(
        int format,
        int currentJournal,
        ulong nextSequence,
        ManifestState.SnapshotRef? snapshot,
        ReadOnlySpan<byte> snapshotPathUtf8,
        Span<byte> destination)
    {
        if (snapshotPathUtf8.Length > ushort.MaxValue)
            throw new InvalidDataException("Manifest snapshot path exceeds maximum encoded length.");

        var encodedLength = ComputeRollEncodedLength(snapshot, snapshotPathUtf8.Length);
        if (destination.Length < encodedLength)
            throw new ArgumentException("Destination span is too small for the encoded roll manifest.", nameof(destination));

        var offset = 0;
        Magic.CopyTo(destination);
        offset += Magic.Length;
        destination[offset++] = Version;

        var bodyStart = offset;
        BinaryPrimitives.WriteInt32LittleEndian(destination.Slice(offset), format);
        offset += 4;
        BinaryPrimitives.WriteInt32LittleEndian(destination.Slice(offset), currentJournal);
        offset += 4;
        BinaryPrimitives.WriteUInt64LittleEndian(destination.Slice(offset), nextSequence);
        offset += 8;

        if (snapshot is null)
        {
            destination[offset++] = 0;
        }
        else
        {
            destination[offset++] = 1;
            BinaryPrimitives.WriteInt32LittleEndian(destination.Slice(offset), snapshot.Index);
            offset += 4;
            BinaryPrimitives.WriteUInt64LittleEndian(destination.Slice(offset), snapshot.LastAppliedSequence);
            offset += 8;
            BinaryPrimitives.WriteInt32LittleEndian(destination.Slice(offset), snapshot.ReplayFromJournalSegment);
            offset += 4;
            var createdMs = new DateTimeOffset(snapshot.CreatedUtc).ToUnixTimeMilliseconds();
            BinaryPrimitives.WriteInt64LittleEndian(destination.Slice(offset), createdMs);
            offset += 8;
            BinaryPrimitives.WriteUInt16LittleEndian(destination.Slice(offset), ushort.CreateTruncating(snapshotPathUtf8.Length));
            offset += 2;
            if (!snapshotPathUtf8.IsEmpty)
            {
                snapshotPathUtf8.CopyTo(destination.Slice(offset));
                offset += snapshotPathUtf8.Length;
            }
        }

        BinaryPrimitives.WriteUInt32LittleEndian(destination.Slice(offset), Crc32C.Compute(destination.Slice(bodyStart, offset - bodyStart)));
        return encodedLength;
    }

    private static ManifestState.SnapshotRef DecodeSnapshotRef(ReadOnlySpan<byte> body, ref int offset)
    {
        if (body.Length < offset + 4 + 8 + 4 + 8 + 2)
            throw new InvalidDataException("Manifest snapshot section is truncated.");

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
            throw new InvalidDataException("Manifest snapshot path is truncated.");

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
            throw new InvalidDataException("Manifest file is truncated.");

        if (!fileBytes.StartsWith(Magic))
            throw new InvalidDataException("Manifest file has an invalid magic header.");

        if (fileBytes[Magic.Length] != Version)
            throw new InvalidDataException("Manifest file has an unsupported version.");

        var bodyEnd = fileBytes.Length - FooterSize;
        body = fileBytes.Slice(FileHeaderSize, bodyEnd - FileHeaderSize);
        var expectedCrc = BinaryPrimitives.ReadUInt32LittleEndian(fileBytes.Slice(bodyEnd));
        if (Crc32C.Compute(body) != expectedCrc)
            throw new InvalidDataException("Manifest file failed CRC validation.");
    }
}
