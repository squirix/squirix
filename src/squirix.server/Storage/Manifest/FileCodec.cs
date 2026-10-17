using System;
using System.Buffers.Binary;
using System.IO;
using Squirix.Server.Utils;

namespace Squirix.Server.Storage.Manifest;

/// <summary>Encodes and decodes manifest files; on-disk layout is documented in docs/manifest-format.md.</summary>
internal static class FileCodec
{
    private const int FileHeaderSize = 5;

    private const int FooterSize = 4;

    private const int RollBodyWithoutSnapshotLength = 4 + 4 + 8 + 1;

    private const int RollSnapshotSectionLength = 4 + 8 + 4 + 8;

    private const byte Version = 1;

    private static ReadOnlySpan<byte> Magic => "SQMF"u8;

    internal static int ComputeEncodedLength(State manifest)
    {
        var bodyLength = 4 + 4 + 8 + 1 + (manifest.LastSnapshot == null ? 0 : RollSnapshotSectionLength);
        return FileHeaderSize + bodyLength + FooterSize;
    }

    internal static int ComputeRollEncodedLength(SnapshotRef? snapshot)
    {
        var bodyLength = snapshot == null ? RollBodyWithoutSnapshotLength : RollBodyWithoutSnapshotLength + RollSnapshotSectionLength;
        return FileHeaderSize + bodyLength + FooterSize;
    }

    internal static State Decode(ReadOnlySpan<byte> fileBytes)
    {
        ValidateFileEnvelope(fileBytes, out var body);

        var offset = 0;
        if (body.Length < 4 + 4 + 8 + 1)
            throw new InvalidDataException("Manifest body is truncated.");

        var format = BinaryPrimitives.ReadInt32LittleEndian(body[offset..]);
        offset += 4;
        var currentJournal = BinaryPrimitives.ReadInt32LittleEndian(body[offset..]);
        offset += 4;
        var nextSequence = BinaryPrimitives.ReadUInt64LittleEndian(body[offset..]);
        offset += 8;
        var hasSnapshot = body[offset++] != 0;
        var lastSnapshot = hasSnapshot ? DecodeSnapshotRef(body, ref offset) : null;

        return new State
        {
            Format = format,
            CurrentJournal = currentJournal,
            NextSequence = nextSequence,
            LastSnapshot = lastSnapshot,
        };
    }

    internal static void WriteEncoded(State manifest, Span<byte> destination)
    {
        if (destination.Length < ComputeEncodedLength(manifest))
            throw new ArgumentException("Destination span is too small for the encoded manifest.", nameof(destination));

        var offset = 0;

        // File envelope: magic + version header, variable body, CRC32C footer over the body bytes.
        Magic.CopyTo(destination);
        offset += Magic.Length;
        destination[offset++] = Version;

        var bodyStart = offset;
        BinaryPrimitives.WriteInt32LittleEndian(destination[offset..], manifest.Format);
        offset += 4;
        BinaryPrimitives.WriteInt32LittleEndian(destination[offset..], manifest.CurrentJournal);
        offset += 4;
        BinaryPrimitives.WriteUInt64LittleEndian(destination[offset..], manifest.NextSequence);
        offset += 8;

        // Snapshot section is optional; absence is encoded as a single zero flag byte.
        if (manifest.LastSnapshot == null)
        {
            destination[offset++] = 0;
        }
        else
        {
            destination[offset++] = 1;
            BinaryPrimitives.WriteInt32LittleEndian(destination[offset..], manifest.LastSnapshot.Index);
            offset += 4;
            BinaryPrimitives.WriteUInt64LittleEndian(destination[offset..], manifest.LastSnapshot.LastAppliedSequence);
            offset += 8;
            BinaryPrimitives.WriteInt32LittleEndian(destination[offset..], manifest.LastSnapshot.ReplayFromJournalSegment);
            offset += 4;
            var createdMs = new DateTimeOffset(manifest.LastSnapshot.CreatedUtc).ToUnixTimeMilliseconds();
            BinaryPrimitives.WriteInt64LittleEndian(destination[offset..], createdMs);
            offset += 8;
        }

        var crcPayload = destination[bodyStart..offset];

        // Footer CRC protects the manifest body against torn or partial writes on disk.
        BinaryPrimitives.WriteUInt32LittleEndian(destination[offset..], Crc32C.Compute(crcPayload));
    }

    /// <summary>Encodes a segment-roll manifest update on the journal hot path.</summary>
    /// <param name="format">Manifest format field.</param>
    /// <param name="currentJournal">Updated current journal segment index.</param>
    /// <param name="nextSequence">Updated next journal sequence.</param>
    /// <param name="snapshot">Snapshot reference copied from the previous manifest (optional).</param>
    /// <param name="destination">Output buffer; must be at least <see cref="ComputeRollEncodedLength" /> bytes.</param>
    /// <returns>Total encoded byte length written to <paramref name="destination" />.</returns>
    /// <exception cref="ArgumentException">Thrown when <paramref name="destination" /> is too small for the encoded roll manifest.</exception>
    internal static int WriteRollEncoded(int format, int currentJournal, ulong nextSequence, SnapshotRef? snapshot, Span<byte> destination)
    {
        var encodedLength = ComputeRollEncodedLength(snapshot);
        if (destination.Length < encodedLength)
            throw new ArgumentException("Destination span is too small for the encoded roll manifest.", nameof(destination));

        var offset = 0;

        // Roll encoding mirrors WriteEncoded but accepts a pre-encoded snapshot path span to avoid allocations.
        Magic.CopyTo(destination);
        offset += Magic.Length;
        destination[offset++] = Version;

        var bodyStart = offset;
        BinaryPrimitives.WriteInt32LittleEndian(destination[offset..], format);
        offset += 4;
        BinaryPrimitives.WriteInt32LittleEndian(destination[offset..], currentJournal);
        offset += 4;
        BinaryPrimitives.WriteUInt64LittleEndian(destination[offset..], nextSequence);
        offset += 8;

        // Journal rolls copy the prior snapshot reference verbatim when one exists.
        if (snapshot == null)
        {
            destination[offset++] = 0;
        }
        else
        {
            destination[offset++] = 1;

            BinaryPrimitives.WriteInt32LittleEndian(destination[offset..], snapshot.Index);
            offset += 4;
            BinaryPrimitives.WriteUInt64LittleEndian(destination[offset..], snapshot.LastAppliedSequence);
            offset += 8;
            BinaryPrimitives.WriteInt32LittleEndian(destination[offset..], snapshot.ReplayFromJournalSegment);
            offset += 4;
            var createdMs = new DateTimeOffset(snapshot.CreatedUtc).ToUnixTimeMilliseconds();
            BinaryPrimitives.WriteInt64LittleEndian(destination[offset..], createdMs);
            offset += 8;
        }

        // CRC spans the roll body only; header magic/version are excluded like the full manifest encode path.
        BinaryPrimitives.WriteUInt32LittleEndian(destination[offset..], Crc32C.Compute(destination[bodyStart..offset]));
        return encodedLength;
    }

    private static SnapshotRef DecodeSnapshotRef(ReadOnlySpan<byte> body, ref int offset)
    {
        if (body.Length < offset + RollSnapshotSectionLength)
            throw new InvalidDataException("Manifest snapshot section is truncated.");

        var snapshotIndex = BinaryPrimitives.ReadInt32LittleEndian(body[offset..]);
        offset += 4;
        var lastAppliedSequence = BinaryPrimitives.ReadUInt64LittleEndian(body[offset..]);
        offset += 8;
        var replayFromJournalSegment = BinaryPrimitives.ReadInt32LittleEndian(body[offset..]);
        offset += 4;
        var createdMs = BinaryPrimitives.ReadInt64LittleEndian(body[offset..]);
        offset += 8;
        return new SnapshotRef
        {
            Index = snapshotIndex,
            LastAppliedSequence = lastAppliedSequence,
            ReplayFromJournalSegment = replayFromJournalSegment,
            CreatedUtc = DateTimeOffset.FromUnixTimeMilliseconds(createdMs).UtcDateTime,
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
        body = fileBytes[FileHeaderSize..bodyEnd];
        var expectedCrc = BinaryPrimitives.ReadUInt32LittleEndian(fileBytes[bodyEnd..]);
        if (Crc32C.Compute(body) != expectedCrc)
            throw new InvalidDataException("Manifest file failed CRC validation.");
    }
}
