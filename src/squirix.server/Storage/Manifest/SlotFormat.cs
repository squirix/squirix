using System;
using System.Buffers.Binary;
using System.IO;
using Squirix.Server.Utils;

namespace Squirix.Server.Storage.Manifest;

/// <summary>Layout, encoding and selection rules of the two-slot <c language="csharp">man-current</c> manifest file.</summary>
/// <remarks>
///     <para>
///     The file is exactly two slots of <see cref="SlotSize" /> bytes at offsets 0 and <see cref="SlotSize" />, little-endian throughout. A slot
///     holds one <see cref="RecordSize" />-byte record at its start, the rest of the slot stays zero; the record is the whole manifest:
///     <c language="csharp">magic(4) | version(1) | sequence(8) | currentJournal(4) | nextSequence(8) | hasSnapshot(1)</c>, then
///     <c language="csharp">snapshotIndex(4) | lastAppliedSequence(8) | replayFromJournalSegment(4) | createdUtcTicks(8)</c>, then
///     <c language="csharp">zero(10) | crc32c(4)</c>. The checksum covers every byte before it.
///     The record fits the first sector of its slot, and a write to one slot never touches the other.
///     </para>
///     <para>
///     A slot is valid when its magic, version, checksum and manifest fields check and its sequence is not zero. The newest valid slot wins; two
///     valid slots with one sequence are accepted only when they are byte-identical. A file whose two slot records hold no non-zero byte was never
///     published. Any other shape without a valid slot is damage and is never repaired here.
///     </para>
/// </remarks>
internal static class SlotFormat
{
    /// <summary>The size of the manifest file in bytes, for its whole life.</summary>
    internal const int FileSize = SlotSize * 2;

    /// <summary>The size of the record at the start of a slot in bytes.</summary>
    internal const int RecordSize = CrcOffset + sizeof(uint);

    /// <summary>The size of one slot in bytes.</summary>
    internal const int SlotSize = 4096;

    private const int CrcOffset = 60;

    private const int CreatedOffset = ReplayFromOffset + sizeof(int);

    private const int CurrentJournalOffset = SequenceOffset + sizeof(ulong);

    /// <summary>Magic bytes of the single-record pointer written by earlier builds, <c language="csharp">"SQMC"</c>.</summary>
    private const uint EarlierMagic = 0x434D5153u;

    /// <summary>Version of the slot record that held only a manifest index.</summary>
    private const byte EarlierSlotVersion = 1;

    private const int HasSnapshotOffset = NextSequenceOffset + sizeof(ulong);

    private const int LastAppliedOffset = SnapshotIndexOffset + sizeof(int);

    private const int NextSequenceOffset = CurrentJournalOffset + sizeof(int);

    private const int PaddingOffset = CreatedOffset + sizeof(long);

    private const int ReplayFromOffset = LastAppliedOffset + sizeof(ulong);

    private const int SequenceOffset = VersionOffset + sizeof(byte);

    /// <summary>Slot magic bytes, <c language="csharp">"SQMP"</c>.</summary>
    private const uint SlotMagic = 0x504D5153u;

    private const byte SlotVersion = 2;

    private const int SnapshotIndexOffset = HasSnapshotOffset + sizeof(byte);

    private const int VersionOffset = sizeof(uint);

    private enum SlotState
    {
        Absent = 0,
        Valid = 1,
        Torn = 2,
    }

    /// <summary>Encodes a record.</summary>
    /// <param name="record">The destination of at least <see cref="RecordSize" /> bytes.</param>
    /// <param name="manifest">The manifest to encode; it must satisfy the rules a reader applies.</param>
    /// <param name="sequence">The sequence number of the record, at least one.</param>
    /// <exception cref="ArgumentException">The destination is too small, or the manifest or sequence is one a reader would reject.</exception>
    internal static void Encode(Span<byte> record, State manifest, ulong sequence)
    {
        if (record.Length < RecordSize)
            throw new ArgumentException("Destination span is too small for a manifest slot record.", nameof(record));

        if (sequence == 0UL || !IsValid(manifest))
            throw new ArgumentException("The manifest or its slot sequence cannot be stored in a manifest slot.", nameof(manifest));

        record[..RecordSize].Clear();
        BinaryPrimitives.WriteUInt32LittleEndian(record, SlotMagic);
        record[VersionOffset] = SlotVersion;
        BinaryPrimitives.WriteUInt64LittleEndian(record[SequenceOffset..], sequence);
        BinaryPrimitives.WriteInt32LittleEndian(record[CurrentJournalOffset..], manifest.CurrentJournal);
        BinaryPrimitives.WriteUInt64LittleEndian(record[NextSequenceOffset..], manifest.NextSequence);
        if (manifest.LastSnapshot is { } snapshot)
        {
            record[HasSnapshotOffset] = 1;
            BinaryPrimitives.WriteInt32LittleEndian(record[SnapshotIndexOffset..], snapshot.Index);
            BinaryPrimitives.WriteUInt64LittleEndian(record[LastAppliedOffset..], snapshot.LastAppliedSequence);
            BinaryPrimitives.WriteInt32LittleEndian(record[ReplayFromOffset..], snapshot.ReplayFromJournalSegment);
            BinaryPrimitives.WriteInt64LittleEndian(record[CreatedOffset..], snapshot.CreatedUtc.Ticks);
        }

        BinaryPrimitives.WriteUInt32LittleEndian(record[CrcOffset..], Crc32C.Compute(record[..CrcOffset]));
    }

    /// <summary>Applies the slot rule to the two records of a manifest file.</summary>
    /// <param name="first">The bytes present of the record in slot 0, at most <see cref="RecordSize" />.</param>
    /// <param name="second">The bytes present of the record in slot 1, at most <see cref="RecordSize" />.</param>
    /// <param name="fileLength">The length of the file in bytes.</param>
    /// <param name="path">The file path, for messages.</param>
    /// <returns>The newest valid record, or a never published file.</returns>
    /// <exception cref="InvalidDataException">The file was written by an earlier build or holds no valid slot.</exception>
    internal static SlotSelection Select(ReadOnlySpan<byte> first, ReadOnlySpan<byte> second, long fileLength, string path)
    {
        if (IsEarlierBuildRecord(first) || IsEarlierBuildRecord(second))
            throw new InvalidDataException($"Manifest at {path} was written by an earlier build and is not supported; the file was left unchanged.");

        if (fileLength > FileSize)
            throw new InvalidDataException($"Manifest at {path} has invalid length {fileLength}.");

        var firstState = Classify(first, out var firstManifest, out var firstSequence);
        var secondState = Classify(second, out var secondManifest, out var secondSequence);
        return (firstState, secondState) switch
        {
            (SlotState.Valid, SlotState.Valid) when firstSequence > secondSequence => new SlotSelection(firstManifest, 0, firstSequence),
            (SlotState.Valid, SlotState.Valid) when secondSequence > firstSequence => new SlotSelection(secondManifest, 1, secondSequence),
            (SlotState.Valid, SlotState.Valid) when first.SequenceEqual(second) => new SlotSelection(firstManifest, 0, firstSequence),
            (SlotState.Valid, SlotState.Valid) => throw new InvalidDataException($"Manifest at {path} has two different records with one sequence."),
            (SlotState.Valid, _) => new SlotSelection(firstManifest, 0, firstSequence),
            (_, SlotState.Valid) => new SlotSelection(secondManifest, 1, secondSequence),
            (SlotState.Absent, SlotState.Absent) => new SlotSelection(new State(), 1, 0UL),
            _ => throw new InvalidDataException($"Manifest at {path} has no valid slot."),
        };
    }

    private static SlotState Classify(ReadOnlySpan<byte> record, out State manifest, out ulong sequence)
    {
        manifest = new State();
        sequence = 0UL;
        if (!record.ContainsAnyExcept<byte>(0))
            return SlotState.Absent;

        if (record.Length < RecordSize || BinaryPrimitives.ReadUInt32LittleEndian(record) != SlotMagic || record[VersionOffset] != SlotVersion ||
            Crc32C.Compute(record[..CrcOffset]) != BinaryPrimitives.ReadUInt32LittleEndian(record[CrcOffset..]))
            return SlotState.Torn;

        var decoded = DecodeOrNull(record, out var decodedSequence);
        if (decoded == null)
            return SlotState.Torn;

        manifest = decoded;
        sequence = decodedSequence;
        return SlotState.Valid;
    }

    private static State? DecodeOrNull(ReadOnlySpan<byte> record, out ulong sequence)
    {
        sequence = BinaryPrimitives.ReadUInt64LittleEndian(record[SequenceOffset..]);
        var hasSnapshot = record[HasSnapshotOffset];
        var snapshotIndex = BinaryPrimitives.ReadInt32LittleEndian(record[SnapshotIndexOffset..]);
        var lastApplied = BinaryPrimitives.ReadUInt64LittleEndian(record[LastAppliedOffset..]);
        var replayFrom = BinaryPrimitives.ReadInt32LittleEndian(record[ReplayFromOffset..]);
        var createdTicks = BinaryPrimitives.ReadInt64LittleEndian(record[CreatedOffset..]);
        var paddingIsZero = !record[PaddingOffset..CrcOffset].ContainsAnyExcept<byte>(0);
        if (sequence == 0UL || !paddingIsZero || hasSnapshot > 1)
            return null;

        SnapshotRef? snapshot = null;
        if (hasSnapshot == 1)
        {
            if (!WireBounds.TryUtcFromTicks(createdTicks, out var createdUtc))
                return null;

            snapshot = new SnapshotRef
            {
                Index = snapshotIndex,
                LastAppliedSequence = lastApplied,
                ReplayFromJournalSegment = replayFrom,
                CreatedUtc = createdUtc,
            };
        }

        if (hasSnapshot == 0 && (snapshotIndex != 0 || lastApplied != 0UL || replayFrom != 0 || createdTicks != 0))
            return null;

        var manifest = new State
        {
            CurrentJournal = BinaryPrimitives.ReadInt32LittleEndian(record[CurrentJournalOffset..]),
            NextSequence = BinaryPrimitives.ReadUInt64LittleEndian(record[NextSequenceOffset..]),
            LastSnapshot = snapshot,
        };
        return IsValid(manifest) ? manifest : null;
    }

    private static bool IsEarlierBuildRecord(ReadOnlySpan<byte> record)
    {
        if (record.Length < sizeof(uint))
            return false;

        var magic = BinaryPrimitives.ReadUInt32LittleEndian(record);
        return magic == EarlierMagic || (magic == SlotMagic && record.Length > VersionOffset && record[VersionOffset] == EarlierSlotVersion);
    }

    private static bool IsValid(State m) =>
        m is { CurrentJournal: >= 1, NextSequence: >= 1UL } && (m.LastSnapshot is not { } s || s is { Index: > 0, ReplayFromJournalSegment: >= 0 });
}
