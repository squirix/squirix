using System;
using System.Buffers.Binary;
using System.IO;
using Squirix.Server.Utils;

namespace Squirix.Server.Storage.Replication;

/// <summary>Layout, encoding and selection rules of the two group snapshot slot files.</summary>
/// <remarks>
///     <para>
///     A slot file is <c language="csharp">magic(4) | slotVersion(1) | sequence(8) | imageLength(4) | image | crc32c(4)</c>, little-endian throughout, and its length
///     is exactly <see cref="Overhead" /> plus the image length. The image is the published snapshot image
///     (see <see cref="GroupSnapshotStore.EncodePublished" />); the checksum covers the slot header and the image, so a torn write that persists only
///     a new header over an older, self-valid image is not accepted as newer.
///     </para>
///     <para>
///     A slot is valid when its magic, version, sequence, length and checksum check and its image decodes. A slot that fails any frame check is torn
///     and ignored. A slot whose frame is intact but whose image does not decode, uses another image version, or exceeds the size bound is refused
///     loudly, since it was written whole by a build that this one cannot read. The valid slot with the higher sequence wins; two valid slots with one
///     sequence are accepted only when they are byte-identical.
///     </para>
/// </remarks>
internal static class GroupSnapshotSlots
{
    /// <summary>The number of slot files.</summary>
    internal const int SlotCount = 2;

    /// <summary>Header size: magic(4) + version(1) + sequence(8) + imageLength(4).</summary>
    internal const int HeaderSize = sizeof(uint) + sizeof(byte) + sizeof(ulong) + sizeof(int);

    /// <summary>The bytes a slot adds around its image: header and checksum.</summary>
    internal const int Overhead = HeaderSize + CrcSize;

    private const int CrcSize = sizeof(uint);

    /// <summary>Slot magic bytes, <c language="csharp">"SQGS"</c>.</summary>
    private const uint SlotMagic = 0x53475153u;

    /// <summary>The slot format version.</summary>
    private const byte SlotVersion = 1;

    private const int SequenceOffset = sizeof(uint) + sizeof(byte);

    private const int LengthOffset = SequenceOffset + sizeof(ulong);

    private enum SlotState
    {
        Absent = 0,
        Torn = 1,
        Valid = 2,
    }

    /// <summary>Writes the header of a slot; the image and checksum are filled in separately.</summary>
    /// <param name="slot">The slot buffer of exactly <see cref="Overhead" /> plus the image length bytes.</param>
    /// <param name="sequence">The sequence number of the slot.</param>
    internal static void WriteHeader(Span<byte> slot, ulong sequence)
    {
        BinaryPrimitives.WriteUInt32LittleEndian(slot, SlotMagic);
        slot[sizeof(uint)] = SlotVersion;
        BinaryPrimitives.WriteUInt64LittleEndian(slot[SequenceOffset..], sequence);
        BinaryPrimitives.WriteInt32LittleEndian(slot[LengthOffset..], slot.Length - Overhead);
    }

    /// <summary>Sets the sequence of a slot whose header and image are in place and recomputes its checksum.</summary>
    /// <param name="slot">The slot buffer of exactly <see cref="Overhead" /> plus the image length bytes.</param>
    /// <param name="sequence">The sequence number of the slot.</param>
    internal static void Seal(Span<byte> slot, ulong sequence)
    {
        BinaryPrimitives.WriteUInt64LittleEndian(slot[SequenceOffset..], sequence);
        var crcOffset = slot.Length - CrcSize;
        BinaryPrimitives.WriteUInt32LittleEndian(slot[crcOffset..], Crc32C.Compute(slot[..crcOffset]));
    }

    /// <summary>Applies the slot rule to the bytes of both slot files.</summary>
    /// <param name="first">The bytes of slot 0; ignored when <paramref name="firstExists" /> is <see langword="false" />.</param>
    /// <param name="firstExists">Whether the file of slot 0 exists.</param>
    /// <param name="second">The bytes of slot 1; ignored when <paramref name="secondExists" /> is <see langword="false" />.</param>
    /// <param name="secondExists">Whether the file of slot 1 exists.</param>
    /// <param name="context">The group, size bound and slot paths the rule applies to.</param>
    /// <returns>The selected snapshot, or a selection without a snapshot when none was ever completely published.</returns>
    /// <exception cref="InvalidDataException">A slot is refused, or the slots leave no snapshot to trust although a file exists.</exception>
    internal static GroupSnapshotSelection Select(
        ReadOnlySpan<byte> first,
        bool firstExists,
        ReadOnlySpan<byte> second,
        bool secondExists,
        in GroupSnapshotSlotContext context)
    {
        var location = Path.GetDirectoryName(context.FirstPath) ?? context.FirstPath;
        var firstState = Classify(first, firstExists, context.MaxSnapshotBytes, context.FirstPath, out var firstSequence, out var firstSnapshot);
        var secondState = Classify(second, secondExists, context.MaxSnapshotBytes, context.SecondPath, out var secondSequence, out var secondSnapshot);

        // Each valid slot is checked, not only the selected one: a foreign slot in the older position would otherwise be overwritten by the next publish.
        EnsureOwnGroup(firstState, in firstSnapshot, context.GroupId, context.FirstPath);
        EnsureOwnGroup(secondState, in secondSnapshot, context.GroupId, context.SecondPath);

        // A first publish that never completed leaves at most slot 0, absent or torn; any other shape lost a snapshot that was acknowledged.
        if (firstState != SlotState.Valid && secondState != SlotState.Valid)
            return secondState == SlotState.Absent ? default : throw new InvalidDataException($"Replica group snapshot at '{location}' is corrupt.");

        var useSecond = PrefersSecond(firstState, firstSequence, first, secondState, secondSequence, second, location);
        var slot = useSecond ? 1 : 0;
        var sequence = useSecond ? secondSequence : firstSequence;
        var snapshot = useSecond ? secondSnapshot : firstSnapshot;
        return new GroupSnapshotSelection(snapshot, slot, sequence);
    }

    private static void EnsureOwnGroup(SlotState state, in GroupSnapshot snapshot, string groupId, string location)
    {
        if (state == SlotState.Valid && !string.Equals(snapshot.GroupId, groupId, StringComparison.Ordinal))
            throw new InvalidDataException($"Replica group snapshot at '{location}' belongs to a different group.");
    }

    private static bool PrefersSecond(
        SlotState firstState,
        ulong firstSequence,
        ReadOnlySpan<byte> first,
        SlotState secondState,
        ulong secondSequence,
        ReadOnlySpan<byte> second,
        string location)
    {
        if (firstState != SlotState.Valid)
            return true;

        var secondValid = secondState == SlotState.Valid;
        return secondValid && firstSequence == secondSequence && !first.SequenceEqual(second)
            ? throw new InvalidDataException($"Replica group snapshot at '{location}' is corrupt.")
            : secondValid && secondSequence > firstSequence;
    }

    private static SlotState Classify(
        ReadOnlySpan<byte> slot,
        bool exists,
        int maxSnapshotBytes,
        string location,
        out ulong sequence,
        out GroupSnapshot snapshot)
    {
        sequence = 0UL;
        snapshot = default;
        if (!exists)
            return SlotState.Absent;

        long maxSlotBytes = maxSnapshotBytes;
        maxSlotBytes += Overhead;
        if (slot.Length > maxSlotBytes)
            throw new InvalidDataException($"Replica group snapshot at '{location}' exceeds the maximum configured size of {maxSnapshotBytes} bytes.");

        if (slot.Length < Overhead || BinaryPrimitives.ReadUInt32LittleEndian(slot) != SlotMagic || slot[sizeof(uint)] != SlotVersion)
            return SlotState.Torn;

        sequence = BinaryPrimitives.ReadUInt64LittleEndian(slot[SequenceOffset..]);
        var imageLength = BinaryPrimitives.ReadInt32LittleEndian(slot[LengthOffset..]);
        if (sequence == 0UL || imageLength < 0 || imageLength != slot.Length - Overhead)
            return SlotState.Torn;

        var crcOffset = slot.Length - CrcSize;
        return Crc32C.Compute(slot[..crcOffset]) != BinaryPrimitives.ReadUInt32LittleEndian(slot[crcOffset..])
            ? SlotState.Torn
            : DecodeImage(slot.Slice(HeaderSize, imageLength), maxSnapshotBytes, location, out snapshot);
    }

    private static SlotState DecodeImage(ReadOnlySpan<byte> image, int maxSnapshotBytes, string location, out GroupSnapshot snapshot) =>
        GroupSnapshotStore.TryDecodePublished(image, maxSnapshotBytes, out snapshot, out var version)
            ? SlotState.Valid
            : throw RefusedImage(version, location);

    private static InvalidDataException RefusedImage(byte version, string location) =>
        version == 0 || version == GroupSnapshotStore.PublishedVersion
            ? new InvalidDataException($"Replica group snapshot at '{location}' is corrupt.")
            : new InvalidDataException($"Replica group snapshot at '{location}' uses format version {version}; version {GroupSnapshotStore.PublishedVersion} is required.");
}
