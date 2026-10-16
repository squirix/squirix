using System;
using System.Buffers.Binary;
using System.IO;
using Squirix.Server.Utils;

namespace Squirix.Server.Storage.Replication;

/// <summary>Layout and selection rules of the two group log generation files.</summary>
/// <remarks>
///     <para>
///     A generation file is <c language="csharp">magic(4) | version(1) | generation(8) | crc32c(4)</c>, little-endian, followed by the log frames. The
///     checksum covers the first <see cref="CrcOffset" /> bytes. A rewrite fills the inactive file with its frames, flushes them, and only then writes
///     and flushes the header, so a header that checks always covers frames that reached stable storage and the header write is the single commit point.
///     </para>
///     <para>
///     A file is valid when its header checks. The valid file with the higher generation wins; two valid files with one generation are refused. A file
///     whose header is intact but uses another format version is refused loudly. When no file is valid, only the shape of a log that was never completely
///     created is accepted as no log; any other shape could hide acknowledged frames and is refused.
///     </para>
/// </remarks>
internal static class GroupLogSlots
{
    /// <summary>The number of generation files.</summary>
    internal const int SlotCount = 2;

    /// <summary>Header size: magic(4) + version(1) + generation(8) + crc32c(4).</summary>
    internal const int HeaderSize = sizeof(uint) + sizeof(byte) + sizeof(ulong) + sizeof(uint);

    /// <summary>The number of header bytes the checksum covers.</summary>
    private const int CrcOffset = HeaderSize - sizeof(uint);

    /// <summary>File magic bytes, <c language="csharp">"SQGL"</c>.</summary>
    private const uint FileMagic = 0x4C475153u;

    /// <summary>The file format version.</summary>
    private const byte FileVersion = 1;

    private const int GenerationOffset = sizeof(uint) + sizeof(byte);

    private enum SlotState
    {
        Absent = 0,
        Invalid = 1,
        Valid = 2,
    }

    /// <summary>Writes the sealed header of a generation file.</summary>
    /// <param name="header">The destination of exactly <see cref="HeaderSize" /> bytes.</param>
    /// <param name="generation">The generation, at least 1.</param>
    internal static void WriteHeader(Span<byte> header, ulong generation)
    {
        BinaryPrimitives.WriteUInt32LittleEndian(header, FileMagic);
        header[sizeof(uint)] = FileVersion;
        BinaryPrimitives.WriteUInt64LittleEndian(header[GenerationOffset..], generation);
        BinaryPrimitives.WriteUInt32LittleEndian(header[CrcOffset..], Crc32C.Compute(header[..CrcOffset]));
    }

    /// <summary>Applies the generation rule to the heads of both files.</summary>
    /// <param name="firstHead">The first bytes of file 0, up to <see cref="HeaderSize" />.</param>
    /// <param name="firstLength">The length of file 0, or -1 when it does not exist.</param>
    /// <param name="secondHead">The first bytes of file 1, up to <see cref="HeaderSize" />.</param>
    /// <param name="secondLength">The length of file 1, or -1 when it does not exist.</param>
    /// <param name="location">The group directory, for diagnostics.</param>
    /// <returns>The active slot and its generation, or generation 0 when no log was ever completely created.</returns>
    /// <exception cref="InvalidDataException">A file is refused, or the files leave no log to trust although one exists.</exception>
    internal static (int Slot, ulong Generation) Select(
        ReadOnlySpan<byte> firstHead,
        long firstLength,
        ReadOnlySpan<byte> secondHead,
        long secondLength,
        string location)
    {
        var firstState = Classify(firstHead, firstLength, location, out var firstGeneration);
        var secondState = Classify(secondHead, secondLength, location, out var secondGeneration);
        if (firstState == SlotState.Valid && secondState == SlotState.Valid)
        {
            var newer = secondGeneration > firstGeneration ? 1 : 0;
            return firstGeneration == secondGeneration
                ? throw new InvalidDataException($"Replica group log at '{location}' is corrupt.")
                : (newer, Math.Max(firstGeneration, secondGeneration));
        }

        if (firstState == SlotState.Valid)
            return (0, firstGeneration);

        if (secondState == SlotState.Valid)
            return (1, secondGeneration);

        // A first creation that never completed leaves at most file 0, absent or no longer than a header; any other shape lost a log that was acknowledged.
        return secondState == SlotState.Absent && firstLength <= HeaderSize
            ? default
            : throw new InvalidDataException($"Replica group log at '{location}' is corrupt.");
    }

    private static SlotState Classify(ReadOnlySpan<byte> head, long length, string location, out ulong generation)
    {
        generation = 0UL;
        if (length < 0)
            return SlotState.Absent;

        if (length < HeaderSize || head.Length < HeaderSize || BinaryPrimitives.ReadUInt32LittleEndian(head) != FileMagic)
            return SlotState.Invalid;

        if (Crc32C.Compute(head[..CrcOffset]) != BinaryPrimitives.ReadUInt32LittleEndian(head[CrcOffset..]))
            return SlotState.Invalid;

        if (head[sizeof(uint)] != FileVersion)
            throw new InvalidDataException($"Replica group log at '{location}' uses format version {head[sizeof(uint)]}; version {FileVersion} is required.");

        generation = BinaryPrimitives.ReadUInt64LittleEndian(head[GenerationOffset..]);
        return generation == 0UL ? SlotState.Invalid : SlotState.Valid;
    }
}
