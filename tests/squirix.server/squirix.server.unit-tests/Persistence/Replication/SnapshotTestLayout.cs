using System;
using System.Buffers.Binary;

namespace Squirix.Server.UnitTests.Persistence.Replication;

/// <summary>
/// On-disk layout of a published replica-group snapshot as the recovery tests decode it:
/// header magic(4) | version(1) | payloadLength(4), a payload opening with four fixed 64-bit fields
/// (configuration generation, included term, included index, commit index), then the CRC32C trailer.
/// </summary>
internal static class SnapshotTestLayout
{
    /// <summary>Gets the header byte count: magic(4) | version(1) | payloadLength(4).</summary>
    internal const int HeaderByteCount = MagicByteCount + VersionByteCount + PayloadLengthFieldByteCount;

    /// <summary>Gets the file offset of the declared payload length.</summary>
    internal const int PayloadLengthFileOffset = MagicByteCount + VersionByteCount;

    /// <summary>Gets the payload offset of the included term, the second fixed 64-bit payload field.</summary>
    internal const int LastIncludedTermPayloadOffset = FixedFieldByteCount;

    /// <summary>Gets the payload offset of the commit index, the fourth fixed 64-bit payload field.</summary>
    internal const int CommitIndexPayloadOffset = 3 * FixedFieldByteCount;

    private const int MagicByteCount = 4;

    private const int VersionByteCount = 1;

    private const int PayloadLengthFieldByteCount = 4;

    private const int FixedFieldByteCount = 8;

    /// <summary>Reads the declared payload length from a raw snapshot file image.</summary>
    /// <param name="bytes">The raw snapshot file bytes.</param>
    /// <returns>The declared payload length.</returns>
    internal static int ReadPayloadLength(byte[] bytes) => BinaryPrimitives.ReadInt32LittleEndian(bytes.AsSpan(PayloadLengthFileOffset, PayloadLengthFieldByteCount));

    /// <summary>Reads the on-disk commit index from a raw snapshot file image.</summary>
    /// <param name="bytes">The raw snapshot file bytes.</param>
    /// <returns>The commit index stored in the payload's fourth fixed field.</returns>
    internal static ulong ReadCommitIndex(byte[] bytes) => BinaryPrimitives.ReadUInt64LittleEndian(bytes.AsSpan(HeaderByteCount + CommitIndexPayloadOffset, FixedFieldByteCount));

    /// <summary>Overwrites the on-disk commit index in a raw snapshot file image.</summary>
    /// <param name="bytes">The raw snapshot file bytes.</param>
    /// <param name="value">The commit index to write.</param>
    internal static void WriteCommitIndex(byte[] bytes, ulong value) => BinaryPrimitives.WriteUInt64LittleEndian(bytes.AsSpan(HeaderByteCount + CommitIndexPayloadOffset, FixedFieldByteCount), value);

    /// <summary>Resolves the CRC32C file offset for the given declared payload length.</summary>
    /// <param name="payloadLength">The declared payload length read from the header.</param>
    /// <returns>The file offset of the CRC32C trailer.</returns>
    internal static int CrcFileOffset(int payloadLength) => HeaderByteCount + payloadLength;
}
