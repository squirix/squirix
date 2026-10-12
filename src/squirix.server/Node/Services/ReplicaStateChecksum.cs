using System;
using System.Buffers.Binary;
using System.Text;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.Utils;

namespace Squirix.Server.Node.Services;

/// <summary>Counts the entries of a state transfer and computes the CRC32C over them in arrival order.</summary>
/// <remarks>
/// Each entry contributes its cache name, key, payload and deadline, each variable part behind its length as a little-endian 32-bit integer and the
/// deadline as a little-endian 64-bit integer of ticks, so no two different streams of entries share a checksum by concatenation. The sender and
/// the receiver of a transfer compute it with this type. Not thread-safe: one transfer feeds one instance, one call at a time.
/// </remarks>
internal sealed class ReplicaStateChecksum
{
    private const int StackLimit = 256;

    private ulong _count;
    private uint _crc = Crc32C.InitialValue;

    /// <summary>Gets what was added so far.</summary>
    internal ReplicaStateTotals Totals => new(_count, Crc32C.Finalize(_crc));

    /// <summary>Adds the next entry of the transfer.</summary>
    /// <param name="entry">The entry.</param>
    internal void Add(in ReplicaStateEntry entry)
    {
        AppendText(entry.CacheName);
        AppendBytes(entry.Key.Span);
        AppendBytes(entry.Payload.Span);
        Span<byte> ticks = stackalloc byte[sizeof(long)];
        BinaryPrimitives.WriteInt64LittleEndian(ticks, entry.ExpiresUtcTicks);
        _crc = Crc32C.Append(_crc, ticks);
        _count++;
    }

    /// <summary>Starts over, as at the beginning of a transfer.</summary>
    internal void Reset()
    {
        _count = 0;
        _crc = Crc32C.InitialValue;
    }

    private void AppendBytes(ReadOnlySpan<byte> bytes)
    {
        AppendLength(bytes.Length);
        _crc = Crc32C.Append(_crc, bytes);
    }

    private void AppendLength(int length)
    {
        Span<byte> prefix = stackalloc byte[sizeof(int)];
        BinaryPrimitives.WriteInt32LittleEndian(prefix, length);
        _crc = Crc32C.Append(_crc, prefix);
    }

    private void AppendText(string text)
    {
        var length = Encoding.UTF8.GetByteCount(text);
        if (length > StackLimit)
        {
            AppendBytes(Encoding.UTF8.GetBytes(text));
            return;
        }

        Span<byte> bytes = stackalloc byte[StackLimit];
        AppendBytes(bytes[..Encoding.UTF8.GetBytes(text, bytes)]);
    }
}
