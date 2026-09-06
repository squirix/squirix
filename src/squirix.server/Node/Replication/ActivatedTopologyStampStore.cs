using System;
using System.Buffers;
using System.Buffers.Binary;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.Storage;
using Squirix.Server.Utils;

namespace Squirix.Server.Node.Replication;

/// <summary>Reads and atomically publishes the activated topology identity stamp.</summary>
/// <remarks>
/// The stamp freezes the topology a node was activated with. A restart whose configured identity
/// differs is refused unless an offline bootstrap rewrote the stamp first, so live and stopped
/// topology changes cannot slip in outside the single authorized migration path.
/// </remarks>
[Immutable]
internal sealed class ActivatedTopologyStampStore
{
    private const int FingerprintLength = 32;
    private const uint Magic = 0x53545153U;
    private const int MaximumStampBytes = 256;
    private const int StampLength = 4 + 2 + 8 + 4 + FingerprintLength + 4;
    private const ushort Version = 1;
    private readonly string _tempPath;

    /// <summary>Initializes a new instance of the <see cref="ActivatedTopologyStampStore" /> class.</summary>
    /// <param name="dataDirectory">Exclusive node data directory.</param>
    internal ActivatedTopologyStampStore(string dataDirectory)
    {
        var directory = FilePathValidator.ResolveValidatedDirectoryPath(dataDirectory);
        StampPath = PathEx.Combine(directory, "topology.stamp");
        _tempPath = PathEx.Combine(directory, "topology.stamp.tmp");
    }

    /// <summary>Gets the durable stamp path.</summary>
    internal string StampPath { get; }

    /// <summary>Flushes a stamp and atomically replaces the published version.</summary>
    /// <param name="stamp">Stamp to publish.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>A task that completes after atomic publication.</returns>
    /// <exception cref="InvalidOperationException">Thrown when the fingerprint is not exactly 32 bytes.</exception>
    internal async Task PublishAsync(ActivatedTopologyStamp stamp, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(stamp);
        if (stamp.Fingerprint.Length != FingerprintLength)
            throw new InvalidOperationException("Activated topology fingerprint must be exactly 32 bytes.");

        var bytes = ArrayPool<byte>.Shared.Rent(StampLength);
        try
        {
            BinaryPrimitives.WriteUInt32LittleEndian(bytes, Magic);
            BinaryPrimitives.WriteUInt16LittleEndian(bytes.AsSpan(4), Version);
            BinaryPrimitives.WriteUInt64LittleEndian(bytes.AsSpan(6), stamp.Generation);
            BinaryPrimitives.WriteInt32LittleEndian(bytes.AsSpan(14), stamp.ReplicaCount);
            stamp.Fingerprint.Span.CopyTo(bytes.AsSpan(18, FingerprintLength));
            BinaryPrimitives.WriteUInt32LittleEndian(bytes.AsSpan(18 + FingerprintLength), Crc32C.Compute(bytes.AsSpan(0, 18 + FingerprintLength)));

            cancellationToken.ThrowIfCancellationRequested();
            var published = false;
            try
            {
                using (var handle = File.OpenHandle(_tempPath, FileMode.Create, FileAccess.Write, FileShare.None, FileOptions.WriteThrough))
                {
                    await RandomAccess.WriteAsync(handle, bytes.AsMemory(0, StampLength), 0L, cancellationToken).ConfigureAwait(false);
                    RandomAccess.FlushToDisk(handle);
                }

                _ = FileEx.PublishFile(_tempPath, StampPath);
                published = true;
            }
            finally
            {
                if (!published)
                    _ = FileEx.TryDeleteFile(_tempPath);
            }
        }
        finally
        {
            ArrayPool<byte>.Shared.ReturnCleared(bytes);
        }
    }

    /// <summary>Reads and validates the stamp, or returns null when the node was never activated.</summary>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The decoded stamp, or null.</returns>
    /// <exception cref="InvalidDataException">Thrown when the stamp is corrupt or unsupported.</exception>
    internal async Task<ActivatedTopologyStamp?> ReadAsync(CancellationToken cancellationToken)
    {
        if (!File.Exists(StampPath))
            return null;

        // Open one handle and validate the length observed through it: a hostile oversized file
        // must fail without materializing its contents, and the exact byte count read from the
        // same handle is what Decode validates, closing the check-to-read race.
        try
        {
            using var handle = File.OpenHandle(StampPath, FileMode.Open, FileAccess.Read, FileShare.Read, FileOptions.SequentialScan);
            var length = RandomAccess.GetLength(handle);
            if (length > MaximumStampBytes)
                throw new InvalidDataException("Activated topology stamp length is invalid.");

            var count = int.CreateChecked(length);
            var bytes = ArrayPool<byte>.Shared.Rent(count);
            try
            {
                var read = 0;
                while (read < count)
                {
                    var received = await RandomAccess.ReadAsync(handle, bytes.AsMemory(read, count - read), read, cancellationToken).ConfigureAwait(false);
                    if (received == 0)
                        break;
                    read += received;
                }

                if (read != count)
                    throw new InvalidDataException("Activated topology stamp length is invalid.");

                var exact = GC.AllocateUninitializedArray<byte>(count);
                bytes.AsSpan(0, count).CopyTo(exact);
                return Decode(exact);
            }
            finally
            {
                ArrayPool<byte>.Shared.ReturnCleared(bytes);
            }
        }
        catch (Exception exception) when (exception is FileNotFoundException or DirectoryNotFoundException)
        {
            return null;
        }
    }

    private static ActivatedTopologyStamp Decode(byte[] bytes)
    {
        if (bytes.Length != StampLength)
            throw new InvalidDataException("Activated topology stamp length is invalid.");
        if (BinaryPrimitives.ReadUInt32LittleEndian(bytes) != Magic || BinaryPrimitives.ReadUInt16LittleEndian(bytes.AsSpan(4)) != Version)
            throw new InvalidDataException("Activated topology stamp header is invalid or unsupported.");

        var expectedChecksum = BinaryPrimitives.ReadUInt32LittleEndian(bytes.AsSpan(18 + FingerprintLength));
        if (Crc32C.Compute(bytes.AsSpan(0, 18 + FingerprintLength)) != expectedChecksum)
            throw new InvalidDataException("Activated topology stamp checksum is invalid.");

        return new ActivatedTopologyStamp
        {
            Generation = BinaryPrimitives.ReadUInt64LittleEndian(bytes.AsSpan(6)),
            ReplicaCount = BinaryPrimitives.ReadInt32LittleEndian(bytes.AsSpan(14)),
            Fingerprint = OwnedBufferKit.CopyToOwned(bytes.AsSpan(18, FingerprintLength)),
        };
    }

    /// <summary>Exact-size owned byte buffer helper for decoder output.</summary>
    /// <remarks>
    /// The decoded stamp outlives the source buffer, so the fingerprint needs an owned copy that the
    /// span cannot lend. The escape is exact-size and caller-retained.
    /// </remarks>
    private static class OwnedBufferKit
    {
#pragma warning disable ZA0302 // ZA0302: exact-size owned buffer escape; the decoder output retains ownership.
        internal static byte[] CopyToOwned(ReadOnlySpan<byte> source)
        {
            var owned = new byte[source.Length];
            source.CopyTo(owned);
            return owned;
        }
#pragma warning restore ZA0302
    }
}
