using System;
using System.Buffers;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.IO;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Storage;
using Squirix.Server.Utils;

namespace Squirix.Server.Node.Replication;

/// <summary>Reads and atomically publishes the checksummed bootstrap manifest.</summary>
[SuppressMessage("StyleCop.CSharp.OrderingRules", "SA1204:Static members should appear before non-static members", Justification = "Public operation flow precedes private binary codec helpers for readability.")]
internal sealed class BootstrapManifestStore
{
    private const uint Magic = 0x42525153U;
    private const ushort Version = 1;
    private const int HeaderLength = 10;
    private const int ChecksumLength = 4;
    private const int MaximumManifestBytes = 16 * 1024 * 1024;
    private const int MaximumGroups = 100_000;
    private const int MaximumStringBytes = 4096;
    private readonly string _tempPath;

    /// <summary>Initializes a new instance of the <see cref="BootstrapManifestStore" /> class.</summary>
    /// <param name="dataDirectory">Exclusive stopped-cluster data directory.</param>
    internal BootstrapManifestStore(string dataDirectory)
    {
        var directory = FilePathValidator.ResolveValidatedDirectoryPath(dataDirectory);
        ManifestPath = PathEx.Combine(directory, "bootstrap.manifest");
        _tempPath = PathEx.Combine(directory, "bootstrap.manifest.tmp");
    }

    /// <summary>Gets the durable manifest path.</summary>
    internal string ManifestPath { get; }

    /// <summary>Reads and validates the manifest, or returns null when preparation has not begun.</summary>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The decoded manifest, or null.</returns>
    /// <exception cref="InvalidDataException">Thrown when the manifest is corrupt or unsupported.</exception>
    internal async Task<BootstrapManifest?> ReadAsync(CancellationToken cancellationToken)
    {
        if (!File.Exists(ManifestPath))
            return null;

        var bytes = await File.ReadAllBytesAsync(ManifestPath, cancellationToken).ConfigureAwait(false);
        return DecodeFile(bytes);
    }

    internal static BootstrapManifest DecodeFile(byte[] bytes)
    {
        if (bytes.Length is < HeaderLength + ChecksumLength or > MaximumManifestBytes)
            throw new InvalidDataException("Bootstrap manifest length is invalid.");
        if (BinaryPrimitives.ReadUInt32LittleEndian(bytes) != Magic || BinaryPrimitives.ReadUInt16LittleEndian(bytes.AsSpan(4)) != Version)
            throw new InvalidDataException("Bootstrap manifest header is invalid or unsupported.");

        var payloadLength = BinaryPrimitives.ReadInt32LittleEndian(bytes.AsSpan(6));
        if (payloadLength <= 0 || HeaderLength + payloadLength + ChecksumLength != bytes.Length)
            throw new InvalidDataException("Bootstrap manifest declared length is invalid.");
        var expectedChecksum = BinaryPrimitives.ReadUInt32LittleEndian(bytes.AsSpan(HeaderLength + payloadLength));
        if (Crc32C.Compute(bytes.AsSpan(HeaderLength, payloadLength)) != expectedChecksum)
            throw new InvalidDataException("Bootstrap manifest checksum is invalid.");

        return Decode(bytes, HeaderLength, payloadLength);
    }

    /// <summary>Flushes a complete manifest and atomically replaces the published version.</summary>
    /// <param name="manifest">Manifest to publish.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>A task that completes after atomic publication.</returns>
    internal async Task PublishAsync(BootstrapManifest manifest, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(manifest);
        var payload = Encode(manifest);
        var byteCount = HeaderLength + payload.Length + ChecksumLength;
        var bytes = ArrayPool<byte>.Shared.Rent(byteCount);
        BinaryPrimitives.WriteUInt32LittleEndian(bytes, Magic);
        BinaryPrimitives.WriteUInt16LittleEndian(bytes.AsSpan(4), Version);
        BinaryPrimitives.WriteInt32LittleEndian(bytes.AsSpan(6), payload.Length);
        payload.CopyTo(bytes.AsSpan(HeaderLength));
        BinaryPrimitives.WriteUInt32LittleEndian(bytes.AsSpan(HeaderLength + payload.Length), Crc32C.Compute(payload));

        cancellationToken.ThrowIfCancellationRequested();
        var published = false;
        try
        {
            using (var handle = File.OpenHandle(_tempPath, FileMode.Create, FileAccess.Write, FileShare.None, FileOptions.WriteThrough))
            {
                await RandomAccess.WriteAsync(handle, bytes.AsMemory(0, byteCount), 0L, cancellationToken).ConfigureAwait(false);
                RandomAccess.FlushToDisk(handle);
            }

            _ = FileEx.PublishFile(_tempPath, ManifestPath);
            published = true;
        }
        finally
        {
            if (!published)
                _ = FileEx.TryDeleteFile(_tempPath);
            ArrayPool<byte>.Shared.ReturnCleared(bytes);
        }
    }

    private static BootstrapManifest Decode(byte[] bytes, int offset, int length)
    {
        try
        {
            using var stream = new MemoryStream(bytes, offset, length, false, false);
            using var reader = new BinaryReader(stream, Encoding.UTF8, false);
            var sourceClusterId = ReadString(reader);
            var sourceFingerprint = ReadBytes(reader, 32);
            var sourceGeneration = reader.ReadUInt64();
            var targetFingerprint = ReadBytes(reader, 32);
            var targetGeneration = reader.ReadUInt64();
            var targetReplicaCount = reader.ReadInt32();
            var count = reader.ReadInt32();
            if (count is < 0 or > MaximumGroups)
                throw new InvalidDataException("Bootstrap manifest group count is invalid.");

            var groups = new List<BootstrapGroupProgress>(count);
            for (var index = 0; index < count; index++)
            {
                var groupId = ReadString(reader);
                var state = reader.ReadByte() switch
                {
                    0 => BootstrapGroupState.Pending,
                    1 => BootstrapGroupState.Prepared,
                    2 => BootstrapGroupState.Installed,
                    3 => BootstrapGroupState.Verified,
                    _ => throw new InvalidDataException("Bootstrap manifest group state is invalid."),
                };
                groups.Add(new BootstrapGroupProgress(groupId, state));
            }

            if (stream.Position != stream.Length)
                throw new InvalidDataException("Bootstrap manifest contains trailing bytes.");
            return new BootstrapManifest
            {
                FormatVersion = Version,
                Groups = groups,
                SourceClusterId = sourceClusterId,
                SourceFingerprint = sourceFingerprint,
                SourceGeneration = sourceGeneration,
                TargetFingerprint = targetFingerprint,
                TargetGeneration = targetGeneration,
                TargetReplicaCount = targetReplicaCount,
            };
        }
        catch (EndOfStreamException exception)
        {
            throw new InvalidDataException("Bootstrap manifest payload is truncated.", exception);
        }
    }

    [SuppressMessage("Usage", "MA0045:Use async disposable", Justification = "BinaryWriter and MemoryStream are in-memory and are intentionally encoded synchronously before durable asynchronous I/O.")]
    private static byte[] Encode(BootstrapManifest manifest)
    {
        // Reject reader-incompatible shapes before allocating the payload buffer, mirroring the Decode
        // limits, so Encode never produces a file this reader classifies as corrupt.
        if (manifest.Groups.Count > MaximumGroups)
            throw new InvalidOperationException("Bootstrap manifest group count exceeds the maximum.");
        if (Encoding.UTF8.GetByteCount(manifest.SourceClusterId) > MaximumStringBytes)
            throw new InvalidOperationException("Bootstrap manifest source cluster identifier exceeds the maximum length.");
        for (var index = 0; index < manifest.Groups.Count; index++)
        {
            if (Encoding.UTF8.GetByteCount(manifest.Groups[index].GroupId) > MaximumStringBytes)
                throw new InvalidOperationException("Bootstrap manifest group identifier exceeds the maximum length.");
        }

        using var stream = new MemoryStream();
        using (var writer = new BinaryWriter(stream, Encoding.UTF8, true))
        {
            WriteString(writer, manifest.SourceClusterId);
            WriteBytes(writer, manifest.SourceFingerprint.Span);
            writer.Write(manifest.SourceGeneration);
            WriteBytes(writer, manifest.TargetFingerprint.Span);
            writer.Write(manifest.TargetGeneration);
            writer.Write(manifest.TargetReplicaCount);
            writer.Write(manifest.Groups.Count);
            for (var index = 0; index < manifest.Groups.Count; index++)
            {
                WriteString(writer, manifest.Groups[index].GroupId);
                writer.Write(StateByte(manifest.Groups[index].State));
            }
        }

        if (stream.Length > MaximumManifestBytes - HeaderLength - ChecksumLength)
            throw new InvalidOperationException("Bootstrap manifest exceeds the maximum size.");
        return stream.ToArray();
    }

    private static byte[] ReadBytes(BinaryReader reader, int requiredLength)
    {
        var length = reader.ReadInt32();
        if (length != requiredLength)
            throw new InvalidDataException($"Bootstrap manifest byte field must be exactly {requiredLength} bytes.");
        var bytes = reader.ReadBytes(length);
        if (bytes.Length != length)
            throw new EndOfStreamException();
        return bytes;
    }

    private static byte StateByte(BootstrapGroupState state) => state switch
    {
        BootstrapGroupState.Pending => 0,
        BootstrapGroupState.Prepared => 1,
        BootstrapGroupState.Installed => 2,
        BootstrapGroupState.Verified => 3,
        _ => throw new InvalidDataException("Bootstrap manifest group state is invalid."),
    };

    private static string ReadString(BinaryReader reader)
    {
        var length = reader.ReadInt32();
        if (length is <= 0 or > MaximumStringBytes)
            throw new InvalidDataException("Bootstrap manifest string length is invalid.");
        var bytes = reader.ReadBytes(length);
        if (bytes.Length != length)
            throw new EndOfStreamException();
        return Encoding.UTF8.GetString(bytes);
    }

    private static void WriteBytes(BinaryWriter writer, ReadOnlySpan<byte> bytes)
    {
        writer.Write(bytes.Length);
        writer.Write(bytes);
    }

    private static void WriteString(BinaryWriter writer, string value)
    {
        ArgumentException.ThrowIfNullOrEmpty(value);
        var bytes = Encoding.UTF8.GetBytes(value);
        writer.Write(bytes.Length);
        writer.Write(bytes);
    }
}
