using System;
using System.Buffers;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Win32.SafeHandles;
using Squirix.Server.Threading;

namespace Squirix.Server.Storage.Replication;

/// <summary>Copies a bounded range of WAL frames while checking each checksum and index.</summary>
internal static class GroupLogFrameCopy
{
    internal static void Copy(
        string sourcePath,
        SafeFileHandle destination,
        (long Start, long End, ulong First, ulong Last) retained,
        long outputStart,
        CancellationToken cancellationToken)
    {
        if (retained.Start == retained.End)
            return;

        using var source = File.OpenHandle(sourcePath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite | FileShare.Delete);
        Span<byte> header = stackalloc byte[GroupLogCodec.FramePreambleByteCount + sizeof(int)];
        var frame = ArrayPool<byte>.Shared.Rent(header.Length);
        try
        {
            var expected = retained.First;
            var offset = retained.Start;
            while (offset < retained.End)
            {
                cancellationToken.ThrowIfCancellationRequested();
                ReadExactly(source, header, offset);
                if (!GroupLogCodec.TryReadFrameHeaderLength(header, out var length) || length > retained.End - offset)
                    throw new InvalidDataException("The retained WAL frame has an invalid length.");

                if (length > frame.Length)
                {
                    var larger = ArrayPool<byte>.Shared.Rent(length);
                    ArrayPool<byte>.Shared.ReturnCleared(frame);
                    frame = larger;
                }

                var bytes = frame.AsSpan(0, length);
                ReadExactly(source, bytes, offset);
                if (!GroupLogCodec.TryReadFrameFields(bytes, out var index, out _) || index != expected)
                    throw new InvalidDataException("The retained WAL frame is corrupt or out of order.");

                RandomAccess.Write(destination, bytes, outputStart + offset - retained.Start);
                offset += length;
                expected++;
            }

            if (expected - 1UL != retained.Last)
                throw new InvalidDataException("The retained WAL does not reach the expected last index.");
        }
        finally
        {
            ArrayPool<byte>.Shared.ReturnCleared(frame);
        }
    }

    private static void ReadExactly(SafeFileHandle handle, Span<byte> bytes, long offset)
    {
        while (!bytes.IsEmpty)
        {
            var read = RandomAccess.Read(handle, bytes, offset);
            if (read == 0)
                throw new EndOfStreamException("The retained WAL ended inside a frame.");

            bytes = bytes[read..];
            offset += read;
        }
    }

    internal sealed class Preparation : IWorkPoolItem
    {
        private readonly string _source;
        private readonly string _standby;
        private readonly GroupLogDurability _durability;
        private readonly (long Start, long End, ulong First, ulong Last) _retained;
        private CancellationToken _cancellationToken;

        internal Preparation(string source, string standby, GroupLogDurability durability, (long Start, long End, ulong First, ulong Last) retained)
        {
            _source = source;
            _standby = standby;
            _durability = durability;
            _retained = retained;
        }

        void IWorkPoolItem.Execute()
        {
            _cancellationToken.ThrowIfCancellationRequested();
            using var handle = _durability.OpenStandbyForPreparation(_standby, _cancellationToken);
            if (_retained.Start == _retained.End)
                RandomAccess.SetLength(handle, GroupLogSlots.HeaderSize);

            Copy(_source, handle, _retained, GroupLogSlots.HeaderSize, _cancellationToken);
            _cancellationToken.ThrowIfCancellationRequested();
            RandomAccess.FlushToDisk(handle);
        }

        internal async Task RunAsync(CancellationToken shutdownToken, CancellationToken cancellationToken)
        {
            using var lifetime = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, shutdownToken);
            _cancellationToken = lifetime.Token;
            await WorkPool.RunAsync(this, TaskCreationOptions.None, lifetime.Token).ConfigureAwait(false);
        }
    }
}
