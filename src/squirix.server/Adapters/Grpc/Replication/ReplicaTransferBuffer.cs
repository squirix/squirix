using System;
using System.Buffers;
using System.IO;
using System.Threading;
using Google.Protobuf;

namespace Squirix.Server.Adapters.Grpc.Replication;

/// <summary>Owns the bytes borrowed by a parsed transfer request until its RPC finishes.</summary>
internal sealed class ReplicaTransferBuffer : IDisposable
{
    private readonly ArrayPool<byte> _pool;
    private byte[]? _buffer;

    private ReplicaTransferBuffer(TransferReplicaStateRequest request, byte[] buffer, ArrayPool<byte> pool)
    {
        Request = request;
        _buffer = buffer;
        _pool = pool;
    }

    /// <summary>Gets the decoded request while this buffer is held.</summary>
    internal TransferReplicaStateRequest Request { get; }

    /// <inheritdoc />
    public void Dispose()
    {
        var buffer = Interlocked.Exchange(ref _buffer, null);
        if (buffer != null)
            _pool.Return(buffer, true);
    }

    /// <summary>Parses the transport bytes into the request owned by this scope.</summary>
    /// <param name="bytes">The complete protobuf payload.</param>
    /// <param name="pool">The byte pool of the receiver.</param>
    /// <returns>The owner of the parsed request.</returns>
    /// <exception cref="InvalidDataException">The message is malformed.</exception>
    internal static ReplicaTransferBuffer Parse(in ReadOnlySequence<byte> bytes, ArrayPool<byte> pool)
    {
        ArgumentNullException.ThrowIfNull(pool);
        var length = int.CreateChecked(bytes.Length);
        var buffer = pool.Rent(length);
        try
        {
            var source = bytes;
            source.CopyTo(buffer);
            return new ReplicaTransferBuffer(ReplicaTransferParser.Parse(buffer, length), buffer, pool);
        }
        catch (InvalidProtocolBufferException exception)
        {
            pool.Return(buffer, true);
            throw new InvalidDataException("The state transfer protobuf message is malformed.", exception);
        }
        catch
        {
            pool.Return(buffer, true);
            throw;
        }
    }
}
