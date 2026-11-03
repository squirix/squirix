using System;
using System.Runtime.ExceptionServices;
using System.Threading;
using System.Threading.Tasks;
using Grpc.Core;
using Squirix.Server.Attributes;
using Squirix.Server.Cluster;
using Squirix.Server.Threading;

namespace Squirix.Server.Adapters.Grpc.Replication;

/// <summary>Serializes exchanges of one peer/group, retiring a call before any cancelled exchange can be reused.</summary>
[ThreadSafe]
internal sealed class ReplicaAppendSession : IDisposable
{
    private readonly AsyncLock _gate = new();
    private readonly string _node;
    private readonly IServerClientPool _pool;
    private int _disposed;
    private ReplicaAppendEpoch? _epoch;
    private bool _unary;

    internal ReplicaAppendSession(IServerClientPool pool, string node)
    {
        _pool = pool;
        _node = node;
    }

    /// <summary>Closes the current call and refuses queued exchanges without releasing an active channel lease early.</summary>
    public void Dispose()
    {
        if (Interlocked.Exchange(ref _disposed, 1) != 0)
            return;

        Volatile.Read(ref _epoch)?.Close();
        _gate.Dispose();
    }

    internal async Task<AppendReplicaEntriesResponse> SendAsync(AppendReplicaEntriesRequest request, CancellationToken cancellationToken)
    {
        using var held = await _gate.LockAsync(cancellationToken).ConfigureAwait(false);
        ObjectDisposedException.ThrowIf(Volatile.Read(ref _disposed) != 0, this);
        cancellationToken.ThrowIfCancellationRequested();
        if (_unary)
            return await SendUnaryAsync(request, cancellationToken).ConfigureAwait(false);

        try
        {
            return await ExchangeAsync(request, cancellationToken).ConfigureAwait(false);
        }
        catch (RpcException exception) when (exception.StatusCode == StatusCode.Unimplemented)
        {
            // An older peer has not handled a stream frame: only that explicit refusal permits a unary retry.
            _unary = true;
            cancellationToken.ThrowIfCancellationRequested();
            ObjectDisposedException.ThrowIf(Volatile.Read(ref _disposed) != 0, this);
            return await SendUnaryAsync(request, cancellationToken).ConfigureAwait(false);
        }
    }

    private async Task<AppendReplicaEntriesResponse> ExchangeAsync(AppendReplicaEntriesRequest request, CancellationToken cancellationToken)
    {
        var epoch = _epoch;
        if (epoch?.Closed != false)
        {
            epoch = new ReplicaAppendEpoch(_pool, _node);
            Volatile.Write(ref _epoch, epoch);
        }

        if (Volatile.Read(ref _disposed) != 0 || !epoch.Begin())
        {
            epoch.Close();
            throw new RpcException(new Status(StatusCode.Cancelled, "Append stream closed."));
        }

        var registration = cancellationToken.UnsafeRegister(ReplicaAppendEpoch.CloseCallback, epoch);
        await using var cancellation = registration.ConfigureAwait(false);
        try
        {
            var sequence = epoch.NextSequence();
            epoch.ExpectReply();
            if (epoch.ReadCompleted)
            {
                var (bufferedFrame, bufferedFailure) = await epoch.ReadAsync().ConfigureAwait(false);
                if (bufferedFailure != null)
                    ExceptionDispatchInfo.Throw(bufferedFailure);

                if (bufferedFrame == null)
                    throw new RpcException(new Status(StatusCode.Unavailable, "Append stream ended before its acknowledgement."));

                throw new RpcException(new Status(StatusCode.DataLoss, "Append peer replied before its request."));
            }

            await epoch.Call.RequestStream.WriteAsync(new ReplicaAppendStreamRequest { Sequence = sequence, Request = request }, CancellationToken.None).ConfigureAwait(false);
            var (frame, failure) = await epoch.ReadAsync().ConfigureAwait(false);
            if (failure != null)
                ExceptionDispatchInfo.Throw(failure);

            if (frame == null)
                throw new RpcException(new Status(StatusCode.Unavailable, "Append stream ended before its acknowledgement."));
            if (frame.Sequence != sequence || frame.Response == null)
                throw new RpcException(new Status(StatusCode.DataLoss, "Append acknowledgement does not match its request."));

            if (sequence >= 4096)
                epoch.Close();
            else
                epoch.ReadAhead();

            return frame.Response;
        }
        catch
        {
            epoch.Close();
            throw;
        }
        finally
        {
            epoch.End();
        }
    }

    private async Task<AppendReplicaEntriesResponse> SendUnaryAsync(AppendReplicaEntriesRequest request, CancellationToken cancellationToken)
    {
        using var lease = _pool.LeaseChannel(_node, cancellationToken);
        var client = new SquirixReplicationService.SquirixReplicationServiceClient(lease.Channel);
        return await client.AppendReplicaEntriesAsync(request, cancellationToken: lease.Token).ResponseAsync.ConfigureAwait(false);
    }
}
