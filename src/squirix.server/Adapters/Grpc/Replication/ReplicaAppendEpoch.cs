using System;
using System.Threading;
using System.Threading.Tasks;
using Grpc.Core;
using Squirix.Server.Attributes;
using Squirix.Server.Cluster;

namespace Squirix.Server.Adapters.Grpc.Replication;

/// <summary>One duplex call and its pool lease; shutdown releases an idle lease or waits for the active exchange to end.</summary>
[ThreadSafe]
internal sealed class ReplicaAppendEpoch
{
    private readonly ServerChannelLease _lease;
    private readonly CancellationTokenRegistration _shutdown;
    private int _active;
    private int _closed;
    private int _closeFinished;
    private int _released;
    private int _expectingReply;
    private Task<(ReplicaAppendStreamResponse? Frame, Exception? Error)>? _pending;
    private Task<bool>? _receiving;
    private ulong _sequence;

    internal ReplicaAppendEpoch(IServerClientPool pool, string node)
    {
        _lease = pool.LeaseChannel(node, CancellationToken.None);
        try
        {
            var client = new SquirixReplicationService.SquirixReplicationServiceClient(_lease.Channel);
            Call = client.AppendReplicaEntriesStream(cancellationToken: _lease.Token);
            _shutdown = _lease.Token.UnsafeRegister(CloseCallback, this);
        }
        catch
        {
            _lease.Dispose();
            throw;
        }
    }

    internal AsyncDuplexStreamingCall<ReplicaAppendStreamRequest, ReplicaAppendStreamResponse> Call { get; }

    internal bool Closed => Volatile.Read(ref _closed) != 0;

    internal bool ReadCompleted => _receiving?.IsCompleted ?? false;

    internal static void CloseCallback(object? state)
    {
        if (state is ReplicaAppendEpoch epoch)
            epoch.Close();
    }

    internal void ExpectReply() => Volatile.Write(ref _expectingReply, 1);

    internal Task<(ReplicaAppendStreamResponse? Frame, Exception? Error)> ReadAsync() => _pending ??= ReceiveAsync();

    internal void ReadAhead()
    {
        Volatile.Write(ref _expectingReply, 0);
        _pending = ReceiveAsync();
    }

    /// <summary>Advances the sequence under the owning session gate.</summary>
    /// <returns>The next request sequence.</returns>
    internal ulong NextSequence() => ++_sequence;

    internal bool Begin()
    {
        _ = Interlocked.Exchange(ref _active, 1);
        if (!Closed)
            return true;

        End();
        return false;
    }

    internal void End()
    {
        _ = Interlocked.Exchange(ref _active, 0);
        if (Volatile.Read(ref _closeFinished) != 0)
            Release();
    }

    internal void Close()
    {
        if (Interlocked.Exchange(ref _closed, 1) != 0)
            return;

        try
        {
            Call.Dispose();
        }
        finally
        {
            _ = Interlocked.Exchange(ref _closeFinished, 1);
            if (Volatile.Read(ref _active) == 0)
                Release();
        }
    }

    private async Task<(ReplicaAppendStreamResponse? Frame, Exception? Error)> ReceiveAsync()
    {
        Exception? failure = null;
        try
        {
            var receiving = Call.ResponseStream.MoveNext(CancellationToken.None);
            _receiving = receiving;
            if (await receiving.ConfigureAwait(false))
            {
                if (Volatile.Read(ref _expectingReply) != 0)
                    return (Call.ResponseStream.Current, null);

                failure = new RpcException(new Status(StatusCode.DataLoss, "Append peer replied without an active request."));
            }
        }
        catch (Exception exception) when (exception is not OutOfMemoryException)
        {
            failure = exception;
        }

        // Idle reads have no awaiting caller; carry failures as data rather than leaving a faulted task behind.
        try
        {
            Close();
        }
        catch (Exception exception) when (exception is not OutOfMemoryException)
        {
            failure ??= exception;
        }

        return (null, failure);
    }

    private void Release()
    {
        if (Interlocked.Exchange(ref _released, 1) != 0)
            return;

        // Closing requests cancellation, but the response read may still be completing. Headers alone do not cover that read;
        // they also need observing when cancellation happened before the HTTP dispatch finished.
        _ = ReleaseAfterCallAsync();
    }

    private async Task ReleaseAfterCallAsync()
    {
        Task? receiving = _receiving;
        if (receiving != null)
            await receiving.ConfigureAwait(ConfigureAwaitOptions.SuppressThrowing);

        Task headers = Call.ResponseHeadersAsync;
        await headers.ConfigureAwait(ConfigureAwaitOptions.SuppressThrowing);
        ReleaseLease();
    }

    private void ReleaseLease()
    {
        // Closing is finished before this point. Never wait on a callback that can be our own caller.
        _ = _shutdown.Unregister();
        _lease.Dispose();
    }
}
