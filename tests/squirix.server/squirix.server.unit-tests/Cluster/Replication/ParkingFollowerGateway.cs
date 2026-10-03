using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.Storage.Replication;

namespace Squirix.Server.UnitTests.Cluster.Replication;

/// <summary>
/// Follower transport double that parks every append until the test answers it, and records what each request carried; a call can
/// optionally observe its cancellation token, as the real transport does.
/// </summary>
[ThreadSafe]
internal sealed class ParkingFollowerGateway : IReplicaRpcGateway
{
    private readonly Dictionary<int, TaskCompletionSource<Call>> _arrivals = [];
    private readonly List<Call> _calls = [];
    private readonly Lock _sync = new();

    /// <summary>Gets a value indicating whether a parked call ends as canceled when its token is canceled; <see langword="false" /> unless set.</summary>
    internal bool ObservesCancellation { get; init; }

    /// <summary>Gets the number of appends received so far.</summary>
    internal int CallCount
    {
        get
        {
            lock (_sync)
                return _calls.Count;
        }
    }

    public Task<FollowerLogAppendResult> AppendEntriesAsync(string nodeId, ReplicaRpcHeader header, FollowerBatch batch, CancellationToken cancellationToken)
    {
        var call = new Call(nodeId, in batch, cancellationToken);
        TaskCompletionSource<Call> arrival;
        lock (_sync)
        {
            _calls.Add(call);
            arrival = ArrivalFor(_calls.Count - 1);
        }

        if (ObservesCancellation)
            _ = cancellationToken.Register(call.Cancel);

        _ = arrival.TrySetResult(call);
        return call.Answer;
    }

    /// <summary>Waits for the append with the given zero-based arrival number.</summary>
    /// <param name="index">The zero-based arrival number.</param>
    /// <returns>The call, once it arrived.</returns>
    internal Task<Call> CallAsync(int index)
    {
        lock (_sync)
            return ArrivalFor(index).Task;
    }

    /// <summary>Answers every parked call as accepted, so work parked by a test never outlives it.</summary>
    internal void ReleaseAll()
    {
        Call[] parked;
        lock (_sync)
            parked = [.. _calls];

        foreach (var call in parked)
            call.Accept();
    }

    private TaskCompletionSource<Call> ArrivalFor(int index)
    {
        if (!_arrivals.TryGetValue(index, out var arrival))
        {
            arrival = new TaskCompletionSource<Call>(TaskCreationOptions.RunContinuationsAsynchronously);
            _arrivals[index] = arrival;
        }

        return arrival;
    }

    /// <summary>One append request the double received, parked until answered.</summary>
    [ThreadSafe]
    internal sealed class Call
    {
        private readonly FollowerBatch _batch;
        private readonly TaskCompletionSource<FollowerLogAppendResult> _answer = new(TaskCreationOptions.RunContinuationsAsynchronously);

        internal Call(string nodeId, in FollowerBatch batch, CancellationToken token)
        {
            NodeId = nodeId;
            _batch = batch;
            Token = token;
        }

        /// <summary>Gets the leader commit index the request carried.</summary>
        internal ulong CommitIndex => _batch.LeaderCommitIndex;

        /// <summary>Gets the number of entries the request carried.</summary>
        internal int Count => _batch.Records.Count;

        /// <summary>Gets the log index of the first entry of the request.</summary>
        internal ulong FirstIndex => _batch.Records[0].LogIndex;

        /// <summary>Gets the log index of the last entry of the request.</summary>
        internal ulong LastIndex => _batch.Records[^1].LogIndex;

        /// <summary>Gets the node the request was sent to.</summary>
        internal string NodeId { get; }

        /// <summary>Gets the predecessor index the request named.</summary>
        internal ulong PrevLogIndex => _batch.PrevLogIndex;

        /// <summary>Gets the token the request was sent with.</summary>
        internal CancellationToken Token { get; }

        internal Task<FollowerLogAppendResult> Answer => _answer.Task;

        /// <summary>Answers the request as accepted through its last entry.</summary>
        internal void Accept() => _ = _answer.TrySetResult(new FollowerLogAppendResult(true, string.Empty, _batch.LeaderTerm, LastIndex));

        /// <summary>Answers the request as canceled.</summary>
        internal void Cancel() => _ = _answer.TrySetCanceled(Token);

        /// <summary>Answers the request as failed in transport.</summary>
        /// <param name="error">The transport failure.</param>
        internal void Fail(Exception error) => _ = _answer.TrySetException(error);

        /// <summary>Answers the request as refused.</summary>
        /// <param name="code">The refusal code.</param>
        internal void Refuse(string code) => _ = _answer.TrySetResult(new FollowerLogAppendResult(false, code, _batch.LeaderTerm, 0));
    }
}
