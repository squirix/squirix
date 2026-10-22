using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Rocks;
using Squirix.ProtocolModel;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.Node.Services;
using Squirix.Server.Storage.Replication;
using Squirix.Server.Utils;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;

namespace Squirix.Server.IntegrationTests.Cluster.Replication;

/// <summary>Test-only production trace capture and independent safety projection.</summary>
internal static class ConformanceTestKit
{
    /// <summary>The value of a trace field the product scenario does not observe.</summary>
    internal const int NotObserved = ModelCommitTracePoint.NotObserved;

    /// <summary>The group size of every conformance scenario.</summary>
    internal const int ReplicaCount = 3;

    /// <summary>What a follower answers to the leader in a scenario.</summary>
    internal enum FollowerReply
    {
        /// <summary>A valid answer in the leader term.</summary>
        Answer = 0,

        /// <summary>An answer that carries a term older than the leader term.</summary>
        StaleTerm = 1,

        /// <summary>No answer: the follower is busy with an earlier request, which a test releases.</summary>
        Busy = 2,
    }

    /// <summary>What a peer answers to a vote request in a scenario.</summary>
    internal enum VoteReply
    {
        /// <summary>A grant in the candidate term.</summary>
        Grant = 0,

        /// <summary>A grant that reports a term older than the candidate term.</summary>
        GrantStaleTerm = 1,

        /// <summary>A refusal in the candidate term.</summary>
        Refuse = 2,
    }

    /// <summary>Asserts that the protocol model reaches the trace; the oracle is checked before any outcome of the scenario.</summary>
    /// <param name="pipeline">The pipeline whose trace is checked.</param>
    /// <param name="allowEmpty">Whether a scenario that leaves no leader observation, because it correctly wins nothing, is accepted.</param>
    /// <param name="profile">The bounds the model searches in, or none for the profile of the trace.</param>
    /// <remarks>The copies of a commit are counted per pipeline, so a trace that reports them must come from a scenario with one local entry.</remarks>
    internal static async Task AssertModelAcceptedAsync(Pipeline pipeline, bool allowEmpty = false, ExploreProfile? profile = null)
    {
        var trace = pipeline.Trace;
        var reportsCopies = false;
        for (var index = 0; index < trace.Count; index++)
            reportsCopies |= trace[index].Acks != NotObserved;

        if (reportsCopies)
            _ = await Assert.That(pipeline.LocalIndexes.Count).IsEqualTo(1).Because("The copies are counted per pipeline, so a trace that reports them needs exactly one local entry. Recovered tail entries are not local appends: a commit inside the tail takes its copies from the declared holders, and a later commit counts only the valid answers for the one local entry.");

        if (allowEmpty && trace.Count == 0)
            return;

        _ = await Assert.That(trace).IsNotEmpty();
        var modelTrace = new ModelCommitTracePoint[trace.Count];
        for (var index = 0; index < trace.Count; index++)
        {
            var current = trace[index];
            modelTrace[index] = new ModelCommitTracePoint(
                Convert.ToInt32(current.Term),
                Convert.ToInt32(current.LogIndex),
                Convert.ToInt32(current.CommitIndex),
                Convert.ToInt32(current.AppliedIndex),
                current.LeaderId,
                current.LastLogTerm,
                current.Votes,
                current.Acks,
                current.ReadIndex,
                current.ReadAcks);
        }

        var accepted = profile == null ? ExploreRunner.AcceptsCommitTrace(modelTrace) : ExploreRunner.AcceptsCommitTrace(modelTrace, profile);
        _ = await Assert.That(accepted).IsTrue().Because("The production trace is not accepted by the protocol model transition system.");
    }

    internal static ReplicaCommitCoordinator CreateCoordinator(
        Pipeline pipeline,
        TimeProvider budgetClock,
        int maxInFlight = 4,
        int replicaCount = ReplicaCount,
        ReplicaRecoveredTail? recoveredTail = null)
    {
        var expectations = new IReplicaCommitFaultHooksCreateExpectations();
        _ = expectations.Setups.OnStageAsync(Arg.Any<ReplicaCommitStage>(), Arg.Any<PreparedReplicaMutation>(), Arg.Any<CancellationToken>()).ReturnValue(ValueTask.CompletedTask);
        var options = recoveredTail == null
            ? new ReplicaCommitCoordinatorOptions(replicaCount, 0, 0, maxInFlight, pipeline.LeaderSlot)
            : new ReplicaCommitCoordinatorOptions(replicaCount, recoveredTail.LastIndex, recoveredTail.FirstIndex - 1, maxInFlight, pipeline.LeaderSlot);
        return new ReplicaCommitCoordinator(options, pipeline, expectations.Instance(), new GroupIdempotencyState(maxInFlight + 2, TimeSpan.MaxValue, TimeProvider.System), null, recoveredTail)
        {
            ObserveTimeProvider = TimeProvider.System,
            BudgetTimeProvider = budgetClock,
        };
    }

    internal static PreparedReplicaMutation CreateMutation(ulong index, ulong term = 1)
    {
        var identity = new ReplicaOperationIdentity("group-a", "client", index.ToString("x32", CultureInfo.InvariantCulture), new[] { Convert.ToByte(index) });
        return new PreparedReplicaMutation(identity, term, index, new ReplicaMutationPayload(new byte[] { 2 }, new byte[] { 7 }, Convert.ToUInt32(index)));
    }

    /// <summary>Builds the recovered tail of a leader: entries at the given indexes, all written in one term.</summary>
    /// <param name="term">The term of the entries.</param>
    /// <param name="currentTerm">The current term of the leader.</param>
    /// <param name="indexes">The contiguous log indexes of the entries.</param>
    /// <returns>The tail.</returns>
    internal static ReplicaRecoveredTail CreateRecoveredTail(ulong term, ulong currentTerm, params ulong[] indexes)
    {
        var entries = new FollowerLogEntry[indexes.Length];
        for (var i = 0; i < entries.Length; i++)
            entries[i] = new FollowerLogEntry(indexes[i], term, ReadOnlyMemory<byte>.Empty);

        var rebuilder = new IReplicaTailRebuilderCreateExpectations();
        _ = rebuilder.Setups.Rebuild(Arg.Any<FollowerLogEntry>()).Callback(static entry => CreateMutation(entry.LogIndex, entry.Term));
        return new ReplicaRecoveredTail(entries, currentTerm, rebuilder.Instance());
    }

    /// <summary>Tells the coordinator which followers held the recovered tail when the leader started; they are the copies of a commit inside the tail.</summary>
    /// <param name="coordinator">The coordinator built over the pipeline.</param>
    /// <param name="pipeline">The pipeline, whose <see cref="Pipeline.RecoveredHolders" /> name the followers.</param>
    internal static void AdmitTailHolders(ReplicaCommitCoordinator coordinator, Pipeline pipeline)
    {
        for (var i = 0; i < pipeline.RecoveredHolders.Length; i++)
            coordinator.AdmitReplica(pipeline.RecoveredHolders[i], pipeline.RecoveredLogIndex);
    }

    /// <summary>One production observation of the leader; a field set to <see cref="NotObserved" /> is not checked.</summary>
    /// <param name="Term">The leader term.</param>
    /// <param name="LogIndex">The last local log index.</param>
    /// <param name="CommitIndex">The commit index.</param>
    /// <param name="AppliedIndex">The applied index, or <see cref="NotObserved" /> when the scenario does not report it.</param>
    /// <param name="LeaderId">The leader slot.</param>
    /// <param name="LastLogTerm">The term of the last local entry.</param>
    /// <param name="Votes">The votes of the election: its own and the grants in the candidate term.</param>
    /// <param name="Acks">The copies the entry had when the commit index rose; set on that point only.</param>
    /// <param name="ReadIndex">The read index of a served read.</param>
    /// <param name="ReadAcks">The confirmations of the round that served the read.</param>
    internal sealed record TracePoint(
        ulong Term,
        ulong LogIndex,
        ulong CommitIndex,
        long AppliedIndex,
        int LeaderId = NotObserved,
        int LastLogTerm = NotObserved,
        int Votes = NotObserved,
        int Acks = NotObserved,
        int ReadIndex = NotObserved,
        int ReadAcks = NotObserved);

    /// <summary>Answers the vote requests of a scenario by peer and counts the grants a candidate may count.</summary>
    internal sealed class VoteRecorder
    {
        private readonly IReadOnlyDictionary<string, VoteReply> _replies;
        private int _grants;

        internal VoteRecorder(IReadOnlyDictionary<string, VoteReply> replies)
        {
            _replies = replies;
        }

        /// <summary>Counts the votes of the candidate: its own durable vote and the grants whose reply term is the candidate term.</summary>
        internal int CountVotes() => 1 + Volatile.Read(ref _grants);

        internal Task<FollowerLogVoteResult> RequestVoteAsync(string nodeId, ulong candidateTerm)
        {
            var reply = _replies[nodeId];
            var result = reply switch
            {
                VoteReply.Grant => new FollowerLogVoteResult(true, string.Empty, candidateTerm),
                VoteReply.GrantStaleTerm => new FollowerLogVoteResult(true, string.Empty, candidateTerm - 1),
                VoteReply.Refuse => new FollowerLogVoteResult(false, "vote-refused", candidateTerm),
                _ => throw new ArgumentOutOfRangeException(nameof(nodeId), reply, "Unsupported vote reply."),
            };
            if (result.Granted && result.CurrentTerm == candidateTerm)
                _ = Interlocked.Increment(ref _grants);

            return Task.FromResult(result);
        }
    }

    /// <summary>
    /// The real read-index round of one leader term, with followers that answer as the scenario says; every valid answer it delivers after the
    /// round started is counted, so the confirmations are the scenario's facts and not the round's own bookkeeping.
    /// </summary>
    internal sealed class ReadProbe : IReplicaRpcGateway
    {
        private readonly TaskCompletionSource<FollowerLogAppendResult>?[] _busy;
        private readonly IReplicaRpcGateway _gateway;
        private readonly int _leaderSlot;
        private readonly string[] _members;
        private readonly List<Task> _pending = [];
        private readonly FollowerReply[] _replies;
        private readonly ReplicaReadIndexRound _round;
        private readonly Lock _sync = new();
        private readonly ulong _term;
        private int _confirmed;

        /// <summary>Initializes a new instance of the <see cref="ReadProbe" /> class and sends the earlier request of every busy follower.</summary>
        /// <param name="term">The leader term.</param>
        /// <param name="leaderSlot">The leader slot.</param>
        /// <param name="replies">What each slot answers; the entry of the leader slot is ignored.</param>
        internal ReadProbe(ulong term, int leaderSlot, FollowerReply[] replies)
        {
            _term = term;
            _leaderSlot = leaderSlot;
            _replies = replies;
            _busy = new TaskCompletionSource<FollowerLogAppendResult>?[replies.Length];
            _members = new string[replies.Length];
            for (var slot = 0; slot < _members.Length; slot++)
                _members[slot] = string.Create(CultureInfo.InvariantCulture, $"node-{slot}");

            _round = new ReplicaReadIndexRound(term, replies.Length, leaderSlot);
            _gateway = _round.Observing(this, _members);
            for (var slot = 0; slot < replies.Length; slot++)
            {
                if (slot == leaderSlot || replies[slot] != FollowerReply.Busy)
                    continue;

                _busy[slot] = new TaskCompletionSource<FollowerLogAppendResult>(TaskCreationOptions.RunContinuationsAsynchronously);
                Send(slot);
            }
        }

        public Task<FollowerLogAppendResult> AppendEntriesAsync(string nodeId, ReplicaRpcHeader header, FollowerBatch batch, CancellationToken cancellationToken)
        {
            var slot = Array.IndexOf(_members, nodeId);
            switch (_replies[slot])
            {
                case FollowerReply.Answer:
                    _ = Interlocked.Increment(ref _confirmed);
                    return Task.FromResult(Reply(_term));
                case FollowerReply.StaleTerm:
                    return Task.FromResult(Reply(_term - 1));
                case FollowerReply.Busy:
                    return _busy[slot]!.Task;
                default:
                    throw new ArgumentOutOfRangeException(nameof(nodeId), _replies[slot], "Unsupported follower reply.");
            }
        }

        /// <summary>Counts the confirmations the round had: the leader and every valid answer delivered after the round started.</summary>
        internal int CountConfirmations() => 1 + Volatile.Read(ref _confirmed);

        /// <summary>Starts a read: the round takes the commit index of the pipeline as the read index and sends its heartbeat inline.</summary>
        /// <param name="pipeline">The pipeline of the leader.</param>
        /// <param name="cancellationToken">Cancellation token.</param>
        /// <returns>The read, which completes with the read index once a majority confirmed the leader term.</returns>
        internal Task<ulong> StartConfirmAsync(Pipeline pipeline, CancellationToken cancellationToken) =>
            _round.ConfirmAsync(this, _ => pipeline.CommitIndex, static probe => probe.SendHeartbeat(), cancellationToken).AsTask();

        /// <summary>Delivers the replies to the requests sent before the round started, then closes the round.</summary>
        /// <returns>An asynchronous operation.</returns>
        /// <remarks>
        /// A round that confirmed by now had its result set before the close, so the close cannot undo it; a round that did not is failed, so
        /// its read is refused. Nothing then waits for a reply that never comes.
        /// </remarks>
        internal async Task SettleAsync()
        {
            await ReleaseBusyAsync().ConfigureAwait(false);
            _round.Close();
        }

        private static FollowerLogAppendResult Reply(ulong term) => new(true, string.Empty, term, 1UL);

        private async Task ReleaseBusyAsync()
        {
            foreach (var parked in _busy)
                _ = parked?.TrySetResult(Reply(_term));

            Task[] requests;
            lock (_sync)
                requests = [.. _pending];

            await Task.WhenAll(requests).ConfigureAwait(false);
        }

        private void Send(int slot)
        {
            var request = _gateway.AppendEntriesAsync(_members[slot], default, default, CancellationToken.None);
            lock (_sync)
                _pending.Add(request);
        }

        private void SendHeartbeat()
        {
            for (var slot = 0; slot < _replies.Length; slot++)
            {
                if (slot != _leaderSlot && _replies[slot] != FollowerReply.Busy)
                    Send(slot);
            }
        }
    }

    internal sealed class Pipeline : IReplicaCommitPipeline
    {
        private readonly Gate? _applyGate;
        private readonly Gate? _firstLocalAppendGate;
        private readonly TaskCompletionSource<bool> _laggingStarted = new(TaskCreationOptions.RunContinuationsAsynchronously);
        private readonly TaskCompletionSource<ReplicaDurableAcknowledgement> _lagging = new(TaskCreationOptions.RunContinuationsAsynchronously);

        private readonly int _laggingReplica;
        private readonly int _staleTermReplica;
        private readonly Lock _sync = new();
        private readonly List<TracePoint> _trace = [];
        private readonly int _unavailableReplica;
        private ulong _appliedIndex;
        private ulong _commitIndex;
        private int _followerCalls;
        private ulong _lastTerm;
        private int _localAppendCalls;
        private int _validAcks;

        internal Pipeline(int laggingReplica = -1, bool blockFirstLocalAppend = false, int unavailableReplica = -1, int staleTermReplica = -1, bool parkApply = false)
        {
            _laggingReplica = laggingReplica;
            _firstLocalAppendGate = blockFirstLocalAppend ? new Gate() : null;
            _unavailableReplica = unavailableReplica;
            _staleTermReplica = staleTermReplica;
            _applyGate = parkApply ? new Gate() : null;
        }

        /// <summary>Gets the task that completes once the first apply is parked; the commit index is durable by then.</summary>
        internal Task ApplyParked => RequiredGate(_applyGate).Parked;

        internal ulong AppliedIndex
        {
            get
            {
                lock (_sync)
                    return _appliedIndex;
            }
        }

        internal ulong CommitIndex
        {
            get
            {
                lock (_sync)
                    return _commitIndex;
            }
        }

        internal Task FirstLocalAppendStarted => RequiredGate(_firstLocalAppendGate).Parked;

        internal int FollowerCalls => Volatile.Read(ref _followerCalls);

        internal Task LaggingFollowerStarted => _laggingStarted.Task;

        /// <summary>Gets the slot of the leader, which every trace point reports and the coordinator never sends to.</summary>
        internal int LeaderSlot { get; init; }

        /// <summary>Gets the leader term the trace reports; zero reports the term of the last entry.</summary>
        internal ulong LeaderTerm { get; init; }

        internal List<ulong> LocalIndexes { get; } = [];

        /// <summary>Gets the follower slots that hold the recovered tail when the leader starts; they are the copies of a commit that stops inside the tail.</summary>
        internal int[] RecoveredHolders { get; init; } = [];

        /// <summary>Gets the last log index of the recovered tail, or zero when the leader starts with an empty tail.</summary>
        internal ulong RecoveredLogIndex { get; init; }

        /// <summary>Gets the term of the last recovered entry.</summary>
        internal ulong RecoveredLogTerm { get; init; }

        /// <summary>Gets a value indicating whether the trace reports the applied index; a scenario whose model keeps it fixed turns it off.</summary>
        internal bool ReportsApplied { get; init; } = true;

        /// <summary>Gets a snapshot of the trace.</summary>
        internal IReadOnlyList<TracePoint> Trace
        {
            get
            {
                lock (_sync)
                    return [.. _trace];
            }
        }

        /// <summary>Gets the recorder of the election that made the leader, or none.</summary>
        internal VoteRecorder? Votes { get; init; }

        /// <summary>Gets the last log index: the last local entry, or the end of the recovered tail before any local append.</summary>
        /// <remarks>Read under the lock.</remarks>
        private ulong LastLogIndex => LocalIndexes.Count > 0 ? LocalIndexes[^1] : RecoveredLogIndex;

        /// <summary>Gets the term of the last log entry: the last local entry, or the last recovered one before any local append.</summary>
        /// <remarks>Read under the lock.</remarks>
        private ulong LastLogTerm => LocalIndexes.Count > 0 ? _lastTerm : RecoveredLogTerm;

        public ValueTask AdvanceCommitIndexAsync(ulong commitIndex, CancellationToken cancellationToken)
        {
            lock (_sync)
            {
                _commitIndex = commitIndex;

                // Every follower of the single local entry is asked before the commit step, so the valid answers counted by now are its copies;
                // a commit that stops inside the recovered tail is backed by the followers that held the tail from the start.
                var copies = commitIndex <= RecoveredLogIndex ? RecoveredHolders.Length : Volatile.Read(ref _validAcks);
                _trace.Add(Point(LastLogIndex, 1 + copies));
            }

            return ValueTask.CompletedTask;
        }

        public ValueTask<ReplicaDurableAcknowledgement> AppendFollowerAsync(int replicaIndex, PreparedReplicaMutation mutation, CancellationToken cancellationToken)
        {
            _ = Interlocked.Increment(ref _followerCalls);

            // A faulted task, not a synchronous throw: production gateways are async, so transport
            // failures surface as faulted follower tasks that the coordinator records as lagging.
            var isUnavailable = replicaIndex == _unavailableReplica;
            var isLagging = replicaIndex == _laggingReplica;
            var isStale = replicaIndex == _staleTermReplica;
            return (isUnavailable, isLagging, isStale) switch
            {
                (true, _, _) => ValueTask.FromException<ReplicaDurableAcknowledgement>(new IOException("Simulated replica unavailable.")),
                (false, true, _) => ParkLaggingAsync(),
                (false, false, true) => ValueTask.FromResult(Acknowledge(mutation, mutation.Term - 1)),
                (false, false, false) => ValueTask.FromResult(CountedAcknowledgement(mutation)),
            };
        }

        public ValueTask AppendLocalAsync(PreparedReplicaMutation mutation, CancellationToken cancellationToken)
        {
            lock (_sync)
            {
                LocalIndexes.Add(mutation.LogIndex);
                _lastTerm = mutation.Term;
                _trace.Add(Point(mutation.LogIndex));
            }

            return _firstLocalAppendGate == null || Interlocked.Increment(ref _localAppendCalls) != 1
                ? ValueTask.CompletedTask
                : new ValueTask(_firstLocalAppendGate.ParkAsync(cancellationToken));
        }

        public async ValueTask ApplyMemoryAsync(PreparedReplicaMutation mutation, CancellationToken cancellationToken)
        {
            if (_applyGate != null)
                _ = await _applyGate.ParkAsync(cancellationToken).ConfigureAwait(false);

            lock (_sync)
            {
                _appliedIndex = mutation.LogIndex;
                _trace.Add(Point(LastLogIndex));
            }
        }

        public void RecordLaggingReplica(int replicaIndex, ulong logIndex)
        {
        }

        /// <summary>Records the observation of a served read at the current positions.</summary>
        /// <param name="readIndex">The read index.</param>
        /// <param name="readAcks">The confirmations of the round that served the read.</param>
        internal void RecordRead(ulong readIndex, int readAcks)
        {
            lock (_sync)
                _trace.Add(Point(LastLogIndex, NotObserved, Convert.ToInt32(readIndex), readAcks));
        }

        internal void ReleaseApply() => _applyGate?.Release();

        internal void ReleaseFirstLocalAppend() => _firstLocalAppendGate?.Release();

        internal void ReleaseLagging(PreparedReplicaMutation mutation) => _ = _lagging.TrySetResult(CountedAcknowledgement(mutation));

        private static ReplicaDurableAcknowledgement Acknowledge(PreparedReplicaMutation mutation, ulong term) => new(
            mutation.GroupId,
            term,
            mutation.LogIndex,
            mutation.OperationFingerprint,
            mutation.PayloadChecksum,
            true,
            true);

        private static Gate RequiredGate(Gate? gate) => ThrowHelper.Required(gate, "The pipeline was built without this gate.");

        private ReplicaDurableAcknowledgement CountedAcknowledgement(PreparedReplicaMutation mutation)
        {
            _ = Interlocked.Increment(ref _validAcks);
            return Acknowledge(mutation, mutation.Term);
        }

        /// <summary>Builds the observation of the leader at the current positions.</summary>
        /// <param name="logIndex">The last local log index.</param>
        /// <param name="acks">The copies of the entry, set on the commit step only.</param>
        /// <param name="readIndex">The read index of a served read.</param>
        /// <param name="readAcks">The confirmations of the round that served the read.</param>
        /// <returns>The observation.</returns>
        /// <remarks>Call under the lock.</remarks>
        private TracePoint Point(ulong logIndex, int acks = NotObserved, int readIndex = NotObserved, int readAcks = NotObserved) => new(
            LeaderTerm != 0 ? LeaderTerm : LastLogTerm,
            logIndex,
            _commitIndex,
            ReportsApplied ? Convert.ToInt64(_appliedIndex) : NotObserved,
            LeaderSlot,
            Convert.ToInt32(LastLogTerm),
            Votes?.CountVotes() ?? NotObserved,
            acks,
            readIndex,
            readAcks);

        private ValueTask<ReplicaDurableAcknowledgement> ParkLaggingAsync()
        {
            _ = _laggingStarted.TrySetResult(true);
            return new ValueTask<ReplicaDurableAcknowledgement>(_lagging.Task);
        }

        /// <summary>Parks a caller until a test releases it, and tells the test the caller is parked.</summary>
        private sealed class Gate
        {
            private readonly TaskCompletionSource<bool> _parked = new(TaskCreationOptions.RunContinuationsAsynchronously);
            private readonly TaskCompletionSource<bool> _release = new(TaskCreationOptions.RunContinuationsAsynchronously);

            internal Task Parked => _parked.Task;

            internal Task<bool> ParkAsync(CancellationToken cancellationToken)
            {
                _ = _parked.TrySetResult(true);
                return _release.Task.WaitAsync(cancellationToken);
            }

            internal void Release() => _ = _release.TrySetResult(true);
        }
    }
}
