using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Grpc.Core;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Time.Testing;
using Squirix.Server.Attributes;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.Storage.Replication;
using Squirix.Server.TestKit.IO;

namespace Squirix.Server.UnitTests.Cluster.Replication;

/// <summary>A voter set that answers by a script across rounds, and a leadership that records what the election driver asked of it.</summary>
internal static class ElectionTestDoubles
{
    /// <summary>The election timing of the driver tests: no jitter unless a test asks for it.</summary>
    internal static readonly ElectionTimerOptions Options = new()
    {
        ElectionTimeout = TimeSpan.FromMilliseconds(500),
        HeartbeatInterval = TimeSpan.FromMilliseconds(100),
        MaxJitter = TimeSpan.Zero,
        VoteRpcTimeout = TimeSpan.FromMilliseconds(250),
        JitterSeed = 11UL,
    };

    private static readonly byte[] Fingerprint = [9, 8, 7];

    /// <summary>Creates a driver for a group over a real log on a temp directory.</summary>
    /// <param name="scope">The driver parts.</param>
    /// <param name="members">The members in slot order; this node is <c language="text">n1</c>.</param>
    /// <returns>The driver.</returns>
    internal static ReplicaGroupElection CreateElection(ElectionScope scope, string[] members) => new(
        scope.State,
        scope.Log,
        scope.Votes,
        scope.Leadership,
        members,
        new ReplicaRpcHeader(scope.GroupId, Fingerprint, 1UL, 0UL, string.Empty, "n1"));

    /// <summary>Opens the parts of a driver for a group.</summary>
    /// <param name="groupId">The group.</param>
    /// <param name="replicaCount">The number of members.</param>
    /// <param name="votes">The voters.</param>
    /// <param name="options">The election timing; <see cref="Options" /> unless set.</param>
    /// <returns>The opened scope.</returns>
    internal static async Task<ElectionScope> OpenAsync(string groupId, int replicaCount, IReplicaVoteGateway votes, ElectionTimerOptions? options = null)
    {
        var dir = new TempDirectory("squirix-election-driver");
        FollowerLog? log = null;
        try
        {
            log = new FollowerLog(dir, groupId, GroupComposition.Create(groupId), NullLogger<FollowerLog>.Instance);
            await log.OpenAsync(CancellationToken.None);
            var time = new FakeTimeProvider();
            var state = new ReplicaGroupState(replicaCount, options ?? Options, time);
            var scope = new ElectionScope(groupId, dir, log, time, state, votes, new RecordingLeadership(state));
            log = null;
            return scope;
        }
        finally
        {
            if (log != null)
                await log.DisposeAsync();
        }
    }

    /// <summary>The parts of one driver under test.</summary>
    /// <param name="GroupId">The group.</param>
    /// <param name="Dir">The directory of the log.</param>
    /// <param name="Log">The group log.</param>
    /// <param name="Time">The fake clock.</param>
    /// <param name="State">The election state.</param>
    /// <param name="Votes">The voters.</param>
    /// <param name="Leadership">The recording leadership.</param>
    internal sealed record ElectionScope(
        string GroupId,
        TempDirectory Dir,
        FollowerLog Log,
        FakeTimeProvider Time,
        ReplicaGroupState State,
        IReplicaVoteGateway Votes,
        RecordingLeadership Leadership) : IAsyncDisposable
    {
        /// <inheritdoc />
        public async ValueTask DisposeAsync()
        {
            await Log.DisposeAsync();
            Dir.Dispose();
        }
    }

    /// <summary>Voters that answer each call through a script; a <see langword="null" /> answer is a transport failure.</summary>
    [ThreadSafe]
    internal sealed class ScriptedVotes : IReplicaVoteGateway
    {
        private readonly Func<VoteCall, FollowerLogVoteResult?> _answer;
        private readonly List<VoteCall> _calls = [];
        private readonly Lock _sync = new();

        /// <summary>Initializes a new instance of the <see cref="ScriptedVotes" /> class.</summary>
        /// <param name="answer">Answers one call.</param>
        internal ScriptedVotes(Func<VoteCall, FollowerLogVoteResult?> answer)
        {
            _answer = answer;
        }

        /// <summary>Gets the calls received so far.</summary>
        internal VoteCall[] Calls
        {
            get
            {
                lock (_sync)
                    return [.. _calls];
            }
        }

        /// <inheritdoc />
        public Task<FollowerLogVoteResult> PreVoteAsync(string nodeId, ReplicaRpcHeader header, ulong lastLogIndex, ulong lastLogTerm, CancellationToken cancellationToken) =>
            AnswerAsync(new VoteCall(nodeId, true, header.Term, lastLogIndex, lastLogTerm));

        /// <inheritdoc />
        public Task<FollowerLogVoteResult> RequestVoteAsync(string nodeId, ReplicaRpcHeader header, ulong lastLogIndex, ulong lastLogTerm, CancellationToken cancellationToken) =>
            AnswerAsync(new VoteCall(nodeId, false, header.Term, lastLogIndex, lastLogTerm));

        private Task<FollowerLogVoteResult> AnswerAsync(VoteCall call)
        {
            lock (_sync)
                _calls.Add(call);

            return _answer(call) is { } reply ? Task.FromResult(reply)
                : Task.FromException<FollowerLogVoteResult>(new RpcException(new Status(StatusCode.Unavailable, "voter unreachable")));
        }
    }

    /// <summary>One vote call a voter received.</summary>
    /// <param name="NodeId">The voter.</param>
    /// <param name="PreVote">Whether it was a pre-vote.</param>
    /// <param name="Term">The proposed or candidate term.</param>
    /// <param name="LastLogIndex">The last index of the candidate log.</param>
    /// <param name="LastLogTerm">The term of that index.</param>
    [Immutable]
    internal sealed record VoteCall(string NodeId, bool PreVote, ulong Term, ulong LastLogIndex, ulong LastLogTerm);

    /// <summary>A leadership that answers promotions and retirements from queues and records every call with the authority it saw.</summary>
    [ThreadSafe]
    internal sealed class RecordingLeadership : IReplicaLeadership
    {
        private readonly List<string> _calls = [];
        private readonly Queue<bool> _promotions = new();
        private readonly Queue<bool> _retirements = new();
        private readonly ReplicaGroupState _state;
        private readonly Lock _sync = new();

        /// <summary>Initializes a new instance of the <see cref="RecordingLeadership" /> class.</summary>
        /// <param name="state">The election state whose authority each call records.</param>
        internal RecordingLeadership(ReplicaGroupState state)
        {
            _state = state;
        }

        /// <summary>Gets the calls so far, comma-separated: <c language="text">promote:term</c>, <c language="text">retire:authority</c>, <c language="text">heartbeat</c>.</summary>
        internal string Calls
        {
            get
            {
                lock (_sync)
                    return string.Join(',', _calls);
            }
        }

        /// <summary>Gets or sets what the next promotion does before it answers, such as advancing the fake clock; it runs once.</summary>
        internal Action? DuringNextPromotion { get; set; }

        /// <inheritdoc />
        public Task HeartbeatAsync(string groupId, CancellationToken cancellationToken)
        {
            Record("heartbeat");
            return Task.CompletedTask;
        }

        /// <inheritdoc />
        public Task<bool> PromoteAsync(string groupId, ulong term, CancellationToken cancellationToken)
        {
            lock (_sync)
            {
                _calls.Add($"promote:{term}");
                var during = DuringNextPromotion;
                DuringNextPromotion = null;
                during?.Invoke();
                return Task.FromResult(!_promotions.TryDequeue(out var promoted) || promoted);
            }
        }

        /// <inheritdoc />
        public Task<bool> RetireAsync(string groupId, CancellationToken cancellationToken)
        {
            lock (_sync)
            {
                _calls.Add($"retire:{_state.HasAuthority}");
                return Task.FromResult(!_retirements.TryDequeue(out var retired) || retired);
            }
        }

        /// <summary>Queues the answers of the next promotions; later ones answer committed.</summary>
        /// <param name="answers">The answers.</param>
        internal void AnswerPromotions(params bool[] answers)
        {
            lock (_sync)
            {
                foreach (var answer in answers)
                    _promotions.Enqueue(answer);
            }
        }

        /// <summary>Queues the answers of the next retirements; later ones answer retired.</summary>
        /// <param name="answers">The answers.</param>
        internal void AnswerRetirements(params bool[] answers)
        {
            lock (_sync)
            {
                foreach (var answer in answers)
                    _retirements.Enqueue(answer);
            }
        }

        private void Record(string call)
        {
            lock (_sync)
                _calls.Add(call);
        }
    }
}
