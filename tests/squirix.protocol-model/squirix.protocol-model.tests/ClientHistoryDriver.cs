using System;
using System.Collections.Generic;
using System.Runtime.InteropServices;
using Squirix.E2ETests.Fixtures;

namespace Squirix.ProtocolModel.Tests;

/// <summary>Projects one explicit client proposal and completed reads from legal model transitions.</summary>
/// <remarks>
/// Only the target proposal writes this key; election no-ops and filler proposals do not. The model identifies entries by term/index.
/// Logical ticks order client invocations and responses. A write responds only after commit and apply; a read responds only at ReadReady
/// and observes the target entry through the reader's applied index. Other keys are omitted by the register model's per-key projection.
/// </remarks>
internal abstract class ClientHistoryDriver
{
    private readonly BrokenMode _broken;
    private readonly ExploreProfile _profile = ExploreProfile.ForClientHistory();
    private readonly List<RegisterRead> _reads = [];
    private LogEntry? _write;
    private long _started;
    private long _acked;
    private long _clock;

    protected ClientHistoryDriver(BrokenMode broken)
    {
        _broken = broken;
    }

    internal ClusterState State { get; private set; } = ClusterState.CreateInitial(3);

    internal List<string> Trace { get; } = [];

    internal RegisterHistory History()
    {
        var history = new RegisterHistory();
        if (_write != null)
            history.RecordWrite(new RegisterWrite("target", 1, _started, _acked == 0 ? _clock : _acked, _acked != 0));
        foreach (ref readonly var read in CollectionsMarshal.AsSpan(_reads))
            history.RecordRead(in read);
        return history;
    }

    protected void Bootstrap()
    {
        Require(Elect(0, 1), "bootstrap election");
        Replicate(0, 1, 1);
        Replicate(0, 2, 1);
        Apply(0);
        DropAll();
    }

    protected bool Acknowledge(int node)
    {
        var entry = _write.GetValueOrDefault();
        Require(_write != null, "client proposal exists");
        var leader = State.Nodes[node];
        if (leader.Role != NodeRole.Leader || leader.CommitIndex < entry.Index || leader.AppliedIndex < entry.Index || !ClientHistoryQueries.Contains(leader, entry))
            return false;
        _acked = ++_clock;
        return true;
    }

    protected void Write(int node)
    {
        _started = ++_clock;
        Propose(node);
        _write = State.Nodes[node].LogEntries[^1];
    }

    protected void Propose(int node)
    {
        var last = State.Nodes[node].LastLogIndex;
        Step("client proposal", next => next.Nodes[node].Role == NodeRole.Leader && next.Nodes[node].LastLogIndex == last + 1);
    }

    protected void Apply(int node)
    {
        while (State.Nodes[node].AppliedIndex < State.Nodes[node].CommitIndex)
            ApplyOne(node);
    }

    protected bool Read(int node, int confirmer)
    {
        var start = ++_clock;
        Step("invoke read", next => next.Nodes[node].ReadIndex > 0);
        if (!State.Nodes[node].ReadReady)
        {
            Deliver(MsgKind.ReadIndexRequest, node, confirmer);
            Deliver(MsgKind.ReadIndexResponse, confirmer, node);
        }

        var reader = State.Nodes[node];
        if (!reader.ReadReady)
            return false;
        var entry = _write.GetValueOrDefault();
        Require(_write != null, "client proposal exists");
        var observed = reader.AppliedIndex >= entry.Index && ClientHistoryQueries.Contains(reader, entry) ? 1 : 0;
        _reads.Add(new RegisterRead("target", start, ++_clock, observed));
        return true;
    }

    protected bool Elect(int node, int voter)
    {
        Campaign(node);
        Deliver(MsgKind.RequestVote, node, voter);
        Deliver(MsgKind.VoteResponse, voter, node);
        return State.Nodes[node].Role == NodeRole.Leader;
    }

    protected void Campaign(int node)
    {
        var term = State.Nodes[node].CurrentTerm;
        Step("campaign", next => next.Nodes[node].Role == NodeRole.Candidate && next.Nodes[node].CurrentTerm == term + 1);
    }

    protected void Crash(int node) => Step("crash", next => next.Nodes[node].Role == NodeRole.Follower && next.Nodes[node].VotedFor == State.Nodes[node].VotedFor);

    protected void Replicate(int leader, int follower, int through)
    {
        DropAll();
        for (var index = State.MatchIndexes[follower] + 1; index <= through; index++)
        {
            Resend(leader, follower, index);
            Deliver(MsgKind.AppendEntries, leader, follower);
            Deliver(MsgKind.AppendResponse, follower, leader);
        }
    }

    protected void Deliver(MsgKind kind, int from, int to)
    {
        var message = ClientHistoryQueries.Message(State, kind, from, to);
        Require(State.CanCommunicate(from, to), "message route is connected");

        // Deliveries precede drops in NextStates. The first successor consuming this message is its delivery.
        Step("deliver message", next => !ClientHistoryQueries.HasMessage(next, message.Id));
    }

    protected void DropAll()
    {
        while (State.Messages.Count != 0)
            DropOne();
    }

    protected void Step(string label, Func<ClusterState, bool> matches, ExploreProfile? profile = null) =>
        Require(TryStep(label, matches, profile), label);

    protected bool TryStep(string label, Func<ClusterState, bool> matches, ExploreProfile? profile = null)
    {
        foreach (var next in CollectionsMarshal.AsSpan(ExploreRunner.NextStates(State, profile ?? _profile, _broken)))
        {
            if (!matches(next))
                continue;
            State = next;
            _clock++;
            Trace.Add(label + ": " + State.Fingerprint(false));
            return true;
        }

        return false;
    }

    protected void Require(bool condition, string label)
    {
        if (!condition)
            throw new InvalidOperationException(label + " unavailable after " + string.Join(Environment.NewLine, Trace));
    }

    private void ApplyOne(int node)
    {
        var applied = State.Nodes[node].AppliedIndex;
        Step("apply committed entry", next => next.Nodes[node].AppliedIndex == applied + 1);
    }

    private void DropOne()
    {
        var previous = State;
        Step("drop message", next => next.Messages.Count == previous.Messages.Count - 1 && ClientHistoryQueries.SameNodes(previous, next));
    }

    /// <summary>Selects the existing catch-up transition with the same bounds, without selecting an atomic election.</summary>
    /// <param name="leader">The sender.</param>
    /// <param name="follower">The receiver.</param>
    /// <param name="index">The entry index.</param>
    private void Resend(int leader, int follower, int index) =>
        Step("resend log entry", next => ClientHistoryQueries.HasAppend(next, leader, follower, index), ExploreProfile.ForLogRules(6, 8, 32, 10_000));
}
