using System;
using System.Collections.Generic;
using Squirix.Attributes;

namespace Squirix.ProtocolModel;

[Immutable]
internal sealed class NodeState
{
    internal NodeState(int id, NodeRole role, int currentTerm, int votedFor, IReadOnlyList<LogEntry> logEntries, in NodeRuntime runtime, bool unseeded)
    {
        Id = id;
        Unseeded = unseeded;
        Role = role;
        CurrentTerm = currentTerm;
        VotedFor = votedFor;
        LogEntries = CopyLog(logEntries);
        CommitIndex = runtime.CommitIndex;
        AppliedIndex = runtime.AppliedIndex;
        VotesGranted = runtime.VotesGranted;
        ReadIndex = runtime.ReadIndex;
        ReadAcks = runtime.ReadAcks;
        ReadReady = runtime.ReadReady;
        Stopping = runtime.Stopping;
        KnownLeaderTerm = runtime.KnownLeaderTerm;
        KnownLeaderId = runtime.KnownLeaderId;
    }

    private NodeState(NodeState previous, NodeRole role, int currentTerm, int votedFor, IReadOnlyList<LogEntry> logEntries, in NodeRuntime runtime, bool unseeded)
    {
        Id = previous.Id;
        Unseeded = unseeded;
        Role = role;
        CurrentTerm = currentTerm;
        VotedFor = votedFor;
        LogEntries = ReferenceEquals(logEntries, previous.LogEntries) ? logEntries : CopyLog(logEntries);
        CommitIndex = runtime.CommitIndex;
        AppliedIndex = runtime.AppliedIndex;
        VotesGranted = runtime.VotesGranted;
        ReadIndex = runtime.ReadIndex;
        ReadAcks = runtime.ReadAcks;
        ReadReady = runtime.ReadReady;
        Stopping = runtime.Stopping;
        KnownLeaderTerm = runtime.KnownLeaderTerm;
        KnownLeaderId = runtime.KnownLeaderId;
    }

    internal int AppliedIndex { get; }

    internal int CommitIndex { get; }

    internal int CurrentTerm { get; }

    internal int Id { get; }

    internal int LastLogIndex => LogEntries.Count == 0 ? 0 : LogEntries[^1].Index;

    internal int LastLogTerm => LogEntries.Count == 0 ? 0 : LogEntries[^1].Term;

    internal IReadOnlyList<LogEntry> LogEntries { get; }

    internal int ReadAcks { get; }

    internal int ReadIndex { get; }

    internal bool ReadReady { get; }

    internal NodeRole Role { get; }

    internal int VotedFor { get; }

    internal int VotesGranted { get; }

    /// <summary>Gets a value indicating whether this process has stopped: it has retired and cannot campaign again in this lifecycle.</summary>
    /// <remarks>
    /// Volatile, not durable: it lives with the process and a restart clears it, which is why it is carried on the runtime
    /// value rather than on the log. Two transitions restart a process here, and each clears the flag: a collected crash,
    /// which rebuilds the runtime value from the durable state, and the loss of the disk followed by its repair. So a
    /// crash with an intact disk already reopens admission, and the member may acquire authority again from a fresh
    /// process without any disk loss.
    /// </remarks>
    internal bool Stopping { get; }

    /// <summary>Gets the term in which this member last accepted an append-entries, or 0 when it has accepted none.</summary>
    /// <remarks>
    /// Volatile, like <see cref="Stopping" />: it is what this copy remembers, not what the cluster agreed, so it lives
    /// with the process. A receiver learns the leader of a term by taking an append-entries from it, which is the only
    /// evidence of leadership a follower has, and it is what makes a transfer hint attributable: the hint names a
    /// sender, and the receiver can only check that name against a leader it has actually heard from.
    /// </remarks>
    internal int KnownLeaderTerm { get; }

    /// <summary>Gets the member this copy knows as the leader of <see cref="KnownLeaderTerm" />, or -1 when it knows none.</summary>
    internal int KnownLeaderId { get; }

    /// <summary>Gets a value indicating whether the copy lost its disk: it votes for nobody, takes no entries and never campaigns until repaired.</summary>
    internal bool Unseeded { get; }

    internal static NodeState CreateInitial(int id) => new(id, NodeRole.Follower, 0, -1, Array.Empty<LogEntry>(), in NodeRuntime.Initial, false);

    /// <summary>Returns the node after the loss of its disk: term, vote, log and commit index are gone, and the copy is unseeded.</summary>
    /// <returns>The wiped node.</returns>
    internal NodeState Wiped() => new(Id, NodeRole.Follower, 0, -1, Array.Empty<LogEntry>(), in NodeRuntime.Initial, true);

    internal NodeState With(NodeRole role, int currentTerm, int votedFor, IReadOnlyList<LogEntry> logEntries, in NodeRuntime runtime, bool unseeded) =>
        new(this, role, currentTerm, votedFor, logEntries, in runtime, unseeded);

    private static LogEntry[] CopyLog(IReadOnlyList<LogEntry> logEntries)
    {
        var copy = new LogEntry[logEntries.Count];
        for (var i = 0; i < logEntries.Count; i++)
            copy[i] = logEntries[i];

        return copy;
    }
}
