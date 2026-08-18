using System;
using System.Collections.Generic;
using Squirix.Attributes;

namespace Squirix.ProtocolModel;

[Immutable]
internal sealed class NodeState
{
    internal NodeState(int id, NodeRole role, int currentTerm, int votedFor, IReadOnlyList<LogEntry> logEntries, NodeRuntime runtime)
    {
        Id = id;
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
        BadOldCommit = runtime.BadOldCommit;
    }

    internal int AppliedIndex { get; }

    internal bool BadOldCommit { get; }

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

    internal static NodeState CreateInitial(int id) => new(id, NodeRole.Follower, 0, -1, Array.Empty<LogEntry>(), NodeRuntime.Initial);

    internal NodeState With(NodeRole role, int currentTerm, int votedFor, IReadOnlyList<LogEntry> logEntries, NodeRuntime runtime) =>
        new(Id, role, currentTerm, votedFor, logEntries, runtime);

    private static LogEntry[] CopyLog(IReadOnlyList<LogEntry> logEntries)
    {
        var copy = new LogEntry[logEntries.Count];
        for (var i = 0; i < logEntries.Count; i++)
            copy[i] = logEntries[i];

        return copy;
    }
}
