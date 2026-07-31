using System.Collections.Generic;

namespace Squirix.ProtocolModel;

/// <summary>Optional overrides applied by protocol-model transition patch helpers.</summary>
internal sealed class NodePatch
{
    internal int? AppliedIndex { get; init; }

    internal bool? BadOldCommit { get; init; }

    internal int? CommitIndex { get; init; }

    internal int? CurrentTerm { get; init; }

    internal IReadOnlyList<LogEntry>? LogEntries { get; init; }

    internal int? ReadAcks { get; init; }

    internal int? ReadIndex { get; init; }

    internal bool? ReadReady { get; init; }

    internal NodeRole? Role { get; init; }

    internal int? VotedFor { get; init; }

    internal int? VotesGranted { get; init; }
}
