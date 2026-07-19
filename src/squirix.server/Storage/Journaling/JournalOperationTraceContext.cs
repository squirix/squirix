namespace Squirix.Server.Storage.Journaling;

/// <summary>
/// Optional tags for a <see cref="Abstractions.JournalOperationKind" /> trace scope.
/// </summary>
internal sealed record JournalOperationTraceContext
{
    internal bool? GroupCommitEnabled { get; init; }

    internal string? Key { get; init; }

    internal string? Namespace { get; init; }

    internal int? PayloadBytes { get; init; }
}
