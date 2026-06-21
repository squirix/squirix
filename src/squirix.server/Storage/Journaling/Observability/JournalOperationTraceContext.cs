namespace Squirix.Server.Storage.Journaling.Observability;

/// <summary>
/// Optional tags for a <see cref="JournalOperationKind" /> trace scope.
/// </summary>
internal readonly struct JournalOperationTraceContext
{
    public bool? GroupCommitEnabled { get; init; }

    public string? Key { get; init; }

    public string? Namespace { get; init; }

    public int? PayloadBytes { get; init; }
}
